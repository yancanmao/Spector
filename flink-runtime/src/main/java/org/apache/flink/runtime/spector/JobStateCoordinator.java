/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.spector;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.checkpoint.*;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.clusterframework.types.TaskManagerSlot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerImpl;
import org.apache.flink.runtime.spector.streamswitch.ControllerAdaptor;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.Operation.DISPATCH_STATE_TO_STANDBY_TASK;
import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.Operation.REPARTITION_STATE;
import static org.apache.flink.runtime.clusterframework.types.TaskManagerSlot.State.FREE;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class JobStateCoordinator implements JobReconfigAction, CheckpointProgressListener {

	private static final Logger LOG = LoggerFactory.getLogger(JobStateCoordinator.class);

	private final JobGraph jobGraph;

	private ExecutionGraph executionGraph;

	private ComponentMainThreadExecutor mainThreadExecutor;

	private ControllerAdaptor controllerAdaptor;

	private final JobGraphUpdater jobGraphUpdater;

	private final List<ExecutionAttemptID> notYetAcknowledgedTasks;

	private JobStatusListener jobStatusListener;

	private final Object lock = new Object();

	// mutable fields
	private volatile boolean inProcess;

	private volatile ActionType actionType;

	private volatile ReconfigID reconfigId;

	private volatile JobExecutionPlan jobExecutionPlan;

	private volatile ExecutionJobVertex targetVertex;

	private volatile long checkpointId;

	// mutable fields for scale in/out
	private volatile List<ExecutionVertex> createCandidates;

	private volatile List<ExecutionVertex> removedCandidates;

	private volatile Collection<Execution> allocatedExecutions;

	/**
	 * standby executions for backup state maintenance of each operator
	 * Operator -> [BackupTask1, BackTask2, ...]
	 */
	private final HashMap<JobVertexID, List<ExecutionVertex>> standbyExecutionVertexes;

	/**
	 * keys to replicate
	 */
	private final HashMap<JobVertexID, List<Integer>> backupKeyGroups;

	private final Map<InstanceID, List<TaskManagerSlot>> slotsMap;

	public JobStateCoordinator(
		JobGraph jobGraph,
		ExecutionGraph executionGraph,
		ClassLoader userCodeLoader) {

		this.jobGraph = jobGraph;
		this.executionGraph = executionGraph;

		this.notYetAcknowledgedTasks = new ArrayList<>();

		this.controllerAdaptor = new ControllerAdaptor(this, executionGraph);
		this.jobGraphUpdater = JobGraphUpdater.instantiate(jobGraph, userCodeLoader);

		this.standbyExecutionVertexes = new HashMap<>();
		this.backupKeyGroups = new HashMap<>();
		this.slotsMap = new HashMap<>();
	}

	public void setSlotsMap(CompletableFuture<Collection<TaskManagerSlot>> allSlots) {
		allSlots.thenAccept(taskManagerSlots -> {
			// compute
//			Map<String, List<TaskManagerSlot>> slotMap = new HashMap<>();
			for (TaskManagerSlot taskManagerSlot : taskManagerSlots) {
				InstanceID taskManagerId = taskManagerSlot.getInstanceId();
				List<TaskManagerSlot> slots = slotsMap.computeIfAbsent(taskManagerId, k -> new ArrayList<>());
				slots.add(taskManagerSlot);
			}
		});
	}

	//****************************Standby Task Creation****************************

	/**
	 * handle failure recovery based on upstream backup + checkpoint mechanism
	 * @param taskExecution
	 * @param cause
	 */
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		composePrepareNewStandby(); // TODO: this is not required by far.
	}

	/**
	 * reinitialize after failure request new slot from different task managers and deploy backup task on the machine
	 */
	public void composePrepareNewStandby() {

	}

	/**
	 * create backup tasks on task manager, maintain backup state for more efficient state management.
	 * @param newExecutionJobVerticesTopological
	 */
	public void notifyNewVertices(List<ExecutionJobVertex> newExecutionJobVerticesTopological) {
		final ArrayList<CompletableFuture<Void>> schedulingFutures = new ArrayList<>();

		final Collection<CompletableFuture<Void>> currentExecutionFutures = new ArrayList<>();

		for (ExecutionJobVertex executionJobVertex : newExecutionJobVerticesTopological) {
			for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
				currentExecutionFutures.add(
					// TODO: Anti-affinity constraint
					CompletableFuture.runAsync(() -> waitForExecutionToReachRunningState(executionVertex)));
			}
			// once all execution of current executionJobVertex are in running state, deploy backup executions
			FutureUtils.combineAll(currentExecutionFutures).whenComplete((ignored, t) -> {
				Preconditions.checkState(executionGraph.getSlotProvider() instanceof SchedulerImpl,
					"++++++ slotProvider must be SchedulerImpl");

				setSlotsMap(((SchedulerImpl) executionGraph.getSlotProvider()).getAllSlots());

				Preconditions.checkState(slotsMap.size() > 0, "++++++ Empty slotsMap");

				if (t == null) {
					// create a set of executionVertex
					List<ExecutionVertex> createCandidates = executionJobVertex.addStandbyExecutionVertex(
						executionGraph.getRpcTimeout(),
						executionGraph.getGlobalModVersion(),
						System.currentTimeMillis(),
						slotsMap.size());
					// TODO: need to create number of tasks according to number of nodes in the cluster.
					standbyExecutionVertexes.put(executionJobVertex.getJobVertexId(), createCandidates);
					LOG.info("++++++ add standby task for: " + executionJobVertex.getJobVertexId()
						+ " number of backup tasks: " + createCandidates.size());

					checkState(createCandidates.size() == slotsMap.size(),
						"++++++ Inconsistent number of execution vertices are created");

					// schedule for execution
					for (int i = 0; i < createCandidates.size(); i++) {
						SlotID allocatedSlot = null;
						Execution executionAttempt = createCandidates.get(i).getCurrentExecutionAttempt();
						for (TaskManagerSlot taskManagerSlot : (List<TaskManagerSlot>) slotsMap.values().toArray()[i]) {
							if (taskManagerSlot.getState() == FREE) {
								allocatedSlot = taskManagerSlot.getSlotId();
								break;
							}
						}
						Preconditions.checkState(allocatedSlot != null);
						// TODO: schedule backup tasks to different task managers for execution
						schedulingFutures.add(executionAttempt.scheduleForExecution(allocatedSlot));
					}
				} else {
					schedulingFutures.add(
						new CompletableFuture<>());
					schedulingFutures.get(schedulingFutures.size() - 1)
						.completeExceptionally(t);
				}
			});

			currentExecutionFutures.clear();
		}

		final CompletableFuture<Void> allSchedulingFutures = FutureUtils.waitForAll(schedulingFutures);
		allSchedulingFutures.whenComplete((Void ignored, Throwable t) -> {
			if (t != null) {
				LOG.warn("Scheduling of standby tasks failed. Cancelling the scheduling of standby tasks.");
				for (ExecutionJobVertex executionJobVertex : newExecutionJobVerticesTopological) {
					cancelStandbyExecution(executionJobVertex);
				}
			}
		});
	}

	/**
	 * dispatch checkpointed state to backup task based on the fine-grained state management policies
	 * by default, it replicate state to all other backup task
	 */
	public void dispatchLatestCheckpointedStateToStandbyTasks(
		CompletedCheckpoint checkpoint) {
		Map<JobVertexID, ExecutionJobVertex> tasks = executionGraph.getAllVertices();
		// re-assign the task states
		final Map<OperatorID, OperatorState> operatorStates = checkpoint.getOperatorStates();

		StateAssignmentOperation stateAssignmentOperation =
			new StateAssignmentOperation(checkpoint.getCheckpointID(), tasks, operatorStates,
				true, DISPATCH_STATE_TO_STANDBY_TASK, backupKeyGroups);
		checkNotNull(jobExecutionPlan, "jobExecutionPlan should not be null.");
		stateAssignmentOperation.setRedistributeStrategy(jobExecutionPlan);

		stateAssignmentOperation.assignStates();
	}

	// TODO: see if other place need to add this
	public CompletableFuture<?> cancelStandbyExecution(ExecutionJobVertex executionJobVertex) {
		if (!standbyExecutionVertexes.get(executionJobVertex.getJobVertexId()).isEmpty()) {
			LOG.debug(String.format("Cancelling standby execution %s", this));
			final ExecutionVertex standbyExecution =
				standbyExecutionVertexes.get(executionJobVertex.getJobVertexId()).remove(0);
			return standbyExecution.cancel();
		}
		return null;
	}

	private void waitForExecutionToReachRunningState(ExecutionVertex executionVertex) {
		ExecutionState executionState = ExecutionState.CREATED;
		do {
			executionState = executionVertex.getExecutionState();
		} while (executionState == ExecutionState.CREATED ||
			executionState == ExecutionState.SCHEDULED ||
			executionState == ExecutionState.DEPLOYING);
	}

	@Override
	public void onCompleteCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
		checkNotNull(checkpoint);
		LOG.info("++++++ checkpoint complete, start to dispatch state to replica");
		dispatchLatestCheckpointedStateToStandbyTasks(checkpoint);
		if (checkpoint.getProperties().getCheckpointType() == CheckpointType.RECONFIGPOINT) {
			LOG.info("++++++ redistribute operator states");
			handleCollectedStates(new HashMap<>(checkpoint.getOperatorStates()));
		}
	}

	//****************************Scaling Actions****************************


	public void init(ComponentMainThreadExecutor mainThreadExecutor) {
		this.mainThreadExecutor = mainThreadExecutor;
	}

	public void start() {
		notifyNewVertices(executionGraph.getExecutionJobVertices());
		controllerAdaptor.startControllers();

		CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		checkNotNull(checkpointCoordinator);
		checkpointCoordinator.setReconfigpointAcknowledgeListener(this);
	}

	public void stop() {
		controllerAdaptor.stopControllers();
	}

	public void assignExecutionGraph(ExecutionGraph executionGraph) {
		checkState(!inProcess, "ExecutionGraph changed after rescaling starts");
		this.executionGraph = executionGraph;

		controllerAdaptor.stopControllers();
		this.controllerAdaptor = new ControllerAdaptor(this, executionGraph);

		controllerAdaptor.startControllers();
	}

	@Override
	public JobGraph getJobGraph() {
		return this.jobGraph;
	}

	@Override
	public void setInitialJobExecutionPlan(JobVertexID vertexID, JobExecutionPlan jobExecutionPlan) {
		// TODO: by far, we only need to a single operator, but we need to have multiple operator remapping
		this.jobExecutionPlan = jobExecutionPlan;
	}

	@Override
	public void repartition(JobVertexID vertexID, JobExecutionPlan jobExecutionPlan) {
		checkState(!inProcess, "Current rescaling hasn't finished.");
		inProcess = true;
		actionType = ActionType.REPARTITION;

		reconfigId = ReconfigID.generateNextID();
		this.jobExecutionPlan = jobExecutionPlan;

		LOG.info("++++++ repartition job with ReconfigID: " + reconfigId +
			", partitionAssignment: " + jobExecutionPlan);

		List<JobVertexID> involvedUpstream = new ArrayList<>();
		List<JobVertexID> involvedDownstream = new ArrayList<>();
		try {
			jobGraphUpdater.repartition(vertexID,
				jobExecutionPlan.getPartitionAssignment(),
				involvedUpstream, involvedDownstream);
			executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));

			repartitionVertex(vertexID, involvedUpstream, involvedDownstream);
		} catch (Exception e) {
			failExecution(e);
		}
	}

	@Override
	public void scaleOut(JobVertexID vertexID, int parallelism, JobExecutionPlan jobExecutionPlan) {
		checkState(!inProcess, "Current rescaling hasn't finished.");
		inProcess = true;
		actionType = ActionType.SCALE_OUT;

		reconfigId = ReconfigID.generateNextID();
		this.jobExecutionPlan = jobExecutionPlan;
		LOG.info("scale out job with ReconfigID: " + reconfigId +
			", new parallelism: " + parallelism +
			", partitionAssignment: " + jobExecutionPlan);

		List<JobVertexID> involvedUpstream = new ArrayList<>();
		List<JobVertexID> involvedDownstream = new ArrayList<>();
		try {
			jobGraphUpdater.rescale(vertexID, parallelism,
				jobExecutionPlan.getPartitionAssignment(),
				involvedUpstream, involvedDownstream);
			executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));

			scaleOutVertex(vertexID, involvedUpstream, involvedDownstream);
		} catch (Exception e) {
			failExecution(e);
		}
	}

	@Override
	public void scaleIn(JobVertexID vertexID, int parallelism, JobExecutionPlan jobExecutionPlan) {
		checkState(!inProcess, "Current rescaling hasn't finished.");
		inProcess = true;
		actionType = ActionType.SCALE_IN;

		reconfigId = ReconfigID.generateNextID();
		this.jobExecutionPlan = jobExecutionPlan;
		LOG.info("scale in job with ReconfigID: " + reconfigId + ", new parallelism: " + parallelism);

		List<JobVertexID> involvedUpstream = new ArrayList<>();
		List<JobVertexID> involvedDownstream = new ArrayList<>();
		try {
			jobGraphUpdater.rescale(vertexID, parallelism,
				jobExecutionPlan.getPartitionAssignment(),
				involvedUpstream, involvedDownstream);
			executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));

			scaleInVertex(vertexID, involvedUpstream, involvedDownstream);
		} catch (Exception e) {
			failExecution(e);
		}
	}

	private void repartitionVertex(
			JobVertexID vertexID,
			List<JobVertexID> updatedUpstream,
			List<JobVertexID> updatedDownstream) throws ExecutionGraphException {

		Map<JobVertexID, ExecutionJobVertex> tasks = executionGraph.getAllVertices();

		CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		checkNotNull(checkpointCoordinator);
		checkpointCoordinator.setReconfigpointAcknowledgeListener(this);

		this.targetVertex = tasks.get(vertexID);

		for (ExecutionVertex vertex : this.targetVertex.getTaskVertices()) {
			notYetAcknowledgedTasks.add(vertex.getCurrentExecutionAttempt().getAttemptId());
		}

		// state check
		checkState(targetVertex.getParallelism() == jobExecutionPlan.getNumOpenedSubtask(),
			String.format("parallelism in targetVertex %d is not equal to number of executors %d",
				targetVertex.getParallelism(), jobExecutionPlan.getNumOpenedSubtask()));

		// rescale upstream and downstream
		final Collection<CompletableFuture<Void>> rescaleCandidatesFutures = new ArrayList<>();

		for (JobVertexID jobId : updatedUpstream) {
			tasks.get(jobId).cleanBeforeRescale();

			for (ExecutionVertex vertex : tasks.get(jobId).getTaskVertices()) {
				Execution execution = vertex.getCurrentExecutionAttempt();
				rescaleCandidatesFutures.add(execution.scheduleReconfig(
					reconfigId, ReconfigOptions.UPDATE_PARTITIONS_ONLY, null, null));
			}
		}

		for (JobVertexID jobId : updatedDownstream) {
			tasks.get(jobId).cleanBeforeRescale();

			for (ExecutionVertex vertex : tasks.get(jobId).getTaskVertices()) {
				Execution execution = vertex.getCurrentExecutionAttempt();
				notYetAcknowledgedTasks.add(execution.getAttemptId());
				rescaleCandidatesFutures.add(execution.scheduleReconfig(
					reconfigId, ReconfigOptions.UPDATE_GATES_ONLY, null, null));
			}
		}

		for (int subtaskIndex = 0; subtaskIndex < targetVertex.getTaskVertices().length; subtaskIndex++) {
			ExecutionVertex vertex = targetVertex.getTaskVertices()[subtaskIndex];
			Execution execution = vertex.getCurrentExecutionAttempt();
			if (!jobExecutionPlan.isAffectedTask(subtaskIndex)) {
				rescaleCandidatesFutures.add(
					execution.scheduleReconfig(reconfigId, ReconfigOptions.UPDATE_BOTH,
						null, null));
			} else {
				if (jobExecutionPlan.isSourceSubtask(subtaskIndex)) {
					// if is source, set keygroups to be checkpointed
					LOG.info("++++++ Task " + subtaskIndex + " set affected keys: "
						+ jobExecutionPlan.getAffectedKeygroupsForSource(subtaskIndex));
					rescaleCandidatesFutures.add(
						execution.scheduleReconfig(reconfigId, ReconfigOptions.PREPARE_AFFECTED_KEYGROUPS,
							null, jobExecutionPlan.getAffectedKeygroupsForSource(subtaskIndex)));
				} else {
					// if is destination, set the affected keygroups for destination
					rescaleCandidatesFutures.add(
						execution.scheduleReconfig(reconfigId, ReconfigOptions.PREPARE_AFFECTED_KEYGROUPS,
							null, jobExecutionPlan.getAffectedKeygroupsForDestination(subtaskIndex)));
				}
			}
		}

		FutureUtils
			.combineAll(rescaleCandidatesFutures)
			.whenComplete((ignored, failure) -> {
				if (failure != null) {
					failExecution(failure);
					throw new CompletionException(failure);
				}
				LOG.info("++++++ Rescale vertex Completed");
			})
			.thenRunAsync(() -> {
				try {
					checkpointCoordinator.stopCheckpointScheduler();

					checkpointId = checkpointCoordinator
						.triggerReconfigPoint(System.currentTimeMillis()).getCheckpointId();
					LOG.info("++++++ Make rescalepoint with checkpointId=" + checkpointId);
				} catch (Exception e) {
					failExecution(e);
					throw new CompletionException(e);
				}
			}, mainThreadExecutor);
	}

	private void scaleOutVertex(
			JobVertexID vertexID,
			List<JobVertexID> updatedUpstream,
			List<JobVertexID> updatedDownstream) {

		Map<JobVertexID, ExecutionJobVertex> tasks = executionGraph.getAllVertices();

		CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		checkNotNull(checkpointCoordinator);
		checkpointCoordinator.setReconfigpointAcknowledgeListener(this);

		this.targetVertex = tasks.get(vertexID);

		for (ExecutionVertex vertex : this.targetVertex.getTaskVertices()) {
			notYetAcknowledgedTasks.add(vertex.getCurrentExecutionAttempt().getAttemptId());
		}

		final Map<ReconfigOptions, List<ExecutionVertex>> rescaleCandidates = new HashMap<>();

		rescaleCandidates.put(ReconfigOptions.UPDATE_PARTITIONS_ONLY, new ArrayList<>());
		rescaleCandidates.put(ReconfigOptions.UPDATE_GATES_ONLY, new ArrayList<>());
//		rescaleCandidates.put(ReconfigOptions.RESCALE_BOTH, new ArrayList<>());

		for (JobVertexID jobId : updatedUpstream) {
			tasks.get(jobId).cleanBeforeRescale();

			rescaleCandidates
				.get(ReconfigOptions.UPDATE_PARTITIONS_ONLY)
				.addAll(Arrays.asList(tasks.get(jobId).getTaskVertices()));
		}

//		rescaleCandidates
//			.get(ReconfigOptions.RESCALE_BOTH)
//			.addAll(Arrays.asList(this.targetVertex.getTaskVertices()));

		for (JobVertexID jobId : updatedDownstream) {
			tasks.get(jobId).cleanBeforeRescale();

			rescaleCandidates
				.get(ReconfigOptions.UPDATE_GATES_ONLY)
				.addAll(Arrays.asList(tasks.get(jobId).getTaskVertices()));

			for (ExecutionVertex vertex : tasks.get(jobId).getTaskVertices()) {
				notYetAcknowledgedTasks.add(vertex.getCurrentExecutionAttempt().getAttemptId());
			}
		}

		// scale up given ejv, update involved edges & partitions
		this.createCandidates = this.targetVertex
			.scaleOut(executionGraph.getRpcTimeout(), executionGraph.getGlobalModVersion(), System.currentTimeMillis());

		for (JobVertexID downstreamID : updatedDownstream) {
			ExecutionJobVertex downstream = tasks.get(downstreamID);
			downstream.reconnectWithUpstream(this.targetVertex.getProducedDataSets());
		}
		executionGraph.updateNumOfTotalVertices();

		// state check
		checkState(targetVertex.getParallelism() == jobExecutionPlan.getNumOpenedSubtask(),
			String.format("parallelism in targetVertex %d is not equal to number of executors %d",
				targetVertex.getParallelism(), jobExecutionPlan.getNumOpenedSubtask()));

		// required resource for all created vertices
		Collection<CompletableFuture<Execution>> allocateSlotFutures = new ArrayList<>(this.createCandidates.size());

		for (ExecutionVertex vertex : this.createCandidates) {
			Execution executionAttempt = vertex.getCurrentExecutionAttempt();
			allocateSlotFutures.add(executionAttempt.allocateAndAssignSlotForExecution(reconfigId));
		}

		// rescale existed vertices from upstream to downstream
		CompletableFuture<Void> rescaleCompleted = FutureUtils
			.combineAll(allocateSlotFutures)
			.whenComplete((executions, failure) -> {
				if (failure != null) {
					failExecution(failure);
					throw new CompletionException(failure);
				}
				LOG.info("++++++ allocate resource for vertices Completed");

				allocatedExecutions = executions;
			})
			.thenApplyAsync((ignored) -> {
				try {
					Collection<CompletableFuture<Void>> rescaleCandidatesFutures = new ArrayList<>();

					for (Map.Entry<ReconfigOptions, List<ExecutionVertex>> entry : rescaleCandidates.entrySet()) {
						for (ExecutionVertex vertex : entry.getValue()) {
							Execution execution = vertex.getCurrentExecutionAttempt();
							rescaleCandidatesFutures.add(execution.scheduleReconfig(reconfigId, entry.getKey(),
								null, null));
						}
					}

					for (int subtaskIndex = 0; subtaskIndex < targetVertex.getTaskVertices().length; subtaskIndex++) {
						if (!jobExecutionPlan.isAffectedTask(subtaskIndex)) {
							ExecutionVertex vertex = targetVertex.getTaskVertices()[subtaskIndex];
							Execution execution = vertex.getCurrentExecutionAttempt();

							rescaleCandidatesFutures.add(execution
								.scheduleReconfig(reconfigId, ReconfigOptions.UPDATE_BOTH,
									null, null));
						}
					}

					return FutureUtils
						.completeAll(rescaleCandidatesFutures)
						.whenComplete((ignored2, failure) -> {
							if (failure != null) {
								failExecution(failure);
								throw new CompletionException(failure);
							}
							LOG.info("++++++ Rescale vertex Completed");
						});
				} catch (Exception cause) {
					failExecution(cause);
					throw new CompletionException(cause);
				}
			}, mainThreadExecutor)
			.thenCompose(Function.identity());

		// trigger rescale point
		rescaleCompleted
			.thenRunAsync(() -> {
				try {
					checkpointCoordinator.stopCheckpointScheduler();

					checkpointId = checkpointCoordinator
						.triggerReconfigPoint(System.currentTimeMillis()).getCheckpointId();
					LOG.info("++++++ Make rescalepoint with checkpointId=" + checkpointId);
				} catch (Exception e) {
					failExecution(e);
					throw new CompletionException(e);
				}
			}, mainThreadExecutor);
	}

	private void scaleInVertex(
			JobVertexID vertexID,
			List<JobVertexID> updatedUpstream,
			List<JobVertexID> updatedDownstream) throws ExecutionGraphException {

		Map<JobVertexID, ExecutionJobVertex> tasks = executionGraph.getAllVertices();

		CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		checkNotNull(checkpointCoordinator);
		checkpointCoordinator.setReconfigpointAcknowledgeListener(this);

		this.targetVertex = tasks.get(vertexID);

		for (ExecutionVertex vertex : this.targetVertex.getTaskVertices()) {
			notYetAcknowledgedTasks.add(vertex.getCurrentExecutionAttempt().getAttemptId());
		}

		// scale in by given ejv, update involved edges & partitions
		this.removedCandidates = this.targetVertex
			.scaleIn(executionGraph.getRpcTimeout(), executionGraph.getGlobalModVersion(), System.currentTimeMillis());

		for (JobVertexID upstreamID : updatedUpstream) {
			ExecutionJobVertex upstream = tasks.get(upstreamID);
			upstream.resetProducedDataSets();
			targetVertex.reconnectWithUpstream(upstream.getProducedDataSets());
		}


		for (JobVertexID downstreamID : updatedDownstream) {
			ExecutionJobVertex downstream = tasks.get(downstreamID);
			downstream.reconnectWithUpstream(this.targetVertex.getProducedDataSets());
		}
		executionGraph.updateNumOfTotalVertices();

		// rescale upstream and downstream
		final Collection<CompletableFuture<Void>> rescaleCandidatesFutures = new ArrayList<>();

		for (JobVertexID jobId : updatedUpstream) {
			tasks.get(jobId).cleanBeforeRescale();

			for (ExecutionVertex vertex : tasks.get(jobId).getTaskVertices()) {
				Execution execution = vertex.getCurrentExecutionAttempt();
				rescaleCandidatesFutures.add(execution
					.scheduleReconfig(reconfigId, ReconfigOptions.UPDATE_PARTITIONS_ONLY,
						null, null));
			}
		}

		for (JobVertexID jobId : updatedDownstream) {
			tasks.get(jobId).cleanBeforeRescale();

			for (ExecutionVertex vertex : tasks.get(jobId).getTaskVertices()) {
				Execution execution = vertex.getCurrentExecutionAttempt();
				notYetAcknowledgedTasks.add(execution.getAttemptId());
				rescaleCandidatesFutures.add(execution
					.scheduleReconfig(reconfigId, ReconfigOptions.UPDATE_GATES_ONLY,
						null, null));
			}
		}

		FutureUtils
			.combineAll(rescaleCandidatesFutures)
			.whenComplete((ignored, failure) -> {
				if (failure != null) {
					failExecution(failure);
					throw new CompletionException(failure);
				}
				LOG.info("++++++ Rescale vertex Completed");
			})
			.thenRunAsync(() -> {
				try {
					checkpointCoordinator.stopCheckpointScheduler();

					checkpointId = checkpointCoordinator
						.triggerReconfigPoint(System.currentTimeMillis()).getCheckpointId();
					LOG.info("++++++ Make rescalepoint with checkpointId=" + checkpointId);
				} catch (Exception e) {
					failExecution(e);
					throw new CompletionException(e);
				}
			}, mainThreadExecutor);
	}

	private void handleCollectedStates(Map<OperatorID, OperatorState> operatorStates) throws Exception {
		switch (actionType) {
			case REPARTITION:
				assignNewState(operatorStates);
				break;
			case SCALE_OUT:
				deployCreatedExecution(operatorStates);
				break;
			case SCALE_IN:
				cancelOldExecution(operatorStates);
				break;
			default:
				throw new IllegalStateException("illegal action type");
		}
	}

	private void assignNewState(Map<OperatorID, OperatorState> operatorStates) throws ExecutionGraphException {
		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(targetVertex.getJobVertexId(), targetVertex);

		// TODO: assign unreplicated states to tasks.
//		StateAssignmentOperation stateAssignmentOperation =
//			new StateAssignmentOperation(checkpointId, tasks, operatorStates,
//				true, REPARTITION_STATE);
//		stateAssignmentOperation.setRedistributeStrategy(jobExecutionPlan);
//
//		LOG.info("++++++ start to assign states");
//		stateAssignmentOperation.assignStates();

		Collection<CompletableFuture<Void>> rescaledFuture = new ArrayList<>(targetVertex.getTaskVertices().length);

		for (int i = 0; i < targetVertex.getTaskVertices().length; i++) {
			ExecutionVertex vertex  = targetVertex.getTaskVertices()[i];
			Execution executionAttempt = vertex.getCurrentExecutionAttempt();

			CompletableFuture<Void> scheduledRescale;

			if (jobExecutionPlan.isAffectedTask(i)) {
				scheduledRescale = executionAttempt.scheduleReconfig(reconfigId,
					ReconfigOptions.UPDATE_REDISTRIBUTE_STATE,
					jobExecutionPlan.getAlignedKeyGroupRange(i),
					jobExecutionPlan.getIdInModel(i));
			} else {
				scheduledRescale = executionAttempt.scheduleReconfig(reconfigId,
					ReconfigOptions.UPDATE_KEYGROUP_RANGE_ONLY,
					jobExecutionPlan.getAlignedKeyGroupRange(i), null);
			}

			rescaledFuture.add(scheduledRescale);
		}
		LOG.info("++++++ Assign new state futures created");

		FutureUtils
			.combineAll(rescaledFuture)
			.thenRunAsync(() -> {
				LOG.info("++++++ Assign new state for repartition Completed");
				CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

				checkNotNull(checkpointCoordinator);
				if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
					checkpointCoordinator.startCheckpointScheduler();
				}

				clean();

				// notify streamSwitch that change is finished
				controllerAdaptor.onMigrationExecutorsStopped(targetVertex.getJobVertexId());
				controllerAdaptor.onChangeImplemented(targetVertex.getJobVertexId());
			}, mainThreadExecutor);
	}

	private void deployCreatedExecution(Map<OperatorID, OperatorState> operatorStates)
		throws JobException, ExecutionGraphException {
		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(targetVertex.getJobVertexId(), targetVertex);

		StateAssignmentOperation stateAssignmentOperation =
			new StateAssignmentOperation(checkpointId, tasks, operatorStates,
				true, REPARTITION_STATE, backupKeyGroups);
		stateAssignmentOperation.setRedistributeStrategy(jobExecutionPlan);

		LOG.info("++++++ start to assign states");
		stateAssignmentOperation.assignStates();

		System.out.println("target vertex: " + targetVertex.getTaskVertices().length
			+ " allocated vertex: " + allocatedExecutions.size());

		// update existed tasks state
		Collection<CompletableFuture<Void>> rescaledFuture =
			new ArrayList<>(targetVertex.getTaskVertices().length - allocatedExecutions.size());

		for (int i = 0; i < targetVertex.getTaskVertices().length; i++) {
			ExecutionVertex vertex = targetVertex.getTaskVertices()[i];
			KeyGroupRange keyGroupRange = jobExecutionPlan.getAlignedKeyGroupRange(i);

			CompletableFuture<Void> scheduledRescale;

			if (jobExecutionPlan.isAffectedTask(i)) {
				int idInModel = jobExecutionPlan.getIdInModel(i);

				if (createCandidates.contains(vertex)) {
					scheduledRescale = vertex.getCurrentExecutionAttempt().deploy(keyGroupRange, idInModel);
				} else {
					Execution executionAttempt = vertex.getCurrentExecutionAttempt();

					scheduledRescale = executionAttempt.scheduleReconfig(reconfigId,
						ReconfigOptions. UPDATE_REDISTRIBUTE_STATE,
						keyGroupRange, idInModel);
				}
			} else {
				Execution executionAttempt = vertex.getCurrentExecutionAttempt();

				scheduledRescale = executionAttempt.scheduleReconfig(reconfigId,
					ReconfigOptions.UPDATE_KEYGROUP_RANGE_ONLY,
					keyGroupRange, null);
			}

			rescaledFuture.add(scheduledRescale);
		}

		FutureUtils
			.combineAll(rescaledFuture)
			.thenRunAsync(() -> {
				LOG.info("++++++ Deploy vertex and rescale existing vertices completed");
				CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

				checkNotNull(checkpointCoordinator);
				checkpointCoordinator.addVertices(createCandidates.toArray(new ExecutionVertex[0]), false);

				if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
					checkpointCoordinator.startCheckpointScheduler();
				}

				clean();

				// notify streamSwitch that change is finished
				controllerAdaptor.onMigrationExecutorsStopped(targetVertex.getJobVertexId());
				controllerAdaptor.onChangeImplemented(targetVertex.getJobVertexId());
			}, mainThreadExecutor);
	}

	private void cancelOldExecution(Map<OperatorID, OperatorState> operatorStates) throws ExecutionGraphException {
		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(targetVertex.getJobVertexId(), targetVertex);

		StateAssignmentOperation stateAssignmentOperation =
			new StateAssignmentOperation(checkpointId, tasks, operatorStates, true, REPARTITION_STATE, backupKeyGroups);
		stateAssignmentOperation.setRedistributeStrategy(jobExecutionPlan);

		LOG.info("++++++ start to assign states");
		stateAssignmentOperation.assignStates();

		Collection<CompletableFuture<Void>> rescaledFuture = new ArrayList<>(targetVertex.getTaskVertices().length);
		Map<Integer, List<Integer>> partitionAssignment = jobExecutionPlan.getPartitionAssignment();

		for (int i = 0; i < targetVertex.getTaskVertices().length; i++) {
			ExecutionVertex vertex  = targetVertex.getTaskVertices()[i];
			Execution executionAttempt = vertex.getCurrentExecutionAttempt();

			CompletableFuture<Void> scheduledRescale;

			if (partitionAssignment.get(i).size() == 0) {
				System.out.println("none keygroup assigned for current jobvertex: " + vertex.toString());
				scheduledRescale = executionAttempt.scheduleReconfig(reconfigId,
					ReconfigOptions.UPDATE_REDISTRIBUTE_STATE, null, null);
			} else {
				scheduledRescale = executionAttempt.scheduleReconfig(reconfigId,
					ReconfigOptions.UPDATE_REDISTRIBUTE_STATE,
					jobExecutionPlan.getAlignedKeyGroupRange(i), null);
			}
			rescaledFuture.add(scheduledRescale);
		}
		LOG.info("++++++ Assign new state futures created");

		FutureUtils
			.combineAll(rescaledFuture)
			.thenRunAsync(() -> {
				LOG.info("++++++ Scale in and assign new state Completed");
				CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

				checkNotNull(checkpointCoordinator);
				checkpointCoordinator.dropVertices(removedCandidates.toArray(new ExecutionVertex[0]), false);

				if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
					checkpointCoordinator.startCheckpointScheduler();
				}

				clean();

				// notify streamSwitch that change is finished
				controllerAdaptor.onMigrationExecutorsStopped(targetVertex.getJobVertexId());
				controllerAdaptor.onChangeImplemented(targetVertex.getJobVertexId());
			}, mainThreadExecutor);
	}

	private void failExecution(Throwable throwable) {
		LOG.info("++++++ Rescale failed with err: ", throwable);
		clean();
	}

	private void clean() {
		inProcess = false;
		notYetAcknowledgedTasks.clear();
	}


	@Override
	public void onReceiveReconfigpointAcknowledge(ExecutionAttemptID attemptID, PendingCheckpoint checkpoint) {
		if (inProcess && checkpointId == checkpoint.getCheckpointId()) {

			CompletableFuture.runAsync(() -> {
				LOG.info("++++++ Received Rescalepoint Acknowledgement");
				try {
					synchronized (lock) {
						if (inProcess) {
							if (notYetAcknowledgedTasks.isEmpty()) {
								// late come in snapshot, ignore it
								return;
							}

							notYetAcknowledgedTasks.remove(attemptID);

							if (notYetAcknowledgedTasks.isEmpty()) {
								// receive all required snapshot, force streamSwitch to update metrics
//								streamSwitchAdaptor.onForceRetrieveMetrics(targetVertex.getJobVertexId());
								// only update executor mappings at this time.
//								streamSwitchAdaptor.onMigrationExecutorsStopped(targetVertex.getJobVertexId());

								LOG.info("++++++ handle operator states");
								handleCollectedStates(new HashMap<>(checkpoint.getOperatorStates()));
							}
						}
					}
				} catch (Exception e) {
					failExecution(e);
					throw new CompletionException(e);
				}
			}, mainThreadExecutor);
		}
	}

	public JobStatusListener createActivatorDeactivator() {
		if (jobStatusListener == null) {
			jobStatusListener = new JobRescaleCoordinatorDeActivator(this);
		}

		return jobStatusListener;
	}

	private static class JobRescaleCoordinatorDeActivator implements JobStatusListener {

		private final JobStateCoordinator coordinator;

		public JobRescaleCoordinatorDeActivator(JobStateCoordinator coordinator) {
			this.coordinator = checkNotNull(coordinator);
		}

		@Override
		public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error) {
			if (newJobStatus == JobStatus.RUNNING) {
				coordinator.start();
			} else {
				coordinator.stop();
			}
		}
	}
}
