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
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.runtime.spector.controller.impl.ControllerAdaptor;
import org.apache.flink.runtime.util.profiling.ReconfigurationProfiler;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.Operation.DISPATCH_STATE_TO_STANDBY_TASK;
import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.Operation.REPARTITION_STATE;
import static org.apache.flink.runtime.clusterframework.types.TaskManagerSlot.State.FREE;
import static org.apache.flink.runtime.spector.JobStateCoordinator.AckStatus.DONE;
import static org.apache.flink.runtime.spector.JobStateCoordinator.AckStatus.FAILED;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;


/**
 * General State Migration Steps:
 * 1. on trigger state migration, update jobgraph + execution graph.
 * 2. trigger reconfig point to get the latest state snapshot to redistribute state from src to dst.
 * 2. dispatch snapshoted state to standby tasks. => On all standby tasks ack to coordinator
 * 3.
 */
public class JobStateCoordinator implements JobReconfigAction, CheckpointProgressListener {

	private static final Logger LOG = LoggerFactory.getLogger(JobStateCoordinator.class);
	public final static String REPLICATE_KEYS_FILTER = "spector.replicate_keys_filter";
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

	/**
	 * standby executions for backup state maintenance of each operator
	 * Operator -> [BackupTask1, BackTask2, ...]
	 */
	private final HashMap<JobVertexID, List<ExecutionVertex>> standbyExecutionVertexes;

	/**
	 * keys to replicate
	 */
	private final Set<Integer> backupKeyGroups;

	private final Map<InstanceID, List<TaskManagerSlot>> slotsMap;

	// for replicating states confirmation
	private final Map<ExecutionAttemptID, ExecutionVertex> pendingAckStandbyTasks;

	private final ReconfigurationProfiler reconfigurationProfiler;


	public enum AckStatus {
		DONE,
		FAILED
	}


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
		this.backupKeyGroups = new HashSet<>();
		initBackupKeyGroups(executionGraph.getJobConfiguration());
		this.slotsMap = new HashMap<>();
		this.pendingAckStandbyTasks = new HashMap<>();

		this.reconfigurationProfiler = new ReconfigurationProfiler(executionGraph.getJobConfiguration());
	}

	public void initBackupKeyGroups(Configuration jobConfiguration) {
		int filer = jobConfiguration.getInteger(REPLICATE_KEYS_FILTER, 1);
		String targetOperator = jobConfiguration.getString("controller.target.operators", "flatmap");
		if (filer == 0) return;
		for (Map.Entry<JobVertexID, ExecutionJobVertex> entry : executionGraph.getAllVertices().entrySet()) {
			int maxParallelism = entry.getValue().getMaxParallelism();
			// TODO: hard coded this part, but we need to make it work as a configurable field
			if (entry.getValue().getName().toLowerCase().contains(targetOperator)) {
				for (int i = 0; i < maxParallelism; i++) {
					if (i % filer == 0) {
						backupKeyGroups.add(i);
					}
				}
			}
		}
	}

	public void setSlotsMap(CompletableFuture<Collection<TaskManagerSlot>> allSlots) {
		allSlots.thenAccept(taskManagerSlots -> {
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
		// TODO
	}

	/**
	 * create backup tasks on task manager, maintain backup state for more efficient state management.
	 * @param newExecutionJobVerticesTopological
	 */
	public void notifyNewVertices(List<ExecutionJobVertex> newExecutionJobVerticesTopological) {
		final ArrayList<CompletableFuture<Void>> schedulingFutures = new ArrayList<>();

		LOG.info("++++++ Waiting for standby tasks for: " + newExecutionJobVerticesTopological);

		final Collection<CompletableFuture<Void>> currentExecutionFutures = new ArrayList<>();

		for (ExecutionJobVertex executionJobVertex : newExecutionJobVerticesTopological) {
			for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
				currentExecutionFutures.add(
					// TODO: Anti-affinity constraint
					CompletableFuture.runAsync(() -> waitForExecutionToReachRunningState(executionVertex)));
			}
		}

		// once all execution of current executionJobVertex are in running state, deploy backup executions
		FutureUtils.combineAll(currentExecutionFutures).whenComplete((ignored, t) -> {
			for (ExecutionJobVertex executionJobVertex : newExecutionJobVerticesTopological) {
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
			}
		}).thenRunAsync(() -> {
			final CompletableFuture<Void> allSchedulingFutures = FutureUtils.waitForAll(schedulingFutures);
			allSchedulingFutures.whenComplete((Void ignored2, Throwable t2) -> {
				Preconditions.checkState(standbyExecutionVertexes.size() == newExecutionJobVerticesTopological.size(),
					"++++++ Inconsistent standby tasks number");
				if (t2 != null) {
					LOG.warn("Scheduling of standby tasks failed. Cancelling the scheduling of standby tasks.");
					for (ExecutionJobVertex executionJobVertex : newExecutionJobVerticesTopological) {
						cancelStandbyExecution(executionJobVertex);
					}
				}
			});
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

		StateAssignmentOperation stateAssignmentOperation = checkpoint.getProperties().getCheckpointType() == CheckpointType.RECONFIGPOINT ?
			new StateAssignmentOperation(checkpoint.getCheckpointID(), tasks, operatorStates,
				true, REPARTITION_STATE, backupKeyGroups)
			: new StateAssignmentOperation(checkpoint.getCheckpointID(), tasks, operatorStates,
				true, DISPATCH_STATE_TO_STANDBY_TASK, backupKeyGroups);
		checkNotNull(jobExecutionPlan, "jobExecutionPlan should not be null.");
		stateAssignmentOperation.setRedistributeStrategy(jobExecutionPlan);
		stateAssignmentOperation.setPendingStandbyTasks(pendingAckStandbyTasks);

		stateAssignmentOperation.assignStates();
		// check replication progress once, in case no keys are replicated.
		checkReplicationProgress();
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
		ExecutionState executionState;
		do {
			executionState = executionVertex.getExecutionState();
		} while (executionState == ExecutionState.CREATED ||
			executionState == ExecutionState.SCHEDULED ||
			executionState == ExecutionState.DEPLOYING);
	}

	@Override
	public void onCompleteCheckpoint(CompletedCheckpoint checkpoint) {
		if (checkpoint.getProperties().getCheckpointType() == CheckpointType.RECONFIGPOINT) {
			reconfigurationProfiler.onSyncEnd();
		}
		checkNotNull(checkpoint);
		LOG.info("++++++ checkpoint complete, start to dispatch state to replica");
		reconfigurationProfiler.onReplicationStart();
		dispatchLatestCheckpointedStateToStandbyTasks(checkpoint);
//		if (checkpoint.getProperties().getCheckpointType() == CheckpointType.RECONFIGPOINT) {
//			LOG.info("++++++ redistribute operator states");
//			handleCollectedStates(new HashMap<>(checkpoint.getOperatorStates()));
//		}
	}

	public void onAckReplication(ExecutionAttemptID executionAttemptID, AckStatus ackStatus) {
		if (ackStatus == DONE) {
			pendingAckStandbyTasks.remove(executionAttemptID);
			LOG.info("++++++ Receive Ack from execution: " + executionAttemptID);
			checkReplicationProgress();
		} else if (ackStatus == FAILED) {
			throw new RuntimeException("++++++ Replication failed for some reason.");
		}
	}

	private void checkReplicationProgress() {
		if (pendingAckStandbyTasks.isEmpty()) {
			LOG.info("++++++ Dispatch state to replica completed.");
			reconfigurationProfiler.onReplicationEnd();
			if (inProcess) {
				try {
					assignNewState();
				} catch (ExecutionGraphException e) {
					throw new RuntimeException(e);
				}
			}
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
		reconfigurationProfiler.onReconfigurationStart();
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
					reconfigId, ReconfigOptions.UPDATE_PARTITIONS_ONLY, null, null, null));
			}
		}

		for (JobVertexID jobId : updatedDownstream) {
			tasks.get(jobId).cleanBeforeRescale();

			for (ExecutionVertex vertex : tasks.get(jobId).getTaskVertices()) {
				Execution execution = vertex.getCurrentExecutionAttempt();
				notYetAcknowledgedTasks.add(execution.getAttemptId());
				rescaleCandidatesFutures.add(execution.scheduleReconfig(
					reconfigId, ReconfigOptions.UPDATE_GATES_ONLY, null, null, null));
			}
		}

		for (int subtaskIndex = 0; subtaskIndex < targetVertex.getTaskVertices().length; subtaskIndex++) {
			ExecutionVertex vertex = targetVertex.getTaskVertices()[subtaskIndex];
			Execution execution = vertex.getCurrentExecutionAttempt();
			if (!jobExecutionPlan.isAffectedTask(subtaskIndex)) {
				rescaleCandidatesFutures.add(
					execution.scheduleReconfig(reconfigId, ReconfigOptions.UPDATE_BOTH,
						null, null, null));
			} else {
				if (jobExecutionPlan.isSourceSubtask(subtaskIndex) || jobExecutionPlan.isDestinationSubtask(subtaskIndex)) {
					List<Integer> srcKeygroups = jobExecutionPlan.isSourceSubtask(subtaskIndex) ?
						jobExecutionPlan.getAffectedKeygroupsForSource(subtaskIndex) : null;
					List<Integer> dstKeygroups = jobExecutionPlan.isDestinationSubtask(subtaskIndex) ?
						jobExecutionPlan.getAffectedKeygroupsForDestination(subtaskIndex) : null;
					// if is source, set keygroups to be checkpointed
					LOG.info("++++++ Task " + subtaskIndex + " set affected keys: migrate out => "
						+ srcKeygroups + " : in => "
						+ dstKeygroups);
					rescaleCandidatesFutures.add(execution.scheduleReconfig(
							reconfigId,
							ReconfigOptions.PREPARE_AFFECTED_KEYGROUPS,
							null,
							srcKeygroups,
							dstKeygroups));
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
					reconfigurationProfiler.onSyncStart();
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
				assignNewState();
				break;
			case SCALE_OUT:
				throw new UnsupportedOperationException();
//				break;
			case SCALE_IN:
				throw new UnsupportedOperationException();
//				break;
			default:
				throw new IllegalStateException("illegal action type");
		}
	}

	private void assignNewState() throws ExecutionGraphException {
		reconfigurationProfiler.onUpdateStart();

//		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
//		tasks.put(targetVertex.getJobVertexId(), targetVertex);

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
					jobExecutionPlan.getAlignedKeyGroupRange(i), null, null);
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
				reconfigurationProfiler.onUpdateEnd();
				reconfigurationProfiler.onReconfigurationEnd();
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
		pendingAckStandbyTasks.clear();
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
