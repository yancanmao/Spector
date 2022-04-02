package org.apache.flink.runtime.spector;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.*;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.spector.reconfig.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.Operation.DISPATCH_STATE_TO_STANDBY_TASK;
import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.Operation.REPARTITION_STATE;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Instantiated on JobMaster, each job has one JobMaster
 * 1. initialize
 * 2. implement replication
 * 3. implement repartition
 * 4. implement restore
 */
public class JobStateCoordinator implements CheckpointProgressListener, JobReconfigAction {

	static final Logger LOG = LoggerFactory.getLogger(JobStateCoordinator.class);

	/**
	 * For output key remapping during load balancing/scaling
	 */
	private final JobGraph jobGraph;

	/**
	 * For new task creation / old task removal, state repartition
	 */
	private ExecutionGraph executionGraph;

	/**
	 * For key partitioner update
	 */
	private final JobGraphUpdater jobGraphUpdater;

	/**
	 * standby executions for backup state maintenance of each operator
	 * Operator -> [BackupTask1, BackTask2, ...]
	 */
	private final HashMap<JobVertexID, List<ExecutionVertex>> standbyExecutionVertexes;


	// states for reconfiguration

	private final Object lock = new Object();

	// mutable fields
	private volatile boolean inProcess;

	private volatile long checkpointId;

	private volatile ActionType actionType;

	private volatile ReconfigID reconfigId;

	private volatile JobExecutionPlan jobExecutionPlan;

	private final List<ExecutionAttemptID> notYetAcknowledgedTasks;

	private volatile ExecutionJobVertex targetVertex;

	private ComponentMainThreadExecutor mainThreadExecutor;

	private final SimpleController simpleController;

	private JobStatusListener jobStatusListener;

	/**
	 *
	 * @param jobGraph
	 * @param executionGraph
	 * @param userCodeLoader
	 */
	public JobStateCoordinator(
		JobGraph jobGraph,
		ExecutionGraph executionGraph,
		ClassLoader userCodeLoader) {
		this.jobGraph = jobGraph;
		this.executionGraph = executionGraph;
		this.jobGraphUpdater = JobGraphUpdater.instantiate(jobGraph, userCodeLoader);
		this.standbyExecutionVertexes = new HashMap<>();
		this.notYetAcknowledgedTasks = new ArrayList<>();

		// inject a listener into CheckpointCoordinator to manage received checkpoints
		checkNotNull(executionGraph.getCheckpointCoordinator());
		executionGraph.getCheckpointCoordinator().setCheckpointProgressListener(this);

		this.simpleController = new SimpleController(this, executionGraph);
	}


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
				if (t == null) {
					// create a set of executionVertex
					int numBackupTasks = 1;
					List<ExecutionVertex> createCandidates = executionJobVertex.addStandbyExecutionVertex(
						executionGraph.getRpcTimeout(),
						executionGraph.getGlobalModVersion(),
						System.currentTimeMillis(),
						1);
					standbyExecutionVertexes.put(executionJobVertex.getJobVertexId(), createCandidates); // TODO: need to create number of tasks according to number of nodes in the cluster.

					checkState(createCandidates.size() == numBackupTasks,
						"Inconsistent number of execution vertices are created");

					// schedule for execution
					for (ExecutionVertex vertex : createCandidates) {
						Execution executionAttempt = vertex.getCurrentExecutionAttempt();
						// TODO: schedule backup tasks to different task managers for execution
						schedulingFutures.add(executionAttempt.scheduleForExecution());
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
				true, DISPATCH_STATE_TO_STANDBY_TASK);
		stateAssignmentOperation.setRedistributeStrategy(jobExecutionPlan);

		stateAssignmentOperation.assignStates();
	}

	// TODO: see if other place need to add this
	public CompletableFuture<?> cancelStandbyExecution(ExecutionJobVertex executionJobVertex) {
		if (!standbyExecutionVertexes.get(executionJobVertex.getJobVertexId()).isEmpty()) {
			LOG.debug(String.format("Cancelling standby execution %s", this));
			final ExecutionVertex standbyExecution = standbyExecutionVertexes.get(executionJobVertex.getJobVertexId()).remove(0);
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
	public void onReceiveRescalepointAcknowledge(ExecutionAttemptID attemptID, PendingCheckpoint checkpoint) {
		if (inProcess && checkpointId == checkpoint.getCheckpointId()) {

			CompletableFuture.runAsync(() -> {
				LOG.info("++++++ Received ReconfigPoint Acknowledgement");
				try {
					synchronized (lock) {
						if (inProcess) {
							if (notYetAcknowledgedTasks.isEmpty()) {
								// late come in snapshot, ignore it
								return;
							}

							notYetAcknowledgedTasks.remove(attemptID);

							if (notYetAcknowledgedTasks.isEmpty()) {
								// receive all required snapshot, force streamSwitch to update metrices
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

	// for reconfiguration

	public void init(ComponentMainThreadExecutor mainThreadExecutor) {
		this.mainThreadExecutor = mainThreadExecutor;
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

	@Override
	public JobGraph getJobGraph() {
		return jobGraph;
	}

	@Override
	public void scaleOut(JobVertexID vertexID, int newParallelism, JobExecutionPlan jobExecutionPlan) {
		throw new UnsupportedOperationException("Scaling is not supported");
	}

	@Override
	public void scaleIn(JobVertexID vertexID, int newParallelism, JobExecutionPlan jobExecutionPlan) {
		throw new UnsupportedOperationException("Scaling is not supported");
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

		LOG.info("++++++ repartition job with reconfigID: " + reconfigId +
			", taskID" + vertexID +
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

		this.targetVertex = tasks.get(vertexID);

		for (ExecutionVertex vertex : this.targetVertex.getTaskVertices()) {
			notYetAcknowledgedTasks.add(vertex.getCurrentExecutionAttempt().getAttemptId());
		}

		// state check
		checkState(targetVertex.getParallelism() == jobExecutionPlan.getNumOpenedSubtask(),
			String.format("parallelism in targetVertex %d is not equal to number of executors %d",
				targetVertex.getParallelism(), jobExecutionPlan.getNumOpenedSubtask()));

		// rescale upstream and downstream, TDOO: do not need to update target partition and downstream gates
		final Collection<CompletableFuture<Void>> afferctedTaskExecutionsFuture = new ArrayList<>();

		for (JobVertexID jobId : updatedUpstream) {
			tasks.get(jobId).cleanBeforeRescale();

			for (ExecutionVertex vertex : tasks.get(jobId).getTaskVertices()) {
				Execution execution = vertex.getCurrentExecutionAttempt();
				afferctedTaskExecutionsFuture.add(execution.scheduleReconfig(reconfigId, ReconfigOptions.RESCALE_PARTITIONS_ONLY, null));
			}
		}

		for (JobVertexID jobId : updatedDownstream) {
			tasks.get(jobId).cleanBeforeRescale();

			for (ExecutionVertex vertex : tasks.get(jobId).getTaskVertices()) {
				Execution execution = vertex.getCurrentExecutionAttempt();
				notYetAcknowledgedTasks.add(execution.getAttemptId());
				afferctedTaskExecutionsFuture.add(execution.scheduleReconfig(reconfigId, ReconfigOptions.RESCALE_GATES_ONLY, null));
			}
		}

		for (int subtaskIndex = 0; subtaskIndex < targetVertex.getTaskVertices().length; subtaskIndex++) {
			// update non-state-migrated targeting tasks
			if (!jobExecutionPlan.isSubtaskModified(subtaskIndex)) {
				ExecutionVertex vertex = targetVertex.getTaskVertices()[subtaskIndex];
				Execution execution = vertex.getCurrentExecutionAttempt();

				afferctedTaskExecutionsFuture.add(
					execution.scheduleReconfig(reconfigId, ReconfigOptions.RESCALE_BOTH, null));
			}
		}

		FutureUtils
			.combineAll(afferctedTaskExecutionsFuture)
			.whenComplete((ignored, failure) -> {
				if (failure != null) {
					failExecution(failure);
					throw new CompletionException(failure);
				}
				LOG.info("++++++ Task output key mapping update completed");
			})
			.thenRunAsync(() -> {
				try {
					checkpointCoordinator.stopCheckpointScheduler();

					checkpointId = checkpointCoordinator.triggerReconfigPoint(System.currentTimeMillis()).getCheckpointId();
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
				throw new UnsupportedOperationException("Scaling out is not supported");
			case SCALE_IN:
				throw new UnsupportedOperationException("Scaling in is not supported");
			default:
				throw new IllegalStateException("illegal action type");
		}
	}

	private void assignNewState(Map<OperatorID, OperatorState> operatorStates) throws ExecutionGraphException {
		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(targetVertex.getJobVertexId(), targetVertex);

		StateAssignmentOperation stateAssignmentOperation =
			new StateAssignmentOperation(checkpointId, tasks, operatorStates, true, REPARTITION_STATE);
		stateAssignmentOperation.setRedistributeStrategy(jobExecutionPlan);

		LOG.info("++++++ start to assign states");
		stateAssignmentOperation.assignStates();

//		// update keygroup range in replicas in advance.
//		Collection<CompletableFuture<Void>> replicaUpdateFuture = new ArrayList<>(targetVertex.getStandbyExecutionVertexs().size());
//
//		for (ExecutionVertex standbyVertex : targetVertex.getStandbyExecutionVertexs()) {
//			Execution executionAttempt = standbyVertex.getCurrentExecutionAttempt();
//			CompletableFuture<Void> scheduledUpdate;
//			scheduledUpdate = executionAttempt.scheduleReconfig(reconfigId,
//				ReconfigOptions.RESCALE_KEYGROUP_RANGE_ONLY,
//				jobExecutionPlan.getAlignedKeyGroupRangeForStandby());
//			replicaUpdateFuture.add(scheduledUpdate);
//		}
//
//		FutureUtils.combineAll(replicaUpdateFuture)
//			.thenRunAsync(() -> {
//				// execution state update
//				try {
//					Collection<CompletableFuture<Void>> taskUpdateFuture = new ArrayList<>(targetVertex.getTaskVertices().length);
//
//					for (int i = 0; i < targetVertex.getTaskVertices().length; i++) {
//						ExecutionVertex vertex = targetVertex.getTaskVertices()[i];
//						Execution executionAttempt = vertex.getCurrentExecutionAttempt();
//
//						CompletableFuture<Void> scheduledUpdate;
//
//						if (jobExecutionPlan.isSubtaskModified(i)) {
//							scheduledUpdate = executionAttempt.scheduleReconfig(reconfigId,
//								ReconfigOptions.RESCALE_REDISTRIBUTE,
//								jobExecutionPlan.getAlignedKeyGroupRange(i));
//						} else {
//							scheduledUpdate = executionAttempt.scheduleReconfig(reconfigId,
//								ReconfigOptions.RESCALE_KEYGROUP_RANGE_ONLY,
//								jobExecutionPlan.getAlignedKeyGroupRange(i));
//						}
//
//						taskUpdateFuture.add(scheduledUpdate);
//
//						FutureUtils
//							.combineAll(taskUpdateFuture)
//							.thenRunAsync(() -> {
//								LOG.info("++++++ Assign new state for repartition Completed");
//								CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
//
//								checkNotNull(checkpointCoordinator);
//								if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
//									checkpointCoordinator.startCheckpointScheduler();
//								}
//
//								clean();
//
//								simpleController.onMigrationCompleted();
//							}, mainThreadExecutor);
//					}
//					LOG.info("++++++ Assign new state futures created");
//				} catch (Exception e) {
//					failExecution(e);
//				}}, mainThreadExecutor);
		Collection<CompletableFuture<Void>> taskUpdateFuture = new ArrayList<>(targetVertex.getTaskVertices().length);

		for (int i = 0; i < targetVertex.getTaskVertices().length; i++) {
			ExecutionVertex vertex = targetVertex.getTaskVertices()[i];
			Execution executionAttempt = vertex.getCurrentExecutionAttempt();

			CompletableFuture<Void> scheduledUpdate;

			if (jobExecutionPlan.isSubtaskModified(i)) {
				scheduledUpdate = executionAttempt.scheduleReconfig(reconfigId,
					ReconfigOptions.RESCALE_REDISTRIBUTE,
					jobExecutionPlan.getAlignedKeyGroupRange(i));
			} else {
				scheduledUpdate = executionAttempt.scheduleReconfig(reconfigId,
					ReconfigOptions.RESCALE_KEYGROUP_RANGE_ONLY,
					jobExecutionPlan.getAlignedKeyGroupRange(i));
			}

			taskUpdateFuture.add(scheduledUpdate);

			FutureUtils
				.combineAll(taskUpdateFuture)
				.thenRunAsync(() -> {
					LOG.info("++++++ Assign new state for repartition Completed");
					CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

					checkNotNull(checkpointCoordinator);
					if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
						checkpointCoordinator.startCheckpointScheduler();
					}

					clean();

					simpleController.onMigrationCompleted();
				}, mainThreadExecutor);
		}
		LOG.info("++++++ Assign new state futures created");
	}

	private void failExecution(Throwable throwable) {
		LOG.info("++++++ Rescale failed with err: ", throwable);
		clean();
	}

	private void clean() {
		inProcess = false;
		notYetAcknowledgedTasks.clear();
	}

	public void start() {
		simpleController.start();
	}

	public void stop() {
	}

	public JobStatusListener createActivatorDeactivator() {
		if (jobStatusListener == null) {
			jobStatusListener = new JobStateCoordinatorDeActivator(this);
		}

		return jobStatusListener;
	}

	private static class JobStateCoordinatorDeActivator implements JobStatusListener {

		private final JobStateCoordinator coordinator;

		public JobStateCoordinatorDeActivator(JobStateCoordinator coordinator) {
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
