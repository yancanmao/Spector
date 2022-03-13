package org.apache.flink.runtime.spector;

import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.Operation.DISPATCH_STATE_TO_STANDBY_TASK;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Instantiated on JobMaster, each job has one JobMaster
 * 1. initialize
 * 2. implement replication
 * 3. implement repartition
 * 4. implement restore
 */
public class JobStateCoordinator implements CheckpointProgressListener {

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
	private final JobGraphRescaler jobGraphRescaler;

	/**
	 * standby executions for backup state maintenance of each operator
	 * Operator -> [BackupTask1, BackTask2, ...]
	 */
	private final HashMap<JobVertexID, List<ExecutionVertex>> standbyExecutionVertexs;

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
		this.jobGraphRescaler = JobGraphRescaler.instantiate(jobGraph, userCodeLoader);
		this.standbyExecutionVertexs = new HashMap<>();

		// inject a listener into CheckpointCoordinator to manage received checkpoints
		checkNotNull(executionGraph.getCheckpointCoordinator());
		executionGraph.getCheckpointCoordinator().setCheckpointProgressListener(this);
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
					standbyExecutionVertexs.put(executionJobVertex.getJobVertexId(), createCandidates); // TODO: need to create number of tasks according to number of nodes in the cluster.

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

		stateAssignmentOperation.assignStates();
	}

	// TODO: see if other place need to add this
	public CompletableFuture<?> cancelStandbyExecution(ExecutionJobVertex executionJobVertex) {
		if (!standbyExecutionVertexs.get(executionJobVertex.getJobVertexId()).isEmpty()) {
			LOG.debug(String.format("Cancelling standby execution %s", this));
			final ExecutionVertex standbyExecution = standbyExecutionVertexs.get(executionJobVertex.getJobVertexId()).remove(0);
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

	}

	@Override
	public void onCompleteCheckpoint(CompletedCheckpoint checkpoint) {
		checkNotNull(checkpoint);
		dispatchLatestCheckpointedStateToStandbyTasks(checkpoint);
	}
}
