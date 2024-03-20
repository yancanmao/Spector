package org.apache.flink.runtime.spector.replication;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.*;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Instantiated on each task manager, each Job need to instantiate one TaskStateManager on each TaskManager
 * TODO: maybe we do not need this class, we assign a task to be a backup task, other task can access the task to get the backup state.
 */
public class ReplicaStateManager {
	private final Map<JobVertexID, TaskStateManager> replicas;

	public ReplicaStateManager() {
		replicas = new ConcurrentHashMap<>();
	}

	/**
	 * The API to retrieve task restore from the replica tasks.
	 * @param jobvertexId
	 * @return
	 */
	public JobManagerTaskRestore getTaskRestoreFromReplica(JobVertexID jobvertexId) {
		TaskStateManager taskStateManager = this.getReplicas().get(jobvertexId);
		// directly assign backup taskrestore for the targeting task, and it will be updated in each operator during
		// state initialization.
		Preconditions.checkNotNull(taskStateManager);
		return ((TaskStateManagerImpl) taskStateManager).getTaskRestore();
	}

	public void put(JobVertexID jobvertexId, TaskStateManager taskStateManager) {
		this.getReplicas().put(jobvertexId, taskStateManager);
	}

	/**
	 * Substitute keyed state that is shown up in
	 */
	public void mergeState(JobVertexID jobvertexId, JobManagerTaskRestore newStateRestore) {
		TaskStateManager taskStateManager = this.getReplicas().get(jobvertexId);
		JobManagerTaskRestore oldTaskRestore = ((TaskStateManagerImpl) taskStateManager).getTaskRestore();

		newStateRestore.merge(oldTaskRestore);

		taskStateManager.setTaskRestore(newStateRestore);
	}

	/**
	 * Substitute keyed state that is shown up in
	 */
	public void mergeState(JobVertexID jobvertexId, Map<Integer, Tuple2<Long, StreamStateHandle>> hashedKeyGroupToHandle) {
		TaskStateManager taskStateManager = this.getReplicas().get(jobvertexId);
		Map<Integer, Tuple2<Long, StreamStateHandle>> localReplicatedStateHandle = ((TaskStateManagerImpl) taskStateManager).getHashedKeyGroupToHandle();
		Preconditions.checkNotNull(localReplicatedStateHandle);
		hashedKeyGroupToHandle.forEach(localReplicatedStateHandle::putIfAbsent);
	}

	public Map<JobVertexID, TaskStateManager> getReplicas() {
		return replicas;
	}
}
