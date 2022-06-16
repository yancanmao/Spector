package org.apache.flink.runtime.spector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Instantiated on each task manager, each Job need to instantiate one TaskStateManager on each TaskManager
 * TODO: maybe we do not need this class, we assign a task to be a backup task, other task can access the task to get the backup state.
 */
public class BackupStateManager {
	public HashMap<JobVertexID, TaskStateManager> replicas;

	public BackupStateManager() {
		replicas = new HashMap<>();
	}

	/**
	 * The API to retrieve task restore from the replica tasks.
	 * @param jobvertexId
	 * @return
	 */
	public JobManagerTaskRestore getTaskRestoreFromReplica(JobVertexID jobvertexId) {
		TaskStateManager taskStateManager = this.replicas.get(jobvertexId);
		// directly assign backup taskrestore for the targeting task, and it will be updated in each operator during
		// state initialization.
		return ((TaskStateManagerImpl) taskStateManager).getTaskRestore();
	}

	public void put(JobVertexID jobvertexId, TaskStateManager taskStateManager) {
		this.replicas.put(jobvertexId, taskStateManager);
	}

	/**
	 * Substitute keyed state that is shown up in
	 */
	public void mergeState(JobVertexID jobvertexId, JobManagerTaskRestore newStateRestore) {
		TaskStateManager taskStateManager = this.replicas.get(jobvertexId);
		JobManagerTaskRestore oldTaskRestore = ((TaskStateManagerImpl) taskStateManager).getTaskRestore();
		// Merge all key state from old and new state restore.
		Map<Integer, KeyedStateHandle> oldHashedKeyGroupToManagedStateHandle =
			getHashedKeyGroupToHandleFromStateRestore(oldTaskRestore, OperatorSubtaskState::getManagedKeyedState);
		Map<Integer, KeyedStateHandle> oldHashedKeyGroupToRawStateHandle =
			getHashedKeyGroupToHandleFromStateRestore(oldTaskRestore, OperatorSubtaskState::getRawKeyedState);

		Map<Integer, KeyedStateHandle> newHashedKeyGroupToManagedStateHandle =
			getHashedKeyGroupToHandleFromStateRestore(newStateRestore, OperatorSubtaskState::getManagedKeyedState);
		Map<Integer, KeyedStateHandle> newHashedKeyGroupToRawStateHandle =
			getHashedKeyGroupToHandleFromStateRestore(newStateRestore, OperatorSubtaskState::getRawKeyedState);

		// TODO: combine two pair of state handle into one, and then create a new OperatorSubtaskState.
		oldHashedKeyGroupToManagedStateHandle.putAll(newHashedKeyGroupToManagedStateHandle);
		oldHashedKeyGroupToRawStateHandle.putAll(newHashedKeyGroupToRawStateHandle);

		Map.Entry<OperatorID, OperatorSubtaskState> operatorSubtaskStateEntry = getOperatorSubtaskState(newStateRestore);

		Preconditions.checkState(operatorSubtaskStateEntry != null, "++++++ operatorSubtaskState cannot be null");

		OperatorSubtaskState newOperatorSubtaskState = new OperatorSubtaskState(
			operatorSubtaskStateEntry.getValue().getManagedOperatorState(),
			operatorSubtaskStateEntry.getValue().getRawOperatorState(),
			new StateObjectCollection<>(oldHashedKeyGroupToManagedStateHandle.values()),
			new StateObjectCollection<>(oldHashedKeyGroupToRawStateHandle.values()));

		newStateRestore.getTaskStateSnapshot()
			.putSubtaskStateByOperatorID(operatorSubtaskStateEntry.getKey(), newOperatorSubtaskState);

		taskStateManager.setTaskRestore(newStateRestore);
	}

	private Map<Integer, KeyedStateHandle> getHashedKeyGroupToHandleFromStateRestore(
		JobManagerTaskRestore stateRestore,
		Function<OperatorSubtaskState, StateObjectCollection<KeyedStateHandle>> applier) {

		Map<Integer, KeyedStateHandle> hashedKeyGroupToHandle = new HashMap<>();

		if (stateRestore == null) {
			return hashedKeyGroupToHandle;
		}

		Set<Map.Entry<OperatorID, OperatorSubtaskState>> subtaskStateMappings
			= stateRestore.getTaskStateSnapshot().getSubtaskStateMappings();

		// By default, there will only be one Operator inside the OperatorChain.
		for (Map.Entry<OperatorID, OperatorSubtaskState> subtaskStateEntry : subtaskStateMappings) {
			for (KeyedStateHandle keyedStateHandle : applier.apply(subtaskStateEntry.getValue())) {
				if (keyedStateHandle != null) {
					if (keyedStateHandle instanceof KeyGroupsStateHandle) {

						KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
						KeyGroupRange keyGroupRangeFromOldState = keyGroupsStateHandle.getKeyGroupRange();

						if (keyGroupRangeFromOldState.equals(KeyGroupRange.EMPTY_KEY_GROUP_RANGE)) {
							continue;
						}

						int start = keyGroupRangeFromOldState.getStartKeyGroup();
						int end = keyGroupRangeFromOldState.getEndKeyGroup();

						try (FSDataInputStream fsDataInputStream = keyGroupsStateHandle.openInputStream()) {
							DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(fsDataInputStream);

							for (int alignedOldKeyGroup = start; alignedOldKeyGroup <= end; alignedOldKeyGroup++) {
								long offset = keyGroupsStateHandle.getOffsetForKeyGroup(alignedOldKeyGroup);

								int nextAlignedOldKeyGroup = alignedOldKeyGroup + 1;

								long nextOffset = nextAlignedOldKeyGroup <= end ?
									keyGroupsStateHandle.getOffsetForKeyGroup(nextAlignedOldKeyGroup)
									: keyGroupsStateHandle.getStateSize();

								// skip if the keygroup does not have any state snapshot
								if (nextOffset == offset) {
									continue;
								}

								fsDataInputStream.seek(offset);
								int hashedKeyGroup = inView.readInt();

								hashedKeyGroupToHandle.put(
									hashedKeyGroup, keyGroupsStateHandle);
							}
						} catch (IOException err) {
							throw new RuntimeException(err);
						}
					} else {
						throw new RuntimeException("++++++ unsupported KeyedStateHandle for state migration");
					}
				}
			}
		}
		return hashedKeyGroupToHandle;
	}

	private Map.Entry<OperatorID, OperatorSubtaskState> getOperatorSubtaskState(JobManagerTaskRestore stateRestore) {

		Preconditions.checkState(stateRestore != null, "++++++ new state restore should not be null");

		Set<Map.Entry<OperatorID, OperatorSubtaskState>> subtaskStateMappings
			= stateRestore.getTaskStateSnapshot().getSubtaskStateMappings();

		// By default, there will only be one Operator inside the OperatorChain.
		for (Map.Entry<OperatorID, OperatorSubtaskState> subtaskStateEntry : subtaskStateMappings) {
			return subtaskStateEntry;
		}

		return null;
	}
}
