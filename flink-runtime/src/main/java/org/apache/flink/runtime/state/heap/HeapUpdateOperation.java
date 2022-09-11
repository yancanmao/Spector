package org.apache.flink.runtime.state.heap;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.StateMigrationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HeapUpdateOperation<K> {
	private static final Logger LOG = LoggerFactory.getLogger(HeapUpdateOperation.class);

	private final Collection<KeyedStateHandle> stateHandles;
	private final CloseableRegistry cancelStreamRegistry;
	private final KeyGroupRange keyGroupRange;
	private final ClassLoader userCodeClassLoader;
	private final StateSerializerProvider<K> keySerializerProvider;
	private final Map<String, StateTable<K, ?, ?>> oldRegisteredKVStates;
	private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper> oldRegisteredPQStates;

	// this two is just a wrapper to keep the compatibility
	// we do not create new stat table or priority queue, just update the old one in fine-grained.
	private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
	private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates;
	private final HeapPriorityQueueSetFactory priorityQueueSetFactory;


	private final Collection<Integer> migrateInKeygroup;


	public HeapUpdateOperation(Collection<KeyedStateHandle> stateHandles,
							   HeapKeyedStateBackend<K> keyedStateBackend,
							   TypeSerializer<K> keySerializer,
							   KeyGroupRange keyGroupRange,
							   int maxNumberOfParallelSubtasks,
							   CloseableRegistry cancelStreamRegistry,
							   Collection<Integer> migrateInKeygroup) {
		this.stateHandles = stateHandles;
		this.oldRegisteredKVStates = keyedStateBackend.getRegisteredKVStates();
		this.oldRegisteredPQStates = keyedStateBackend.getRegisteredPQStates();
		this.registeredKVStates = new HashMap<>();
		this.registeredPQStates = new HashMap<>();
		this.userCodeClassLoader = keyedStateBackend.getUserCodeClassLoader();
		this.keySerializerProvider = StateSerializerProvider.fromNewRegisteredSerializer(keySerializer);
		this.keyGroupRange = keyGroupRange;
		this.cancelStreamRegistry = cancelStreamRegistry;
		this.migrateInKeygroup = migrateInKeygroup;
		this.priorityQueueSetFactory =
			new HeapPriorityQueueSetFactory(keyGroupRange, maxNumberOfParallelSubtasks, 128);
	}

	public void updateHeapState() throws Exception {
		// TODO: how to remove old state?
		// TODO: once the snapshot has been persisted, remove the key state from the source task.

		final Map<Integer, StateMetaInfoSnapshot> kvStatesById = new HashMap<>();
		registeredKVStates.clear();
		registeredPQStates.clear();

		boolean keySerializerRestored = false;

		for (KeyedStateHandle keyedStateHandle : stateHandles) {
			if (keyedStateHandle == null) {
				continue;
			}

			if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
				throw new IllegalStateException("Unexpected state handle type, " +
					"expected: " + KeyGroupsStateHandle.class +
					", but found: " + keyedStateHandle.getClass());
			}

			KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
			FSDataInputStream fsDataInputStream = keyGroupsStateHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(fsDataInputStream);

			try {
				DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(fsDataInputStream);

				KeyedBackendSerializationProxy<K> serializationProxy =
					new KeyedBackendSerializationProxy<>(userCodeClassLoader);

				serializationProxy.read(inView);

				if (!keySerializerRestored) {
					// check for key serializer compatibility; this also reconfigures the
					// key serializer to be compatible, if it is required and is possible
					TypeSerializerSchemaCompatibility<K> keySerializerSchemaCompat =
						keySerializerProvider.setPreviousSerializerSnapshotForRestoredState(serializationProxy.getKeySerializerSnapshot());
					if (keySerializerSchemaCompat.isCompatibleAfterMigration() || keySerializerSchemaCompat.isIncompatible()) {
						throw new StateMigrationException("The new key serializer must be compatible.");
					}

					keySerializerRestored = true;
				}

				List<StateMetaInfoSnapshot> restoredMetaInfos =
					serializationProxy.getStateMetaInfoSnapshots();

				createOrCheckStateForMetaInfo(restoredMetaInfos, kvStatesById);

				readStateHandleStateData(
					fsDataInputStream,
					inView,
					keyGroupsStateHandle.getGroupRangeOffsets(),
					kvStatesById, restoredMetaInfos.size(),
					serializationProxy.getReadVersion(),
					serializationProxy.isUsingKeyGroupCompression());
			} finally {
				if (cancelStreamRegistry.unregisterCloseable(fsDataInputStream)) {
					IOUtils.closeQuietly(fsDataInputStream);
				}
			}
		}
	}

	private void createOrCheckStateForMetaInfo(
		List<StateMetaInfoSnapshot> restoredMetaInfo,
		Map<Integer, StateMetaInfoSnapshot> kvStatesById) {

		for (StateMetaInfoSnapshot metaInfoSnapshot : restoredMetaInfo) {
			// always put metaInfo into kvStatesById, because kvStatesById is KeyGroupsStateHandle related
//			kvStatesById.put(kvStatesById.size(), metaInfoSnapshot);
			final StateSnapshotRestore registeredState;

			switch (metaInfoSnapshot.getBackendStateType()) {
				case KEY_VALUE:
					registeredState = registeredKVStates.get(metaInfoSnapshot.getName());
					if (registeredState == null) {
						// We do not need to create a new kv state table for the backend, just to update it.
						registeredKVStates.put(
							metaInfoSnapshot.getName(),
							oldRegisteredKVStates.get(metaInfoSnapshot.getName()));
					}
					break;
				case PRIORITY_QUEUE:
					registeredState = registeredPQStates.get(metaInfoSnapshot.getName());
					if (registeredState == null) {
						registeredPQStates.put(metaInfoSnapshot.getName(),
							oldRegisteredPQStates.get(metaInfoSnapshot.getName()));
					}
					break;
				default:
					throw new IllegalStateException("Unexpected state type: " +
						metaInfoSnapshot.getBackendStateType() + ".");
			}

			if (registeredState == null) {
				kvStatesById.put(kvStatesById.size(), metaInfoSnapshot);
			}
		}
	}

	private void readStateHandleStateData(
		FSDataInputStream fsDataInputStream,
		DataInputViewStreamWrapper inView,
		KeyGroupRangeOffsets keyGroupOffsets,
		Map<Integer, StateMetaInfoSnapshot> kvStatesById,
		int numStates,
		int readVersion,
		boolean isCompressed) throws IOException {

		final StreamCompressionDecorator streamCompressionDecorator = isCompressed ?
			SnappyStreamCompressionDecorator.INSTANCE : UncompressedStreamCompressionDecorator.INSTANCE;

		for (Tuple2<Integer, Long> groupOffset : keyGroupOffsets) {
//			int keyGroupIndex = groupOffset.f0;
			// aligned keygroup index in original keygroupoffset will be never used.
//			int alignedKeyGroupIndex = groupOffset.f0;
			long offset = groupOffset.f1;

			// Check that restored key groups all belong to the backend.
			fsDataInputStream.seek(offset);

			int hashedKeyGroup = inView.readInt();

			if (!migrateInKeygroup.contains(hashedKeyGroup)) {
				continue;
			}

			int alignedKeyGroupIndex = keyGroupRange.mapFromHashedToAligned(hashedKeyGroup);

			LOG.info("++++++-- keyGroupRange: " + keyGroupRange +
				", alignedKeyGroupIndex: " + keyGroupRange.mapFromHashedToAligned(hashedKeyGroup) +
				", offset: " + offset +
				", hashedKeyGroup: " + hashedKeyGroup);

			try (InputStream kgCompressionInStream =
					 streamCompressionDecorator.decorateWithCompression(fsDataInputStream)) {

				readKeyGroupStateData(
					kgCompressionInStream,
					kvStatesById,
					alignedKeyGroupIndex,
					numStates,
					readVersion);
			}
		}
	}

	private void readKeyGroupStateData(
		InputStream inputStream,
		Map<Integer, StateMetaInfoSnapshot> kvStatesById,
		int alignedKeyGroupIndex,
		int numStates,
		int readVersion) throws IOException {

		DataInputViewStreamWrapper inView =
			new DataInputViewStreamWrapper(inputStream);

		for (int i = 0; i < numStates; i++) {

			final int kvStateId = inView.readShort();
			final StateMetaInfoSnapshot stateMetaInfoSnapshot = kvStatesById.get(kvStateId);
			final StateSnapshotRestore registeredState;

			switch (stateMetaInfoSnapshot.getBackendStateType()) {
				case KEY_VALUE: // TODO: we only support kv store state update by far.
					registeredState = registeredKVStates.get(stateMetaInfoSnapshot.getName());
					break;
				case PRIORITY_QUEUE:
					registeredState = registeredPQStates.get(stateMetaInfoSnapshot.getName());
					break;
				default:
					throw new IllegalStateException("Unexpected state type: " +
						stateMetaInfoSnapshot.getBackendStateType() + ".");
			}

			//TODO: avoid redundant write.
			StateSnapshotKeyGroupReader keyGroupReader = registeredState.keyGroupReader(readVersion);
			keyGroupReader.readMappingsInKeyGroup(inView, keyGroupRange.mapFromAlignedToHashed(alignedKeyGroupIndex));
		}
	}
}
