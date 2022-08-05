package org.apache.flink.runtime.spector;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.util.HashMap;
import java.util.Map;

public class GlobalStateManager {
	// OperatorId -> {hashedKeygroup -> <alignedKeygroup, statehandle, isChanged>}
	public static Map<Integer, Tuple3<Long, StreamStateHandle, Boolean>> globalManagedStateHandles = new HashMap<>();
	public static Map<Integer, Tuple3<Long, StreamStateHandle, Boolean>> globalRawStateHandles = new HashMap<>();
}
