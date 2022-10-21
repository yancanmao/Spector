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

package org.apache.flink.runtime.spector.controller.impl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.spector.controller.ReconfigExecutor;
import org.apache.flink.runtime.spector.controller.StateMigrationPlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.spector.SpectorOptions.NUM_AFFECTED_KEYS;
import static org.apache.flink.runtime.spector.SpectorOptions.SYNC_KEYS;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Set the plan for the state migration according to the Configurations
 */
public class StateMigrationPlannerImpl implements StateMigrationPlanner {
	private static final Logger LOG = LoggerFactory.getLogger(StateMigrationPlanner.class);


	private final Configuration configuration;
	private final JobVertexID jobVertexID;

	private int numOpenedSubtask;

	private Map<String, List<String>> oldExecutorMapping;

	private final ReconfigExecutor reconfigExecutor;

	private volatile boolean waitForMigrationDeployed;

	public final int syncKeys;

	public final ExecutionGraph executionGraph;

	public StateMigrationPlannerImpl(Configuration configuration,
									 JobVertexID jobVertexID,
									 int parallelism,
									 Map<String, List<String>> executorMapping,
									 ReconfigExecutor reconfigExecutor,
									 ExecutionGraph executionGraph) {
		this.configuration = configuration;

		int numAffectedKeys = configuration.getInteger(NUM_AFFECTED_KEYS);
		this.syncKeys = configuration.getInteger(SYNC_KEYS) == 0 ?
			numAffectedKeys : configuration.getInteger(SYNC_KEYS);

		this.jobVertexID = jobVertexID;
		this.numOpenedSubtask = parallelism;

		// Deep copy
		this.oldExecutorMapping = new HashMap<>();
		for (String taskId : executorMapping.keySet()) {
			oldExecutorMapping.put(taskId, new ArrayList<>(executorMapping.get(taskId)));
		}

		this.reconfigExecutor = reconfigExecutor;

		this.executionGraph = executionGraph;
	}

	@Override
	public void remap(Map<String, List<String>> executorMapping) {
		makePlan(executorMapping);
	}

	@Override
	public void scale(int parallelism, Map<String, List<String>> executorMapping) {
		makePlan(executorMapping);
	}

	public void makePlan(Map<String, List<String>> executorMapping) {
		// Stop checkpoint coordinator then start the reconfiguration
		LOG.info("++++++ Stop Checkpoint Coordinator and start to make plan");
		CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		checkNotNull(checkpointCoordinator);
		checkpointCoordinator.stopCheckpointScheduler();


		int newParallelism = executorMapping.keySet().size();
		// find out the affected keys.
		// order the migrating keys
		// return the state migration plan to reconfig executor
		Map<String, Tuple2<String, String>> affectedKeys = getAffectedKeys(executorMapping);

		prioritizeKeys(affectedKeys);
		List<Map<String, Tuple2<String, String>>> plan = batching(affectedKeys);

		if (numOpenedSubtask >= newParallelism) {
			// repartition
			for (Map<String, Tuple2<String, String>> batchedAffectedKeys : plan) {
				Map<String, List<String>> fluidExecutorMapping = deepCopy(oldExecutorMapping);
				for (String affectedKey : batchedAffectedKeys.keySet()) {
					Tuple2<String, String> srcToDst = batchedAffectedKeys.get(affectedKey);

					fluidExecutorMapping.get(srcToDst.f0).remove(affectedKey);
					fluidExecutorMapping.get(srcToDst.f1).add(affectedKey);
				}
				// trigger reconfig Executor rescale
				triggerAction(
					"trigger 1 fluid repartition",
					() -> reconfigExecutor.remap(fluidExecutorMapping),
					fluidExecutorMapping);
			}
		} else {
			// scale out
			throw new UnsupportedOperationException();
		}

		checkNotNull(checkpointCoordinator);
		if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
			checkpointCoordinator.startCheckpointScheduler();
		}
	}

	private Map<String, List<String>> deepCopy(Map<String, List<String>> executorMapping) {
		Map<String, List<String>> fluidExecutorMapping = new HashMap<>();
		for (String taskId : executorMapping.keySet()) {
			fluidExecutorMapping.put(taskId, new ArrayList<>(executorMapping.get(taskId)));
		}
		return fluidExecutorMapping;
	}

	private Map<String, Tuple2<String, String>> getAffectedKeys(Map<String, List<String>> executorMapping) {
		// Key -> <SrcTaskId, DstTaskId>, can be used to set new ExecutorMapping
		Map<String, Tuple2<String, String>> affectedKeys = new HashMap<>();

		// Key -> Task
		Map<String, String> oldKeysToExecutorMapping = new HashMap<>();
		oldExecutorMapping.keySet().forEach(oldTaskId -> {
			for (String key : oldExecutorMapping.get(oldTaskId)) {
				oldKeysToExecutorMapping.put(key, oldTaskId);
			}
		});

		executorMapping.keySet().forEach(newTaskId -> {
			for (String key : executorMapping.get(newTaskId)) {
				// check whether the keys is migrated from old task to new task
				if (!oldKeysToExecutorMapping.get(key).equals(newTaskId)) {
					affectedKeys.put(key, Tuple2.of(oldKeysToExecutorMapping.get(key), newTaskId));
				}
			}
		});
		return affectedKeys;
	}

	private List<Map<String, Tuple2<String, String>>> batching(Map<String, Tuple2<String, String>> affectedKeys) {
		List<Map<String, Tuple2<String, String>>> plan = new ArrayList<>();

		int count = 0;
		Map<String, Tuple2<String, String>> batchedAffectedKeys = new HashMap<>();

		for (String affectedKey : affectedKeys.keySet()) {
			batchedAffectedKeys.put(affectedKey, affectedKeys.get(affectedKey));
			count++;
			if (count % syncKeys == 0) {
				plan.add(batchedAffectedKeys);
				batchedAffectedKeys = new HashMap<>();
			}
		}
		return plan;
	}

	public void prioritizeKeys(Map<String, Tuple2<String, String>> affectedKeys) {
	}

	private void triggerAction(String logStr, Runnable runnable, Map<String, List<String>> partitionAssignment) {
		LOG.info("------ " + logStr + "   partitionAssignment: " + partitionAssignment);
		long start = System.currentTimeMillis();
		waitForMigrationDeployed = true;

		runnable.run();

		while (waitForMigrationDeployed);
		oldExecutorMapping = partitionAssignment;
		LOG.info("++++++ reconfig completion time: " + (System.currentTimeMillis() - start));
	}

	public void onMigrationCompleted() {
		waitForMigrationDeployed = false;
	}
}
