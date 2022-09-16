package org.apache.flink.runtime.spector.controller.impl;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.spector.controller.ReconfigExecutor;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DummyController extends Thread implements org.apache.flink.runtime.spector.controller.OperatorController {

	private static final Logger LOG = LoggerFactory.getLogger(DummyController.class);

	private ReconfigExecutor reconfigExecutor;

	private Map<String, List<String>> executorMapping;

	private volatile boolean waitForMigrationDeployed;

	private volatile boolean isStopped;

	private Random random;

//	private final ReconfigurationProfiler reconfigurationProfiler;

	public final static String NUM_AFFECTED_KEYS = "spector.reconfig.affected_keys";

	public final static String NUM_AFFECTED_TASKS = "spector.reconfig.affected_tasks";
	public final static String START_TIME = "spector.reconfig.start";
	public final static String RECONFIG_INTERVAL = "spector.reconfig.interval";

	public final static String SYNC_KEYS = "spector.reconfig.sync_keys";

	private final String name;
	private final int numAffectedKeys;
	private final int numAffectedTasks;
	private final int start;
	private final int interval;

	private final int syncKeys;

	public DummyController(Configuration configuration, String name, int start,
						   ReconfigExecutor reconfigExecutor, Map<String, List<String>> executorMapping) {
		this.name = name;
		this.numAffectedKeys = configuration.getInteger(NUM_AFFECTED_KEYS, 64);
		this.numAffectedTasks = configuration.getInteger(NUM_AFFECTED_TASKS, 65535);
//		this.start = configuration.getInteger(START_TIME, 5 * 1000);
		this.start = start;
		this.interval = configuration.getInteger(RECONFIG_INTERVAL, 10 * 1000);
//		this.reconfigurationProfiler = new ReconfigurationProfiler(configuration);
		// by default 0, indicate to disable sync keys
		this.syncKeys = configuration.getInteger(SYNC_KEYS, 0) == 0 ?
			numAffectedKeys : configuration.getInteger(SYNC_KEYS, 0);


		this.reconfigExecutor = reconfigExecutor;

		this.executorMapping = executorMapping;

		this.random = new Random();
		this.random.setSeed(System.currentTimeMillis());
	}

	@Override
	public void initMetrics(JobGraph jobGraph, JobVertexID vertexID, Configuration jobConfiguration, int parallelism) {
	}

	@Override
	public void onForceRetrieveMetrics() {
	}

	@Override
	public void stopGracefully() {
		isStopped = true;
	}

	@Override
	public void onMigrationExecutorsStopped() {

	}

	@Override
	public void onMigrationCompleted() {
		waitForMigrationDeployed = false;
	}

	@Override
	public void run() {
		try {
			LOG.info("------ dummy streamSwitch start to run");

			// cooldown time, wait for fully deployment
			Thread.sleep(start);

//			testRepartition();
//			testScaleOut();
//			testScaleIn();
//			testCaseOneToOneChange();
//			testJoin();
//			testCaseScaleIn();
//			testRandomScalePartitionAssignment();

			isStopped = false;

			while(!isStopped) {
				stateMigration(numAffectedTasks, numAffectedKeys);
				Thread.sleep(interval);
			}

			LOG.info("------ dummy streamSwitch finished");
		} catch (Exception e) {
			LOG.info("------ exception", e);
		}
	}

	/**
	 * set number of keys to migrate and select equi-sized number of keys from each task to migrate.
	 */
	private void stateMigration(int numAffectedTasks, int numAffectedKeys) throws InterruptedException {
		for (int i = 0; i < numAffectedKeys / syncKeys; i++) {
			Map<String, List<String>> newExecutorMapping = deepCopy(executorMapping);
			Map<String, List<String>> selectedTasks = selectAffectedTasks(numAffectedTasks, newExecutorMapping);
			equiShuffle(syncKeys, selectedTasks);

			// run state migration
			triggerAction(
				"trigger 1 repartition",
				() -> reconfigExecutor.remap(newExecutorMapping),
				newExecutorMapping);
		}
	}

	private Map<String, List<String>> selectAffectedTasks(int numAffectedTasks, Map<String, List<String>> newExecutorMapping) {
		numAffectedTasks = Math.min(numAffectedTasks, newExecutorMapping.size());
		Map<String, List<String>> selectedTasks = new HashMap<>(numAffectedTasks);
		List<String> allTaskID = new ArrayList<>(newExecutorMapping.keySet());
		Collections.shuffle(allTaskID);
		for (int i = 0; i < numAffectedTasks; i++) {
			selectedTasks.put(allTaskID.get(i), newExecutorMapping.get(allTaskID.get(i)));
		}
		return selectedTasks;
	}

	private static Map<String, List<String>> deepCopy(Map<String, List<String>> executorMapping) {
		Map<String, List<String>> newExecutorMapping = new HashMap<>();
		for (String taskId : executorMapping.keySet()) {
			newExecutorMapping.put(taskId, new ArrayList<>(executorMapping.get(taskId)));
		}
		return newExecutorMapping;
	}

	private void equiShuffle(int numAffectedKeys, Map<String, List<String>> newExecutorMapping) {
		int parallelism = newExecutorMapping.size();
		List<String> tasks = new ArrayList<>(newExecutorMapping.keySet());
		for (int i = 0; i < parallelism; i++) {
			int start = (i * numAffectedKeys + parallelism - 1) / parallelism;
			int end = ((i + 1) * numAffectedKeys - 1) / parallelism;
			List<String> curTaskKeys = newExecutorMapping.get(tasks.get(i));
			List<String> nextTaskKeys = newExecutorMapping.get(tasks.get((i + 1) % parallelism));
			int keysToMigrate = Math.min(end - start + 1, curTaskKeys.size());
			nextTaskKeys.addAll(curTaskKeys.subList(0, keysToMigrate));
			curTaskKeys.subList(0, keysToMigrate).clear();
		}
	}

	private void triggerAction(String logStr, Runnable runnable, Map<String, List<String>> partitionAssignment) throws InterruptedException {
		LOG.info("------ " + logStr + "   partitionAssignment: " + partitionAssignment);
		long start = System.currentTimeMillis();
//		reconfigurationProfiler.onReconfigurationStart();
		waitForMigrationDeployed = true;

		runnable.run();

		while (waitForMigrationDeployed);
		executorMapping = partitionAssignment;
//		reconfigurationProfiler.onReconfigurationEnd();
		LOG.info("++++++ reconfig completion time: " + (System.currentTimeMillis() - start));
	}

	private void preparePartitionAssignment(int parallelism) {
		executorMapping.clear();

		for (int i = 0; i < parallelism; i++) {
			executorMapping.put(String.valueOf(i), new ArrayList<>());
		}
	}

	private void preparePartitionAssignment(String ...executorIds) {
		executorMapping.clear();

		for (String executorId: executorIds) {
			executorMapping.put(executorId, new ArrayList<>());
		}
	}

	private void prepareRandomPartitionAssignment(int parallelism) {
		preparePartitionAssignment(parallelism);

		for (int i = 0; i < 128; i++) {
			if (i < parallelism) {
				executorMapping.get(i + "").add(i + "");
			} else {
				executorMapping.get(random.nextInt(parallelism) + "").add(i + "");
			}
		}
	}

	private void testRepartition() throws InterruptedException {
		/*
		 * init: parallelism 4
		 * 	0: [0, 31]
		 * 	1: [32, 63]
		 *  2: [64, 95]
		 *  3: [96, 127]
		 */

		preparePartitionAssignment("0", "1", "2", "3");
		for (int i = 0; i < 128; i++) {
			if (i <= 31) {
				if (i % 2 == 0)
					executorMapping.get("0").add(i + "");
				else
					executorMapping.get("2").add(i + "");
			} else if (i <= 63)
				executorMapping.get("1").add(i + "");
			else if (i <= 95)
				executorMapping.get("2").add(i + "");
			else
				executorMapping.get("3").add(i + "");
		}
		triggerAction(
			"trigger 1 repartition",
			() -> reconfigExecutor.remap(executorMapping),
			executorMapping);
	}

	private void testScaleOut() throws InterruptedException {
		/*
		 * init: parallelism 3
		 * 	0: [0, 42]
		 * 	1: [43, 85]
		 *  2: [86, 127]
		 */

		preparePartitionAssignment("0", "1", "2", "3");
		for (int i = 0; i < 128; i++) {
			if (i <= 42) {
				if (i % 2 == 0)
					executorMapping.get("0").add(i + "");
				else
					executorMapping.get("3").add(i + "");
			} else if (i <= 85)
				executorMapping.get("1").add(i + "");
			else
				executorMapping.get("2").add(i + "");
		}
		triggerAction(
			"trigger 1 scale out",
			() -> reconfigExecutor.scale(4, executorMapping),
			executorMapping);
	}

	private void testScaleIn() throws InterruptedException {
		/*
		 * init: parallelism 4
		 * 	0: [0, 31]
		 * 	1: [32, 63]
		 *  2: [64, 95]
		 *  3: [96, 127]
		 */

		/*
		 * scale in to parallelism 3
		 *   0: [0, 31], [64, 95]
		 *   1: [32, 63]
		 *   3: [96, 127]
		 */
		preparePartitionAssignment("0", "1", "3");
		for (int i = 0; i < 128; i++) {
			if (i <= 31)
				executorMapping.get("0").add(i + "");
			else if (i <= 63)
				executorMapping.get("1").add(i + "");
			else if (i <= 95)
				executorMapping.get("0").add(i + "");
			else
				executorMapping.get("3").add(i + "");
		}
		triggerAction(
			"trigger 1 scale in",
			() -> reconfigExecutor.scale(3, executorMapping),
			executorMapping);

		Thread.sleep(5000);
		/*
		 * scale out to parallelism 4
		 *   0: [0, 31], [64, 95]
		 *   1: [32, 63]
		 *   3: even of [96, 127]
		 *   4: odd of [96, 127]
		 */
		preparePartitionAssignment("0", "1", "3", "4");
		for (int i = 0; i < 128; i++) {
			if (i <= 31)
				executorMapping.get("0").add(i + "");
			else if (i <= 63)
				executorMapping.get("1").add(i + "");
			else if (i <= 95)
				executorMapping.get("0").add(i + "");
			else
				if (i % 2 == 0)
					executorMapping.get("3").add(i + "");
				else
					executorMapping.get("4").add(i + "");
		}
		triggerAction(
			"trigger 2 scale out",
			() -> reconfigExecutor.scale(4, executorMapping),
			executorMapping);
	}

	private void testCaseOneToOneChange() throws InterruptedException {
		/*
		* init: parallelism 2
		* 	0: [0, 63]
		* 	1: [64, 127]
		*/

		/*
		 * repartition with parallelism 2
		 *   0: [0, 20]
		 *   1: [21, 127]
		 */
		preparePartitionAssignment("0", "1");
		for (int i = 0; i < 128; i++) {
			if (i > 0 && i <= 20)
				executorMapping.get("0").add(i + "");
			else
				executorMapping.get("1").add(i + "");
		}
		triggerAction(
			"trigger 1 repartition",
			() -> reconfigExecutor.remap(executorMapping),
			executorMapping);

//		sleep(10000);
//
//		preparePartitionAssignment("0", "1");
//		for (int i = 0; i < 128; i++) {
//			if (i >= 0 && i <= 63)
//				executorMapping.get("0").add(i + "");
//			else
//				executorMapping.get("1").add(i + "");
//		}
//		triggerAction(
//			"trigger 1 repartition",
//			() -> listener.remap(executorMapping),
//			executorMapping);
//
//		/*
//		 * scale in to parallelism 1
//		 *   1: [0, 127]
//		 */
//		preparePartitionAssignment("1");
//		for (int i = 0; i < 128; i++) {
//			executorMapping.get("1").add(i + "");
//		}
//		triggerAction(
//			"trigger 2 scale in",
//			() -> listener.scale(1, executorMapping),
//			executorMapping);
//		sleep(10000);
//
//		/*
//		 * scale out to parallelism 2
//		 *   1: [0, 50]
//		 *   2: [51, 127]
//		 */
//		preparePartitionAssignment("1", "2");
//		for (int i = 0; i < 128; i++) {
//			if (i <= 50)
//				executorMapping.get("1").add(i + "");
//			else
//				executorMapping.get("2").add(i + "");
//		}
//		triggerAction(
//			"trigger 3 scale out",
//			() -> listener.scale(2, executorMapping),
//			executorMapping);
//		sleep(10000);
//
//		/*
//		 * scale out to parallelism 3
//		 *   1: [0, 50]
//		 *   2: [51, 90]
//		 *   3: [91, 127]
//		 */
//		preparePartitionAssignment("1", "2", "3");
//		for (int i = 0; i < 128; i++) {
//			if (i <= 50)
//				executorMapping.get("1").add(i + "");
//			else if (i <= 90)
//				executorMapping.get("2").add(i + "");
//			else
//				executorMapping.get("3").add(i + "");
//		}
//		triggerAction(
//			"trigger 4 scale out",
//			() -> listener.scale(3, executorMapping),
//			executorMapping);
//		sleep(10000);
//
//		/*
//		 * scale in to parallelism 2
//		 *   1: [0, 90]
//		 *   3: [91, 127]
//		 */
//		preparePartitionAssignment("1", "3");
//		for (int i = 0; i < 128; i++) {
//			if (i <= 90)
//				executorMapping.get("1").add(i + "");
//			else
//				executorMapping.get("3").add(i + "");
//		}
//		triggerAction(
//			"trigger 5 scale in",
//			() -> listener.scale(2, executorMapping),
//			executorMapping);
//		sleep(10000);
//
//		/*
//		 * scale out to parallelism 3
//		 *   1: even in [0, 90]
//		 *   3: [91, 127]
//		 *   4: odd in [0, 90]
//		 */
//		preparePartitionAssignment("1", "3", "4");
//		for (int i = 0; i < 128; i++) {
//			if (i <= 90)
//				if (i % 2 == 0)
//					executorMapping.get("1").add(i + "");
//				else
//					executorMapping.get("4").add(i + "");
//			else
//				executorMapping.get("3").add(i + "");
//		}
//		triggerAction(
//			"trigger 6 scale out",
//			() -> listener.scale(3, executorMapping),
//			executorMapping);
//		sleep(10000);
//
//		/*
//		 * scale out to parallelism 4
//		 *   1: even in [0, 90]
//		 *   3: even in [91, 127]
//		 *   4: odd in [0, 90]
//		 *   5: odd in [91, 127]
//		 */
//		preparePartitionAssignment("1", "3", "4", "5");
//		for (int i = 0; i < 128; i++) {
//			if (i <= 90)
//				if (i % 2 == 0)
//					executorMapping.get("1").add(i + "");
//				else
//					executorMapping.get("4").add(i + "");
//			else
//				if (i % 2 == 0)
//					executorMapping.get("3").add(i + "");
//				else
//					executorMapping.get("5").add(i + "");
//		}
//		triggerAction(
//			"trigger 7 scale out",
//			() -> listener.scale(4, executorMapping),
//			executorMapping);
	}

	private void testJoin() throws InterruptedException {
		/*
		 * init: parallelism 1
		 * 	0: [0, 127]
		 */

		preparePartitionAssignment("0", "1");
		for (int i = 0; i < 128; i++) {
			if (i <= 63)
				executorMapping.get("0").add(i + "");
			else
				executorMapping.get("1").add(i + "");
		}
		triggerAction(
			"trigger 1 scale out",
			() -> reconfigExecutor.scale(2, executorMapping),
			executorMapping);
		sleep(10000);

		preparePartitionAssignment("0", "1", "2");
		for (int i = 0; i < 128; i++) {
			if (i <= 63)
				executorMapping.get("0").add(i + "");
			else if (i <= 80)
				executorMapping.get("1").add(i + "");
			else
				executorMapping.get("2").add(i + "");
		}
		triggerAction(
			"trigger 2 scale out",
			() -> reconfigExecutor.scale(3, executorMapping),
			executorMapping);
		sleep(10000);

		preparePartitionAssignment("0", "2");
		for (int i = 0; i < 128; i++) {
			if (i <= 63)
				executorMapping.get("0").add(i + "");
			else
				executorMapping.get("2").add(i + "");
		}
		triggerAction(
			"trigger 3 scale in",
			() -> reconfigExecutor.scale(2, executorMapping),
			executorMapping);
		sleep(10000);

		preparePartitionAssignment("0", "2", "3");
		for (int i = 0; i < 128; i++) {
			if (i <= 63)
				executorMapping.get("0").add(i + "");
			else if (i <= 90)
				executorMapping.get("2").add(i + "");
			else
				executorMapping.get("3").add(i + "");
		}
		triggerAction(
			"trigger 4 scale out",
			() -> reconfigExecutor.scale(3, executorMapping),
			executorMapping);
	}

	public static void main(String[] args) {
		Map<String, List<String>> oldExecutorMapping = new HashMap<>();

		int numExecutors = 10;
		int numPartitions = 128;
		for (int executorId = 0; executorId < numExecutors; executorId++) {
			List<String> executorPartitions = new ArrayList<>();
			oldExecutorMapping.put(String.valueOf(executorId), executorPartitions);

			KeyGroupRange keyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
				numPartitions, numExecutors, executorId);
			for (int i = keyGroupRange.getStartKeyGroup(); i <= keyGroupRange.getEndKeyGroup(); i++) {
				executorPartitions.add(String.valueOf(i));
			}
		}

		int parallelism = oldExecutorMapping.size();
		int numAffectedKeys = 2;

		System.out.println(oldExecutorMapping);

		Map<String, List<String>> executorMapping = deepCopy(oldExecutorMapping);

		for (int i = 0; i < parallelism; i++) {
			int start = (i * numAffectedKeys + parallelism - 1) / parallelism;
			int end = ((i + 1) * numAffectedKeys - 1) / parallelism;
			List<String> curTaskKeys = executorMapping.get(String.valueOf(i));
			List<String> nextTaskKeys = executorMapping.get(String.valueOf((i + 1) % parallelism));
			System.out.println(i + " => " + ((i + 1) % parallelism) + " : " + (end - start + 1));
			nextTaskKeys.addAll(curTaskKeys.subList(0, end - start + 1));
			curTaskKeys.subList(0, end - start + 1).clear();
		}

		// check modified sources and destinations
		System.out.println(executorMapping);
		List<String> modifiedIdList = executorMapping.keySet().stream()
			.filter(id -> {
				System.out.println(! new HashSet<>(executorMapping.get(id)).containsAll(oldExecutorMapping.get(id)));
				return executorMapping.get(id).size() != oldExecutorMapping.get(id).size()
				|| new HashSet<>(executorMapping.get(id)).containsAll(oldExecutorMapping.get(id));
			}).collect(Collectors.toList());
		System.out.println(modifiedIdList);
	}
}
