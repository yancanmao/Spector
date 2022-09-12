package org.apache.flink.runtime.spector.controller.impl;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.spector.JobExecutionPlan;
import org.apache.flink.runtime.spector.JobReconfigAction;
import org.apache.flink.runtime.spector.controller.ReconfigExecutor;
import org.apache.flink.runtime.spector.controller.OperatorController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ControllerAdaptor {

	private static final Logger LOG = LoggerFactory.getLogger(ControllerAdaptor.class);

	private final JobReconfigAction rescaleAction;

	private final Map<JobVertexID, FlinkOperatorController> controllers;

	private final Configuration config;

//	private final long migrationInterval;

	public ControllerAdaptor(
		JobReconfigAction rescaleAction,
		ExecutionGraph executionGraph) {

		this.rescaleAction = rescaleAction;

		this.controllers = new HashMap<>(executionGraph.getAllVertices().size());

		this.config = executionGraph.getJobConfiguration();

//		this.migrationInterval = config.getLong("streamswitch.system.migration_interval", 5000);
		String targetOperatorsStr = config.getString("controller.target.operators", "flatmap");
		String reconfigStartStr = config.getString("spector.reconfig.start", "5000");
		List<String> targetOperatorsList = Arrays.asList(targetOperatorsStr.split(","));
		List<String> reconfigStartList = Arrays.asList(reconfigStartStr.split(","));
		Map<String, Integer> targetOperators = new HashMap<>(targetOperatorsList.size());
		for (int i = 0; i < targetOperatorsList.size(); i++) {
			targetOperators.put(targetOperatorsList.get(i), Integer.valueOf(reconfigStartList.get(i)));
		}

		for (Map.Entry<JobVertexID, ExecutionJobVertex> entry : executionGraph.getAllVertices().entrySet()) {
			JobVertexID vertexID = entry.getKey();
			int parallelism = entry.getValue().getParallelism();
			int maxParallelism = entry.getValue().getMaxParallelism();

			// TODO scaling: using DummyController for test purpose
//			if (!entry.getValue().getName().toLowerCase().contains("join") && !entry.getValue().getName().toLowerCase().contains("window")) {
//				continue;
//			}

			String operatorName = entry.getValue().getName();
			if (targetOperators.containsKey(operatorName)) {
				FlinkOperatorController controller = new DummyController(config, operatorName, targetOperators.get(operatorName));
				ReconfigExecutor executor = new ReconfigExecutorImpl(vertexID, parallelism, maxParallelism);

				controller.init(
					executor,
					generateExecutorDelegates(parallelism),
					generateFinestPartitionDelegates(maxParallelism));
				controller.initMetrics(rescaleAction.getJobGraph(), vertexID, config, parallelism);

				this.controllers.put(vertexID, controller);
			}
		}
	}

	public void startControllers() {
		for (OperatorController controller : controllers.values()) {
			controller.start();
		}
	}

	public void stopControllers() {
		for (OperatorController controller : controllers.values()) {
			controller.stopGracefully();
		}
	}

	public void onChangeImplemented(JobVertexID jobVertexID) {
		// sleep period of time
//		try {
//			Thread.sleep(migrationInterval);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		LOG.info("++++++ onChangeImplemented triggered for jobVertex " + jobVertexID);
		this.controllers.get(jobVertexID).onMigrationCompleted();
	}

	public void onForceRetrieveMetrics(JobVertexID jobVertexID) {
		LOG.info("++++++ onForceRetrieveMetrics triggered for jobVertex " + jobVertexID);
		this.controllers.get(jobVertexID).onForceRetrieveMetrics();
	}

	public void onMigrationExecutorsStopped(JobVertexID jobVertexID) {
		LOG.info("++++++ onMigrationExecutorsStopped triggered for jobVertex " + jobVertexID);
		this.controllers.get(jobVertexID).onMigrationExecutorsStopped();
	}

	private static List<String> generateExecutorDelegates(int parallelism) {
		List<String> executors = new ArrayList<>();
		for (int i = 0; i < parallelism; i++) {
			executors.add(String.valueOf(i));
		}
		return executors;
	}

	private static List<String> generateFinestPartitionDelegates(int maxParallelism) {
		List<String> finestPartitions = new ArrayList<>();
		for (int i = 0; i < maxParallelism; i++) {
			finestPartitions.add(String.valueOf(i));
		}
		return finestPartitions;
	}

	private class ReconfigExecutorImpl implements ReconfigExecutor {

		public final JobVertexID jobVertexID;

		private int numOpenedSubtask;

		private JobExecutionPlan oldExecutionPlan;

		private Map<String, List<String>> oldExecutorMapping;

		public ReconfigExecutorImpl(JobVertexID jobVertexID, int parallelism, int maxParallelism) {
			this.jobVertexID = jobVertexID;
			this.numOpenedSubtask = parallelism;
		}

		@Override
		public void setup(Map<String, List<String>> executorMapping) {
			this.oldExecutionPlan = new JobExecutionPlan(jobVertexID, executorMapping, numOpenedSubtask);
			// Deep copy
			this.oldExecutorMapping = new HashMap<>();
			for (String taskId : executorMapping.keySet()) {
				oldExecutorMapping.put(taskId, new ArrayList<>(executorMapping.get(taskId)));
			}
			rescaleAction.setInitialJobExecutionPlan(jobVertexID, oldExecutionPlan);
		}

		@Override
		public void remap(Map<String, List<String>> executorMapping) {
			handleTreatment(executorMapping);
		}

		@Override
		public void scale(int newParallelism, Map<String, List<String>> executorMapping) {
			handleTreatment(executorMapping);
		}

		private void handleTreatment(Map<String, List<String>> executorMapping) {
			int newParallelism = executorMapping.keySet().size();

			JobExecutionPlan jobExecutionPlan;

			if (numOpenedSubtask >= newParallelism) {
				// repartition
				jobExecutionPlan = new JobExecutionPlan(
					jobVertexID, executorMapping, oldExecutorMapping, oldExecutionPlan, numOpenedSubtask);

				rescaleAction.repartition(jobVertexID, jobExecutionPlan);
			} else {
				// scale out
				jobExecutionPlan = new JobExecutionPlan(
					jobVertexID, executorMapping, oldExecutorMapping, oldExecutionPlan, newParallelism);

//				rescaleAction.scaleOut(jobVertexID, newParallelism, jobExecutionPlan);
				numOpenedSubtask = newParallelism;
			}

			this.oldExecutionPlan = jobExecutionPlan;
			this.oldExecutorMapping = new HashMap<>(executorMapping);
		}
	}
}
