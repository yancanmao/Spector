package org.apache.flink.runtime.spector.streamswitch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.spector.JobExecutionPlan;
import org.apache.flink.runtime.spector.JobReconfigAction;
import org.apache.flink.runtime.spector.controller.OperatorControllerListener;
import org.apache.flink.runtime.spector.controller.OperatorController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

		for (Map.Entry<JobVertexID, ExecutionJobVertex> entry : executionGraph.getAllVertices().entrySet()) {
			JobVertexID vertexID = entry.getKey();
			int parallelism = entry.getValue().getParallelism();
			int maxParallelism = entry.getValue().getMaxParallelism();

			// TODO scaling: using DummyStreamSwitch for test purpose
//			if (!entry.getValue().getName().toLowerCase().contains("join") && !entry.getValue().getName().toLowerCase().contains("window")) {
//				continue;
//			}
			if (entry.getValue().getName().toLowerCase().contains("flatmap")) {
				FlinkOperatorController controller = new DummyStreamSwitch();
//				FlinkOperatorController controller = new LatencyGuarantor(config);

				OperatorControllerListener listener = new OperatorControllerListenerImpl(vertexID, parallelism, maxParallelism);

				controller.init(listener, generateExecutorDelegates(parallelism), generateFinestPartitionDelegates(maxParallelism));
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

	private class OperatorControllerListenerImpl implements OperatorControllerListener {

		public final JobVertexID jobVertexID;

		private int numOpenedSubtask;

		private JobExecutionPlan oldExecutionPlan;

		private Map<String, List<String>> oldExecutorMapping;

		public OperatorControllerListenerImpl(JobVertexID jobVertexID, int parallelism, int maxParallelism) {
			this.jobVertexID = jobVertexID;
			this.numOpenedSubtask = parallelism;
		}

		@Override
		public void setup(Map<String, List<String>> executorMapping) {
			this.oldExecutionPlan = new JobExecutionPlan(executorMapping, numOpenedSubtask);
			this.oldExecutorMapping = new HashMap<>(executorMapping);
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
					executorMapping, oldExecutorMapping, oldExecutionPlan, numOpenedSubtask);

				rescaleAction.repartition(jobVertexID, jobExecutionPlan);
			} else {
				// scale out
				jobExecutionPlan = new JobExecutionPlan(
					executorMapping, oldExecutorMapping, oldExecutionPlan, newParallelism);

				rescaleAction.scaleOut(jobVertexID, newParallelism, jobExecutionPlan);
				numOpenedSubtask = newParallelism;
			}

			this.oldExecutionPlan = jobExecutionPlan;
			this.oldExecutorMapping = new HashMap<>(executorMapping);
		}
	}
}
