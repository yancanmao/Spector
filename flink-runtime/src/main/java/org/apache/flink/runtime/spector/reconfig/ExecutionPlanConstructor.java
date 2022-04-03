package org.apache.flink.runtime.spector.reconfig;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecutionPlanConstructor {
	public final JobVertexID jobVertexID;

	private int numOpenedSubtask;

	private JobExecutionPlan oldExecutionPlan;

	private Map<String, List<String>> oldExecutorMapping;

	private final JobReconfigAction reconfigAction;

	public ExecutionPlanConstructor(JobReconfigAction reconfigAction, JobVertexID jobVertexID, int parallelism, Map<String, List<String>> executorMapping) {
		this.jobVertexID = jobVertexID;
		this.numOpenedSubtask = parallelism;
		this.reconfigAction = reconfigAction;

		this.oldExecutionPlan = new JobExecutionPlan(executorMapping, numOpenedSubtask);
		this.reconfigAction.setInitialJobExecutionPlan(jobVertexID, oldExecutionPlan);
		this.oldExecutorMapping = new HashMap<>(executorMapping);
	}

	public void remap(Map<String, List<String>> executorMapping) {
		handleTreatment(executorMapping);
	}

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

			reconfigAction.repartition(jobVertexID, jobExecutionPlan);
		} else {
			// scale out
			jobExecutionPlan = new JobExecutionPlan(
				executorMapping, oldExecutorMapping, oldExecutionPlan, newParallelism);

			reconfigAction.scaleOut(jobVertexID, newParallelism, jobExecutionPlan);
			numOpenedSubtask = newParallelism;
		}

		this.oldExecutionPlan = jobExecutionPlan;
		this.oldExecutorMapping = new HashMap<>(executorMapping);
	}
}
