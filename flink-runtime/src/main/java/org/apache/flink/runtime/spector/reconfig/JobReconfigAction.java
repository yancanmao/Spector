package org.apache.flink.runtime.spector.reconfig;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;

public interface JobReconfigAction {

	JobGraph getJobGraph();

	void repartition(JobVertexID vertexID, JobExecutionPlan JobExecutionPlan);

	void scaleOut(JobVertexID vertexID, int newParallelism, JobExecutionPlan JobExecutionPlan);

	void scaleIn(JobVertexID vertexID, int newParallelism, JobExecutionPlan JobExecutionPlan);

	enum ActionType {
		REPARTITION,
		SCALE_OUT,
		SCALE_IN
	}

	default void parseParams(RescaleParamsWrapper wrapper) {
		switch (wrapper.type) {
			case REPARTITION:
				repartition(wrapper.vertexID, wrapper.JobExecutionPlan);
				break;
			case SCALE_OUT:
				scaleOut(wrapper.vertexID, wrapper.newParallelism, wrapper.JobExecutionPlan);
				break;
			case SCALE_IN:
				scaleIn(wrapper.vertexID, wrapper.newParallelism, wrapper.JobExecutionPlan);
				break;
		}
	}

	class RescaleParamsWrapper {

		final ActionType type;
		final JobVertexID vertexID;
		final int newParallelism;
		final JobExecutionPlan JobExecutionPlan;

		public RescaleParamsWrapper(
				ActionType type,
				JobVertexID vertexID,
				int newParallelism,
				JobExecutionPlan JobExecutionPlan) {
			this.type = type;
			this.vertexID = vertexID;
			this.newParallelism = newParallelism;
			this.JobExecutionPlan = JobExecutionPlan;
		}
	}
}
