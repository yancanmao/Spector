package org.apache.flink.runtime.spector.migration;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.spector.migration.JobExecutionPlan;

public interface JobReconfigActor {

	JobGraph getJobGraph();

	void setInitialJobExecutionPlan(JobVertexID vertexID, JobExecutionPlan JobExecutionPlan);

	void repartition(JobVertexID vertexID, JobExecutionPlan jobExecutionPlan);

//	void scaleOut(JobVertexID vertexID, int newParallelism, JobExecutionPlan jobExecutionPlan);

//	void scaleIn(JobVertexID vertexID, int newParallelism, JobExecutionPlan jobExecutionPlan);

	enum ActionType {
		REPARTITION,
		SCALE_OUT,
		SCALE_IN
	}
}
