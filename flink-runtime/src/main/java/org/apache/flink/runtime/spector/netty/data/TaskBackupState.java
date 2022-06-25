package org.apache.flink.runtime.spector.netty.data;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.Serializable;

public class TaskBackupState implements Serializable {
	private ExecutionAttemptID executionAttemptID;
	private JobVertexID jobvertexId;
	private JobManagerTaskRestore taskRestore;
	private Time timeout;

	public TaskBackupState() {}

	public TaskBackupState(
		ExecutionAttemptID executionAttemptID,
		JobVertexID jobvertexId,
		JobManagerTaskRestore taskRestore,
		Time timeout) {
		this.executionAttemptID = executionAttemptID;
		this.jobvertexId = jobvertexId;
		this.taskRestore = taskRestore;
		this.timeout = timeout;
	}

	public ExecutionAttemptID getExecutionAttemptID() {
		return executionAttemptID;
	}

	public JobVertexID getJobvertexId() {
		return jobvertexId;
	}

	public JobManagerTaskRestore getTaskRestore() {
		return taskRestore;
	}

	public Time getTimeout() {
		return timeout;
	}
}
