package org.apache.flink.runtime.spector.netty.data;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.spector.ReconfigOptions;

import java.io.Serializable;

public class TaskDeployment implements Serializable {
	private ExecutionAttemptID executionAttemptID;
	private TaskDeploymentDescriptor tdd;
	private JobMasterId jobMasterId;
	private ReconfigOptions reconfigOptions;

	private Time timeout;

	public TaskDeployment() {}
	public TaskDeployment(
		ExecutionAttemptID executionAttemptID,
		TaskDeploymentDescriptor tdd,
		JobMasterId jobMasterId,
		ReconfigOptions reconfigOptions,
		Time timeout) {
		this.executionAttemptID = executionAttemptID;
		this.tdd = tdd;
		this.jobMasterId = jobMasterId;
		this.reconfigOptions = reconfigOptions;
		this.timeout = timeout;
	}

	public ExecutionAttemptID getExecutionAttemptID() {
		return executionAttemptID;
	}

	public TaskDeploymentDescriptor getTaskDeploymentDescriptor() {
		return tdd;
	}

	public JobMasterId getJobMasterId() {
		return jobMasterId;
	}

	public ReconfigOptions getReconfigOptions() {
		return reconfigOptions;
	}

	public Time getTimeout() {
		return timeout;
	}

	public void read(DataInputViewStreamWrapper in) {}

	public void write(DataOutputView out) {}
}
