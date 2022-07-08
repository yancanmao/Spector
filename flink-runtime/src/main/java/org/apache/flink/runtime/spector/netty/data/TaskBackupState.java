package org.apache.flink.runtime.spector.netty.data;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.Serializable;

public class TaskBackupState implements NettyMessage, Serializable {
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

    public void write(DataOutputView out) throws Exception {
		out.writeLong(executionAttemptID.getLowerPart());
		out.writeLong(executionAttemptID.getUpperPart());
		out.writeLong(jobvertexId.getLowerPart());
		out.writeLong(jobvertexId.getUpperPart());
		taskRestore.write(out);
    }

	public void read(DataInputView in) throws Exception {
		executionAttemptID = new ExecutionAttemptID(in.readLong(), in.readLong());
		jobvertexId = new JobVertexID(in.readLong(), in.readLong());
		taskRestore = new JobManagerTaskRestore();
		taskRestore.read(in);
	}
}
