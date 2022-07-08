/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.Serializable;

/**
 * This class encapsulates the data from the job manager to restore a task.
 */
public class JobManagerTaskRestore implements Serializable {

	private static final long serialVersionUID = 1L;

	/** The id of the checkpoint from which we restore. */
	private long restoreCheckpointId;

	/** The state for this task to restore. */
	private TaskStateSnapshot taskStateSnapshot;

	public JobManagerTaskRestore() {}

	public JobManagerTaskRestore(@Nonnegative long restoreCheckpointId, @Nonnull TaskStateSnapshot taskStateSnapshot) {
		this.restoreCheckpointId = restoreCheckpointId;
		this.taskStateSnapshot = taskStateSnapshot;
	}

	public long getRestoreCheckpointId() {
		return restoreCheckpointId;
	}

	@Nonnull
	public TaskStateSnapshot getTaskStateSnapshot() {
		return taskStateSnapshot;
	}

	@Override
	public String toString() {
		return "JobManagerTaskRestore{" +
			"restoreCheckpointId=" + restoreCheckpointId +
			", taskStateSnapshot=" + taskStateSnapshot +
			'}';
	}

	public void write(DataOutputView out) throws Exception {
		out.writeLong(restoreCheckpointId);
		taskStateSnapshot.write(out);
	}

	public void read(DataInputView in) throws Exception {
		restoreCheckpointId = in.readLong();
		taskStateSnapshot = new TaskStateSnapshot();
		taskStateSnapshot.read(in);
	}
}
