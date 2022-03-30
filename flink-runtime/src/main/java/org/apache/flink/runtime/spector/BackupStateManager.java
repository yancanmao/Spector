package org.apache.flink.runtime.spector;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.taskmanager.Task;

import java.util.HashMap;

/**
 * Instantiated on each task manager, each Job need to instantiate one TaskStateManager on each TaskManager
 * TODO: maybe we do not need this class, we assign a task to be a backup task, other task can access the task to get the backup state.
 */
public class BackupStateManager {
	public HashMap<JobVertexID, Task> replicas;

	public BackupStateManager() {
		replicas = new HashMap<>();
	}
}
