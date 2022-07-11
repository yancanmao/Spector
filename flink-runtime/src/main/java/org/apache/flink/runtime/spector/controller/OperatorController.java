package org.apache.flink.runtime.spector.controller;

import java.util.List;

public interface OperatorController {

	void init(ReconfigExecutor listener, List<String> executors, List<String> partitions);

	void start();

	void stopGracefully();

	//Method used to inform Controller
	void onMigrationExecutorsStopped();
	void onMigrationCompleted();
}
