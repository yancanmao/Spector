package org.apache.flink.runtime.spector;

import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

class TaskReconfigMeta {
	private final ReconfigID reconfigId;
	private final ReconfigOptions reconfigOptions;

	private final Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors;
	private final Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors;

	private final ResultPartition[] newPartitions;

	private final Collection<Integer> affectedKeygroups;

	TaskReconfigMeta(
		ReconfigID reconfigId,
		ReconfigOptions reconfigOptions,
		Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
		Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
		Collection<Integer> affectedKeygroups) {

		this.reconfigId = checkNotNull(reconfigId);
		this.reconfigOptions = checkNotNull(reconfigOptions);

		this.resultPartitionDeploymentDescriptors = checkNotNull(resultPartitionDeploymentDescriptors);
		this.inputGateDeploymentDescriptors = checkNotNull(inputGateDeploymentDescriptors);

		this.newPartitions = new ResultPartition[resultPartitionDeploymentDescriptors.size()];

		this.affectedKeygroups = affectedKeygroups;
	}

	public ReconfigID getReconfigId() {
		return reconfigId;
	}

	public ReconfigOptions getReconfigOptions() {
		return reconfigOptions;
	}

	public Collection<ResultPartitionDeploymentDescriptor> getResultPartitionDeploymentDescriptors() {
		return resultPartitionDeploymentDescriptors;
	}

	public Collection<InputGateDeploymentDescriptor> getInputGateDeploymentDescriptors() {
		return inputGateDeploymentDescriptors;
	}

	public ResultPartition getNewPartitions(int index) {
		checkState(index >= 0 && index < newPartitions.length, "given index out of boundary");

		return newPartitions[index];
	}

	public void addNewPartitions(int index, ResultPartition partition) {
		checkState(index >= 0 && index < newPartitions.length, "given index out of boundary");

		newPartitions[index] = partition;
	}

	public Collection<Integer> getAffectedKeygroups() {
		return affectedKeygroups;
	}

	public InputGateDeploymentDescriptor getMatchedInputGateDescriptor(SingleInputGate gate) {
		for (InputGateDeploymentDescriptor igdd : inputGateDeploymentDescriptors) {
			if (gate.getConsumedResultId().equals(igdd.getConsumedResultId())
				&& gate.getConsumedSubpartitionIndex() == igdd.getConsumedSubpartitionIndex()) {
				return igdd;
			}
		}
		throw new IllegalStateException("Cannot find matched InputGateDeploymentDescriptor");
	}
}
