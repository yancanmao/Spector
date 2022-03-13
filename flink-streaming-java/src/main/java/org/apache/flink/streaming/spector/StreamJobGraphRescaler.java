/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.spector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.spector.JobGraphRescaler;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamJobGraphRescaler extends JobGraphRescaler {

	static final Logger LOG = LoggerFactory.getLogger(StreamJobGraphRescaler.class);

	public StreamJobGraphRescaler(JobGraph jobGraph, ClassLoader userCodeLoader) {
		super(jobGraph, userCodeLoader);
	}

	@Override
	public void rescale(JobVertexID id, int newParallelism, Map<Integer, List<Integer>> partitionAssignment, List<JobVertexID> involvedUpstream, List<JobVertexID> involvedDownstream) {
		JobVertex vertex = jobGraph.findVertexByID(id);
		vertex.setParallelism(newParallelism);

		repartition(id, partitionAssignment, involvedUpstream, involvedDownstream);
	}

	@Override
	public void repartition(JobVertexID id, Map<Integer, List<Integer>> partitionAssignment, List<JobVertexID> involvedUpstream, List<JobVertexID> involvedDownstream) {
	}

	@Override
	public String print(Configuration config) {
		StreamConfig streamConfig = new StreamConfig(config);

		return streamConfig.getOutEdgesInOrder(userCodeLoader).get(0).toString();
	}

}
