/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.spector.netty;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorGateway;
import org.apache.flink.runtime.spector.netty.data.TaskAcknowledgement;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;

/**
 * Server handler in task executor netty server.
 */
public class CheckpointCoordinatorServerHandler extends ChannelInboundHandlerAdapter {
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinatorServerHandler.class);

	private static final Time DEFAULT_RPC_TIMEOUT = Time.seconds(10);
	private final CheckpointCoordinatorGateway checkpointCoordinatorGateway;

	final byte[][] recv = {new byte[0]};
	final int[] position = {0};

	public CheckpointCoordinatorServerHandler(CheckpointCoordinatorGateway checkpointCoordinatorGateway) {
		this.checkpointCoordinatorGateway = checkpointCoordinatorGateway;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//		if (msg instanceof TaskAcknowledgement) {
//			LOG.info("++++++ Receive TaskAcknowledgement");
//			TaskAcknowledgement taskAcknowledgement = (TaskAcknowledgement) msg;
//			checkpointCoordinatorGateway.acknowledgeCheckpoint(
//				taskAcknowledgement.getJobID(),
//				taskAcknowledgement.getExecutionAttemptID(),
//				taskAcknowledgement.getCheckpointId(),
//				taskAcknowledgement.getCheckpointMetrics(),
//				taskAcknowledgement.getSubtaskState());
//		} else {
//			throw new UnsupportedOperationException();
//		}
		if (msg instanceof String) {
			if (msg.equals("-1")) {
				TaskAcknowledgement taskAcknowledgement = new TaskAcknowledgement();
				taskAcknowledgement.read(new DataInputViewStreamWrapper(new ByteArrayInputStream(recv[0])));
				checkpointCoordinatorGateway.acknowledgeCheckpoint(
					taskAcknowledgement.getJobID(),
					taskAcknowledgement.getExecutionAttemptID(),
					taskAcknowledgement.getCheckpointId(),
					taskAcknowledgement.getCheckpointMetrics(),
					taskAcknowledgement.getSubtaskState());
				// release resource.
				recv[0] = null;
				position[0] = 0;
			} else {
				int length = Integer.parseInt(((String) msg).split(":")[1]);
				recv[0] = new byte[length];
			}
		} else {
			System.arraycopy((byte[]) msg, 0, recv[0], position[0], ((byte[]) msg).length);
			position[0] += ((byte[]) msg).length;
		}
	}
}
