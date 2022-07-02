package org.apache.flink.runtime.spector.netty;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.spector.netty.codec.TaskAcknowledgementDecoder;
import org.apache.flink.runtime.spector.netty.codec.TaskAcknowledgementEncoder;
import org.apache.flink.runtime.spector.netty.codec.TaskDeploymentDecoder;
import org.apache.flink.runtime.spector.netty.codec.TaskDeploymentEncoder;
import org.apache.flink.runtime.spector.netty.data.CheckpointCoordinatorSocketAddress;
import org.apache.flink.runtime.spector.netty.data.TaskAcknowledgement;
import org.apache.flink.runtime.spector.netty.data.TaskExecutorSocketAddress;
import org.apache.flink.runtime.spector.netty.socket.NettySocketClient;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;

import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class CheckpointCoordinatorNettyClient implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinatorNettyClient.class);

	private final CheckpointCoordinatorSocketAddress socketAddress;
	private final int channelCount;
	private final int connectTimeoutMills;
	private final int lowWaterMark;
	private final int highWaterMark;
	private final List<NettySocketClient> clientList;
	private final boolean taskAcknowledgementEnabled;

	public CheckpointCoordinatorNettyClient(
		CheckpointCoordinatorSocketAddress socketAddress,
		int channelCount,
		int connectTimeoutMills,
		int lowWaterMark,
		int highWaterMark,
		boolean taskAcknowledgementEnabled) {
		this.socketAddress = socketAddress;
		this.channelCount = channelCount;
		this.connectTimeoutMills = connectTimeoutMills;
		this.lowWaterMark = lowWaterMark;
		this.highWaterMark = highWaterMark;
		this.clientList = new ArrayList<>(channelCount);
		this.taskAcknowledgementEnabled = taskAcknowledgementEnabled;
	}

	public void start() throws Exception {
		Consumer<ChannelPipeline> channelPipelineConsumer;
		if (taskAcknowledgementEnabled) {
			channelPipelineConsumer = channelPipeline -> channelPipeline
				.addLast(new TaskAcknowledgementEncoder())
				.addLast(new TaskAcknowledgementDecoder());
		} else {
			channelPipelineConsumer = channelPipeline -> channelPipeline.addLast(
				new ObjectEncoder(),
				new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)));
		}

		for (int i = 0; i < channelCount; i++) {
			NettySocketClient nettySocketClient = new NettySocketClient(
				socketAddress.getAddress(),
				socketAddress.getPort(),
				connectTimeoutMills,
				lowWaterMark,
				highWaterMark,
				channelPipelineConsumer);
			nettySocketClient.start();
			clientList.add(nettySocketClient);
		}
	}

	public void acknowledgeCheckpoint(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		long checkpointId,
		CheckpointMetrics checkpointMetrics,
		TaskStateSnapshot subtaskState) {
		LOG.info("++++++ Sending acknowledgement");
		CompletableFuture<Acknowledge> submitFuture = new CompletableFuture<>();
		Channel channel = clientList.get(RandomUtils.nextInt(0, clientList.size())).getChannel();
		while (true) {
			if (channel.isWritable()) {
				try {
					TaskAcknowledgement taskAcknowledgement = new TaskAcknowledgement(
						jobID,
						executionAttemptID,
						checkpointId,
						checkpointMetrics,
						subtaskState);
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					DataOutputView dataOutputView = new DataOutputViewStreamWrapper(baos);
					taskAcknowledgement.write(dataOutputView);
					baos.flush();

					byte[] data = baos.toByteArray();
					byte[] chunk;
					int dataSize = data.length;
					int chunkSize = 32 * 1024;
					int numOfChunk = (int) Math.ceil((double) dataSize / chunkSize);

					channel.writeAndFlush("length:" + dataSize);
					for (int i = 0; i < numOfChunk; i++) {
						chunk = Arrays.copyOfRange(data, i*numOfChunk, Math.min(i * numOfChunk + chunkSize - 1, dataSize));
						channel.writeAndFlush(chunk)
							.addListener((ChannelFutureListener) channelFuture -> {
								if (channelFuture.isSuccess()) {
									submitFuture.complete(Acknowledge.get());
								} else {
									submitFuture.completeExceptionally(channelFuture.cause());
								}
							});
					}
					channel.writeAndFlush("length:" + dataSize);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
//					channel.writeAndFlush(new TaskAcknowledgement(jobID, executionAttemptID, checkpointId, checkpointMetrics, subtaskState))
//						.addListener((ChannelFutureListener) channelFuture -> {
//							if (channelFuture.isSuccess()) {
//								submitFuture.complete(Acknowledge.get());
//							} else {
//								submitFuture.completeExceptionally(channelFuture.cause());
//							}
//						});
				break;
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException ignored) { }
		}
		submitFuture.whenCompleteAsync((acknowledge, failure) -> {
			LOG.info("++++++ Channel write completed");
		});
//		return submitFuture;
	}

	@Override
	public void close() {
		for (NettySocketClient nettySocketClient : clientList) {
			try {
				nettySocketClient.closeAsync().get();
			} catch (Exception e) {
				LOG.error("Close the connection to {}:{} failed", socketAddress.getAddress(), socketAddress.getPort(), e);
			}
		}
	}
}
