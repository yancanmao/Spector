package org.apache.flink.runtime.spector.netty.utils;

import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.spector.netty.data.NettyMessage;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelId;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class NettySocketUtils {
	private static final Logger LOG = LoggerFactory.getLogger(NettySocketUtils.class);

	public static byte[] getBytes(NettyMessage nettyMessage) throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputView dataOutputView = new DataOutputViewStreamWrapper(baos);
		nettyMessage.write(dataOutputView);
		baos.flush();

		return baos.toByteArray();
	}

	public static void chunkedWriteAndFlush(
		CompletableFuture<Acknowledge> submitFuture, Channel channel, byte[] data, ExecutionAttemptID executionAttemptID) {
		byte[] chunk;
		int dataSize = data.length;
		int chunkSize = 32 * 1024;
		int numOfChunk = (int) Math.ceil((double) dataSize / chunkSize);

		byte[] id = executionAttemptID.getBytes();

		// fire the tranmission start
		channel.writeAndFlush(executionAttemptID + ":" + dataSize);

		byte[] newChunk;

		// data transmission
		for (int i = 0; i < numOfChunk; i++) {
			chunk = Arrays.copyOfRange(
				data,
				i * chunkSize,
				Math.min(i * chunkSize + chunkSize, dataSize));
			newChunk = new byte[chunk.length + id.length];
			System.arraycopy(id, 0, newChunk, 0, id.length);
			System.arraycopy(chunk, 0, newChunk, id.length, chunk.length);
			channel.writeAndFlush(newChunk);
		}

		// fire an identifier as end of file
		channel.writeAndFlush(executionAttemptID.toHexString() + ":-1").addListener((ChannelFutureListener) channelFuture -> {
			if (channelFuture.isSuccess()) {
				submitFuture.complete(Acknowledge.get());
			} else {
				submitFuture.completeExceptionally(channelFuture.cause());
			}
		});
	}

	public static void chunkedChannelRead(
		Object msg,
		BiConsumer<ChannelHandlerContext, byte[]> consumer,
		ChannelHandlerContext ctx,
		Map<String, byte[]> recv,
		Map<String, Integer> position) {
		if (msg instanceof String) {
			String[] msgArray = ((String) msg).split(":");
			String executionAttemptId = msgArray[0];
			if (msgArray[1].equals("-1")) {
				byte[] bytes = recv.remove(executionAttemptId);
				consumer.accept(ctx, bytes);
				// release resource.
				position.remove(executionAttemptId);

				LOG.info("++++++ complete new object: " + bytes.length);
			} else {
				int length = Integer.parseInt(msgArray[1]);
				recv.computeIfAbsent(executionAttemptId, t -> new byte[length]);
 				position.putIfAbsent(executionAttemptId, 0);
				LOG.info("++++++ receive new object: " + length + " " + Thread.currentThread().getId());
			}
		} else {
			ByteBuf byteBuf = Unpooled.copiedBuffer((byte[]) msg, 0, 16);
			ExecutionAttemptID executionAttemptID = ExecutionAttemptID.fromByteBuf(byteBuf);
			byte[] chunk = Arrays.copyOfRange((byte[]) msg, 16, (((byte[]) msg).length));
			byte[] curRecv = recv.get(executionAttemptID.toHexString());
			int curPos = position.get(executionAttemptID.toHexString());
			Preconditions.checkState(chunk.length + curPos <= curRecv.length,
				"++++++ channel: " + ctx.channel().id() + " received message length exceeded: " + curRecv.length + " : " + chunk.length + curPos);
			System.arraycopy(chunk, 0, curRecv, curPos, chunk.length);
			curPos += chunk.length;
			position.put(executionAttemptID.toHexString(), curPos);
		}
	}
}
