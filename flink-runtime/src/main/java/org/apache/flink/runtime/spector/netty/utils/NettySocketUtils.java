package org.apache.flink.runtime.spector.netty.utils;

import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.spector.netty.data.NettyMessage;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class NettySocketUtils {
	public static byte[] getBytes(NettyMessage nettyMessage) throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputView dataOutputView = new DataOutputViewStreamWrapper(baos);
		nettyMessage.write(dataOutputView);
		baos.flush();

		return baos.toByteArray();
	}

	public static void chunkedWriteAndFlush(CompletableFuture<Acknowledge> submitFuture, Channel channel, byte[] data) {
		byte[] chunk;
		int dataSize = data.length;
		int chunkSize = 32 * 1024;
		int numOfChunk = (int) Math.ceil((double) dataSize / chunkSize);

		// fire the tranmission start
		channel.writeAndFlush("length:" + dataSize);

		// data transmission
		for (int i = 0; i < numOfChunk; i++) {
			chunk = Arrays.copyOfRange(
				data,
				i * chunkSize,
				Math.min(i * chunkSize + chunkSize, dataSize));
			channel.writeAndFlush(chunk);
		}

		// fire an identifier as end of file
		channel.writeAndFlush("-1").addListener((ChannelFutureListener) channelFuture -> {
			if (channelFuture.isSuccess()) {
				submitFuture.complete(Acknowledge.get());
			} else {
				submitFuture.completeExceptionally(channelFuture.cause());
			}
		});
	}

	public static void chunkedChannelRead(Object msg, Consumer<byte[]> consumer, byte[][] recv, int[] position) {
		if (msg instanceof String) {
			if (msg.equals("-1")) {
				consumer.accept(recv[0]);
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
