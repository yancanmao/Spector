package org.apache.flink.runtime.spector.reconfig;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.Serializable;

public class ReconfigID implements Serializable {

	private static final long serialVersionUID = -1588738478594839245L;

	private final long ReconfigID;

	private static long counter;

	private ReconfigID(long id) {
		ReconfigID = id;
	}

	public void writeTo(ByteBuf buf) {
		buf.writeLong(ReconfigID);
	}

	public static ReconfigID fromByteBuf(ByteBuf buf) {
		long ReconfigID = buf.readLong();
		return new ReconfigID(ReconfigID);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == getClass()) {
			ReconfigID other = (ReconfigID) obj;
			return this.ReconfigID == other.ReconfigID;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return (int) (this.ReconfigID + 1);
	}

	@Override
	public String toString() {
		return String.valueOf(ReconfigID);
	}

	// next ID start from 1
	public static ReconfigID generateNextID() {
		return new ReconfigID(++counter);
	}

	public static ReconfigID DEFAULT = new ReconfigID(0);
}
