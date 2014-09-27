package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public class KcoreWritable implements Writable {
	private int phi;
	private HashMap<Integer, Integer> P;

	public KcoreWritable(int _phi) {
		phi = _phi;
		P = new HashMap<Integer, Integer>();
	}

	public KcoreWritable() {
		P = new HashMap<Integer, Integer>();
	}

	public int getPhi() {
		return phi;
	}

	public void setPhi(int _phi) {
		phi = _phi;
	}

	public HashMap<Integer, Integer> getP() {
		return P;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(phi);
		out.writeInt(P.size());
		for (Map.Entry<Integer, Integer> entry : P.entrySet()) {
			out.writeInt(entry.getKey());
			out.writeInt(entry.getValue());
		}
	}

	public void readFields(DataInput in) throws IOException {
		phi = in.readInt();
		int size = in.readInt();
		P.clear();
		for (int i = 0; i < size; i++) {
			int key = in.readInt();
			int value = in.readInt();
			P.put(key, value);
		}
	}

	public static KcoreWritable read(DataInput in) throws IOException {
		KcoreWritable w = new KcoreWritable();
		w.readFields(in);
		return w;
	}
}