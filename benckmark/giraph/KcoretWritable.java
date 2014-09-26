package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.io.Writable;

public class KcoretWritable implements Writable {
	private Vector<Integer> phis, H;
	private HashMap<Integer, Integer> P;
	private int phi;

	public KcoretWritable(int _phi) {
		phi = _phi;
		P = new HashMap<Integer, Integer>();
		phis = new Vector<Integer>();
		H = new Vector<Integer>();
	}

	public KcoretWritable() {
		P = new HashMap<Integer, Integer>();
		phis = new Vector<Integer>();
		H = new Vector<Integer>();
	}

	public int getPhi() {
		return phi;
	}
	
	public Vector<Integer> getPhis()
	{
		return phis;
	}
	public Vector<Integer> getH()
	{
		return H;
	}

	public void setPhi(int _phi) {
		phi = _phi;
	}

	public HashMap<Integer, Integer> getP() {
		return P;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(phi);
		
		
		int size = phis.size();
		out.writeInt(size);
		
		for(int i = 0 ;i < size; i ++)
		{
			out.writeInt(phis.get(i));
			out.writeInt(H.get(i));
		}
		
		out.writeInt(P.size());
		for (Map.Entry<Integer, Integer> entry : P.entrySet()) {
			out.writeInt(entry.getKey());
			out.writeInt(entry.getValue());
		}
	}

	public void readFields(DataInput in) throws IOException {
		phi = in.readInt();
		int size = in.readInt();
		
		phis.clear();
		H.clear();
		
		for (int i = 0; i < size; i++) {
			int key = in.readInt();
			int value = in.readInt();
			phis.add(key);
			H.add(value);
		}
		
		size = in.readInt();
		P.clear();
		for (int i = 0; i < size; i++) {
			int key = in.readInt();
			int value = in.readInt();
			P.put(key, value);
		}
	}

	public static KcoretWritable read(DataInput in) throws IOException {
		KcoretWritable w = new KcoretWritable();
		w.readFields(in);
		return w;
	}
}