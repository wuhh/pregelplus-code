
package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Writable;

public class KcorentWritable implements Writable {
	private Vector<Integer> phis, H;
	private int phi;

	public KcorentWritable(int _phi) {
		phi = _phi;
		phis = new Vector<Integer>();
		H = new Vector<Integer>();
	}

	public KcorentWritable() {
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

	public void write(DataOutput out) throws IOException {
		out.writeInt(phi);
		
		
		int size = phis.size();
		out.writeInt(size);
		
		for(int i = 0 ;i < size; i ++)
		{
			out.writeInt(phis.get(i));
			out.writeInt(H.get(i));
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
	}

	public static KcorentWritable read(DataInput in) throws IOException {
		KcorentWritable w = new KcorentWritable();
		w.readFields(in);
		return w;
	}
}