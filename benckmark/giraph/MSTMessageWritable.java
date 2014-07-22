package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.Writable;


public class MSTMessageWritable implements Writable {
	private int v1;
	private int v2;
	private int v3;
	public MSTMessageWritable()
	{
		this.v1 = -1;
		this.v2 = -1;
		this.v3 = -1;
	}
	public MSTMessageWritable(int v1)
	{
		this.v1 = v1;
		this.v2 = -1;
		this.v3 = -1;
	}
	public MSTMessageWritable(int v1,int v2)
	{
		this.v1 = v1;
		this.v2 = v2;
		this.v3 = -1;
	}
	public MSTMessageWritable(int v1,int v2,int v3) {
		this.v1 = v1;
		this.v2 = v2;
		this.v3 = v3;
	}

	public int getv1()
	{
		return v1;
	}
	public int getv2()
	{
		return v2;
	}
	public int getv3()
	{
		return v3;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(v1);
		out.writeInt(v2);
		out.writeInt(v3);
	}

	public void readFields(DataInput in) throws IOException {
		v1 = in.readInt();
		v2 = in.readInt();
		v3 = in.readInt();
	}

	public static MSTMessageWritable read(DataInput in) throws IOException {
		MSTMessageWritable w = new MSTMessageWritable();
		w.readFields(in);
		return w;
	}
}