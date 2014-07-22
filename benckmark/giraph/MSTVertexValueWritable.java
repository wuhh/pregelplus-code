
package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Writable;


public class MSTVertexValueWritable implements Writable {
	private int root;
	private int type;
	private Vector<MSTMessageWritable> output;
	public MSTVertexValueWritable()
	{
		this.root = -1;
		this.type = -1;
		this.output = new Vector<MSTMessageWritable>();
	}
	public MSTVertexValueWritable(int root,int type) {

		this.root = root;
		this.type = type;
		this.output = new Vector<MSTMessageWritable>();
	}
	public void setRoot(int root)
	{
		this.root = root;
	}
	
	public int getRoot()
	{
		return root;
	}
	public void setType(int type)
	{
		this.type = type;
	}
	public int getType()
	{
		return type;
	}
	public void addEdge(MSTMessageWritable w)
	{
		output.add(w);
	}
	public MSTMessageWritable getLastEdge()
	{
		return output.get(output.size() - 1);
	}
	public void popLastEdge()
	{
		output.remove(output.size() - 1);
	}
	public Vector<MSTMessageWritable> getOutput()
	{
		return output;
	}
	public void write(DataOutput out) throws IOException {
		out.writeInt(root);
		out.writeInt(type);
		out.writeInt(output.size());
		for(MSTMessageWritable w : output)
		{
			out.writeInt(w.getv1());
			out.writeInt(w.getv2());
			out.writeInt(w.getv3());
		}
	}

	public void readFields(DataInput in) throws IOException {
		root = in.readInt();
		type = in.readInt();
		output = new Vector<MSTMessageWritable>();
		int size = in.readInt();
		for(int i = 0 ;i < size; i ++)
		{
			int v1 = in.readInt();
			int v2 = in.readInt();
			int v3 = in.readInt();
			output.add(new MSTMessageWritable(v1,v2,v3));
		}
	}

	public static MSTVertexValueWritable read(DataInput in) throws IOException {
		MSTVertexValueWritable w = new MSTVertexValueWritable();
		w.readFields(in);
		return w;
	}
}