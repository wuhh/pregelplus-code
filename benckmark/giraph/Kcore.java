package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.Algorithm;


import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

@Algorithm(name = "kcore", description = "kcore")
public class Kcore extends Vertex<IntWritable, KcoreWritable, NullWritable, PairWritable> {

	private int inf = (int)1e9;
	
	private int subfunc(Kcore vertex) throws Exception
	{
		HashMap<Integer, Integer> P = this.getValue().getP();
		int phi = this.getValue().getPhi();
		
		int[] cd = new int[vertex.getValue().getPhi()  + 2];
		Arrays.fill(cd, 0);
		
		for (Edge<IntWritable, NullWritable> edge : getEdges())
		{
			if(P.get(edge.getTargetVertexId().get()) > phi)
				P.put(edge.getTargetVertexId().get(), phi);
			cd[ P.get(edge.getTargetVertexId().get()) ] ++;
		}
		for(int i = phi; i >= 1; i --)
		{
			cd[i] += cd[i+1];
			if(cd[i] >= i)
			{
				return i;
			}
		}
		throw new Exception("subfunc");
	}
	
	@Override
	public void compute(Iterable<PairWritable> messages) throws IOException {
		
		HashMap<Integer, Integer> P = this.getValue().getP();
		
		if (getSuperstep() == 0) {
			
			int phi = this.getNumEdges();
			this.getValue().setPhi(phi);

			for (Edge<IntWritable, NullWritable> edge : getEdges()) {
				P.put(edge.getTargetVertexId().get(), inf);
				this.sendMessage(edge.getTargetVertexId(), new PairWritable(this.getId().get(), phi));
			}
			if(phi == 0)
			{
				voteToHalt();
			}
		} else {
			for (PairWritable message : messages) {
				int u = message.getKey();
				int k = message.getValue();
				if(P.get(u) > k)
					P.put(u, k);
			}
			
			int x = inf;
			try {
				x = subfunc(this);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(x < this.getValue().getPhi())
			{
				this.getValue().setPhi(x);
				
				for (Edge<IntWritable, NullWritable> edge : getEdges()) {
					if(this.getValue().getPhi() < P.get(edge.getTargetVertexId().get()))
					{
						this.sendMessage(edge.getTargetVertexId(), new PairWritable(this.getId().get(), this.getValue().getPhi()));
					}
				}
			}
			voteToHalt();
		}

	}
}
