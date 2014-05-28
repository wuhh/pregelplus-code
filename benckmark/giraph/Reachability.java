package org.apache.giraph.examples;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.Vector;

import org.apache.giraph.Algorithm;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */

@Algorithm(name = "Reachability algorithm")
public class Reachability extends
	Vertex<IntWritable, IntWritable, IntWritable, IntWritable> {
    private static int IN_EDGE = 0;
    private static int OUT_EDGE = 1;
    private static String OR_AGG = "or";
    public static int SOURCE = 44881114;
    public static int DEST = -1;
    @Override
    public void compute(Iterable<IntWritable> messages) throws IOException {

	boolean finish = this.<BooleanWritable> getAggregatedValue(OR_AGG)
		.get();
	if (finish) {
	    voteToHalt();
	    return;
	}

	if (getSuperstep() == 0) {
	    int mytag = getValue().get();

	    if (mytag == 2)// v->dst
	    {
		for (Edge<IntWritable, IntWritable> edge : getEdges()) {
		    if (edge.getValue().get() == IN_EDGE) {
			sendMessage(edge.getTargetVertexId(), new IntWritable(
				mytag));
		    }
		}
	    } else if (mytag == 1)// src->v
	    {
		for (Edge<IntWritable, IntWritable> edge : getEdges()) {
		    if (edge.getValue().get() == OUT_EDGE) {
			sendMessage(edge.getTargetVertexId(), new IntWritable(
				mytag));
		    }
		}
	    }

	} else {

	    int tag = 0;
	    for (IntWritable message : messages) {
		tag |= message.get();
	    }
	    int mytag = getValue().get();
	    if ((tag | mytag) != mytag) {
		mytag |= tag;
		if (mytag == 3) {
		    aggregate(OR_AGG, new BooleanWritable(true));
		} else if (tag == 2)// v->dst
		{
		    for (Edge<IntWritable, IntWritable> edge : getEdges()) {
			if (edge.getValue().get() == IN_EDGE) {
			    sendMessage(edge.getTargetVertexId(),
				    new IntWritable(mytag));
			}
		    }
		} else if (tag == 1)// src->v
		{
		    for (Edge<IntWritable, IntWritable> edge : getEdges()) {
			if (edge.getValue().get() == OUT_EDGE) {
			    sendMessage(edge.getTargetVertexId(),
				    new IntWritable(mytag));
			}
		    }
		}
	    }
	    setValue(new IntWritable(mytag));
	}
	voteToHalt();
    }


    /**
     * Master compute associated with {@link SimplePageRankComputation}. It
     * registers required aggregators.
     */
    public static class ReachabilityMasterCompute extends DefaultMasterCompute {
	@Override
	public void initialize() throws InstantiationException,
			IllegalAccessException {
		registerAggregator(OR_AGG, BooleanOrAggregator.class);
	}
}
}