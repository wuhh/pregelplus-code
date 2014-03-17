package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.Algorithm;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

@Algorithm(name = "Color", description = "Color")
public class Color extends
	Vertex<IntWritable, IntWritable, NullWritable, IntWritable> {
    public Random rand = new Random();

    public double myrand() {
	return rand.nextDouble();
    }

    @Override
    public void compute(Iterable<IntWritable> messages) throws IOException {
	if (getSuperstep() == 0) {
	    setValue(new IntWritable(-1));
	    return;
	}
	if (getSuperstep() % 3 == 1) {

	    int degree = getNumEdges();
	    boolean selected;

	    if (degree == 0)
		selected = true;
	    else
		selected = myrand() < (1.0 / (2 * degree));

	    if (selected) {
		setValue(new IntWritable(-2));
		sendMessageToAllEdges(getValue());
	    }

	} else if (getSuperstep() % 3 == 2) {
	    if (getValue().get() == -1) {
		return;
	    }
	    int id = getId().get();
	    int min = id;
	    for (IntWritable msg : messages) {
		min = Math.max(min, msg.get());
	    }
	    if (min < id) {
		setValue(new IntWritable(-1));
	    } else {
		setValue(new IntWritable((int) (getSuperstep() / 3)));
		sendMessageToAllEdges(getId());
		voteToHalt();
		List<Edge<IntWritable, NullWritable>> edges = Lists
			.newArrayList();
		setEdges(edges);
	    }
	} else if (getSuperstep() % 3 == 0) {

	    List<Edge<IntWritable, NullWritable>> new_nbs = Lists
		    .newArrayList();
	    TreeSet<Integer> m = new TreeSet<Integer>();
	    for (IntWritable msg : messages) {
		m.add(msg.get());
	    }
	    for (Edge<IntWritable, NullWritable> e : this.getEdges()) {
		if (m.contains(e) == false)
		    new_nbs.add(EdgeFactory.create(e.getTargetVertexId()));
	    }
	    setEdges(new_nbs);
	}

    }
}
