package gps.examples.benchmark;


import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;


import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.writable.IntWritable;
import gps.writable.NullWritable;

public class ColorVertex extends NullEdgeVertex<IntWritable, IntWritable> {
    public ColorVertex(CommandLine line) {
	java.util.HashMap<String, String> arg_map = new java.util.HashMap<String, String>();
	gps.node.Utils.parseOtherOptions(line, arg_map);
    }
    public Random rand = new Random();

    public double myrand() {
	return rand.nextDouble();
    }
    @Override
    public void compute(Iterable<IntWritable> messageValues, int superstepNo) {

	if (superstepNo % 3 == 1) {

	    int degree = getNeighborsSize();
	    boolean selected;

	    if (degree == 0)
		selected = true;
	    else
		selected = myrand() < (1.0 / (2 * degree));

	    if (selected) {
		setValue(new IntWritable(-2));
		sendMessages(getNeighborIds(),new IntWritable(getId()));
	    }

	} else if (superstepNo % 3 == 2) {
	    if (getValue().getValue() == -1) {
		return;
	    }
	    int id = getId();
	    int min = id;
	    for (IntWritable msg : messageValues) {
		min = Math.max(min, msg.getValue());
	    }
	    if (min < id) {
		setValue(new IntWritable(-1));
	    } else {
		setValue(new IntWritable(superstepNo / 3));
		sendMessages(getNeighborIds(),new IntWritable(getId()));
		voteToHalt();
		removeEdges();
	    }
	} else if (superstepNo % 3 == 0) {

	    TreeSet<Integer> m = new TreeSet<Integer>();
	    for (IntWritable msg : messageValues) {
		m.add(msg.getValue());
	    }
	    int count = 0;
	    for (int e : this.getNeighborIds()) {
		if (m.contains(e) == false)
		    count += 1;
	    }
	    int[] nbs = new int[count];
	    count = 0;
	    for (int e : this.getNeighborIds()) {
		nbs[count] = e;
		count += 1;
	    }
	    this.removeEdges();
	    this.addEdges(nbs, new NullWritable[nbs.length]);
	}
    }

    @Override
    public IntWritable getInitialValue(int id) {
	return new IntWritable(-1);
    }

    /**
     * Factory class for {@link ConnectedComponentsVertex}.
     * 
     * @author Yi Lu
     */
    public static class ColorVertexFactory extends
	    NullEdgeVertexFactory<IntWritable, IntWritable> {

	@Override
	public NullEdgeVertex<IntWritable, IntWritable> newInstance(
		CommandLine commandLine) {
	    return new ConnectedComponentsVertex(commandLine);
	}
    }

    public static class JobConfiguration extends GPSJobConfiguration {

	@Override
	public Class<?> getVertexFactoryClass() {
	    return ColorVertexFactory.class;
	}

	@Override
	public Class<?> getVertexClass() {
	    return ColorVertex.class;
	}

	@Override
	public Class<?> getVertexValueClass() {
	    return IntWritable.class;
	}

	@Override
	public Class<?> getMessageValueClass() {
	    return IntWritable.class;
	}

	@Override
	public boolean hasVertexValuesInInput() {
	    return true;
	}
    }
}