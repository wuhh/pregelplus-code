package org.apache.giraph.examples;

import java.io.IOException;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.io.formats.TextVertexOutputFormat;

public class ReachabilityOutputFormat extends
	TextVertexOutputFormat<IntWritable, IntWritable, IntWritable> {
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
	return new ReachabilityVertexWriter();
    }

    /**
     * Vertex writer used with {@link IdWithValueTextOutputFormat}.
     */
    protected class ReachabilityVertexWriter extends TextVertexWriterToEachLine {
	/** Saved delimiter */
	private String delimiter = "\t";

	@Override
	protected Text convertVertexToLine(
		Vertex<IntWritable, IntWritable, IntWritable, ?> vertex)
		throws IOException {

	    StringBuilder str = new StringBuilder();

	    str.append(vertex.getId().toString());
	    str.append(delimiter);
	    str.append(vertex.getValue().toString());
	    return new Text(str.toString());
	}
    }
}