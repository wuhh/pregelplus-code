
package org.apache.giraph.examples;

import java.io.IOException;
import java.util.Vector;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.io.formats.TextVertexOutputFormat;


public class MSTOutputFormat
		extends TextVertexOutputFormat<IntWritable, MSTVertexValueWritable, MSTMessageWritable> {
	public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
		return new MSTVertexWriter();
	}

	/**
	 * Vertex writer used with {@link IdWithValueTextOutputFormat}.
	 */
	protected class MSTVertexWriter extends TextVertexWriterToEachLine {
		/** Saved delimiter */
		private String delimiter = " ";

		@Override
		protected Text convertVertexToLine(Vertex<IntWritable, MSTVertexValueWritable, MSTMessageWritable, ?> vertex)
				throws IOException {

			StringBuilder str = new StringBuilder();

			Vector<MSTMessageWritable> v = vertex.getValue().getOutput();
			
			for(MSTMessageWritable value : v)
			{
				str.append(value.getv1());
				str.append(delimiter);
				str.append(value.getv2());
				str.append(delimiter);
				str.append(value.getv3());
				str.append("\n");
			}
			return new Text(str.toString());
		}
	}
}