package org.apache.giraph.examples;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class KcoreOutputFormat extends
		TextVertexOutputFormat<IntWritable, KcoreWritable, NullWritable> {
	public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
		return new KcoreVertexWriter();
	}

	/**
	 * Vertex writer used with {@link IdWithValueTextOutputFormat}.
	 */
	protected class KcoreVertexWriter extends TextVertexWriterToEachLine {
		/** Saved delimiter */
		private String delimiter = "\t";

		@Override
		protected Text convertVertexToLine(
				Vertex<IntWritable, KcoreWritable, NullWritable, ?> vertex)
				throws IOException {

			StringBuilder str = new StringBuilder();

			str.append(vertex.getId().toString());
			str.append(delimiter);
			str.append(vertex.getValue().getPhi());
			return new Text(str.toString());
		}
	}
}