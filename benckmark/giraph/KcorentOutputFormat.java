package org.apache.giraph.examples;

import java.io.IOException;
import java.util.Vector;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class KcorentOutputFormat extends
		TextVertexOutputFormat<IntWritable, KcorentWritable, IntWritable> {
	public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
		return new KcorentVertexWriter();
	}

	/**
	 * Vertex writer used with {@link IdWithValueTextOutputFormat}.
	 */
	protected class KcorentVertexWriter extends TextVertexWriterToEachLine {
		/** Saved delimiter */
		private String delimiter = "\t";

		@Override
		protected Text convertVertexToLine(
				Vertex<IntWritable, KcorentWritable, IntWritable, ?> vertex)
				throws IOException {

			StringBuilder str = new StringBuilder();

			str.append(vertex.getId().toString());
			str.append(delimiter);
			
			Vector<Integer> phis = vertex.getValue().getPhis();
			Vector<Integer> H = vertex.getValue().getH();
			
			int size = phis.size();
			
			for(int i = 0 ;i < size; i ++)
			{
				if(i > 0)
					str.append(" ");
				str.append(H.get(i));
				str.append(" ");
				str.append(phis.get(i));
			}
			return new Text(str.toString());
		}
	}
}