
package org.apache.giraph.examples;

import org.apache.giraph.combiner.Combiner;
import org.apache.hadoop.io.IntWritable;

public class ReachabilityCombiner extends Combiner<IntWritable, IntWritable> {
    @Override
    public void combine(IntWritable vertexIndex, IntWritable originalMessage,
	    IntWritable messageToCombine) {
	originalMessage.set(originalMessage.get()|messageToCombine.get());
    }

    @Override
    public IntWritable createInitialMessage() {
	return new IntWritable(0);
    }
}