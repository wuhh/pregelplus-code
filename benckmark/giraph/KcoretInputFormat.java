
package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Input format for HashMin IntWritable, NullWritable, NullWritable Vertex ,
 * Vertex Value, Edge Value Graph vertex \t neighbor1 neighbor 2
 */
public class KcoretInputFormat extends
		TextVertexInputFormat<IntWritable, KcoretWritable, IntWritable> {
    /** Separator of the vertex and neighbors */
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexReader createVertexReader(InputSplit split,
	    TaskAttemptContext context) throws IOException {
    	return new KcoretVertexReader();
    }

    /**
     * Vertex reader associated with {@link IntIntNullTextInputFormat}.
     */
    public class KcoretVertexReader extends
	    TextVertexReaderFromEachLineProcessed<String[]> {
	private IntWritable id;

	@Override
	protected String[] preprocessLine(Text line) throws IOException {
	    String[] tokens = SEPARATOR.split(line.toString());
	    id = new IntWritable(Integer.parseInt(tokens[0]));
	    return tokens;
	}

	@Override
	protected IntWritable getId(String[] tokens) throws IOException {
	    return id;
	}

	@Override
	protected KcoretWritable getValue(String[] tokens) throws IOException {
	    return new KcoretWritable();
	}

	@Override
	protected Iterable<Edge<IntWritable, IntWritable>> getEdges(
		String[] tokens) throws IOException {
	    
		int num = Integer.parseInt(tokens[1]);
		
		List<Edge<IntWritable, IntWritable>> edges = Lists
			    .newArrayListWithCapacity(num);
		
		
		for(int i = 0, it = 2; i < num; i ++)
		{
			int v = Integer.parseInt(tokens[it++]);
			int t = Integer.parseInt(tokens[it++]);
			it += t;	
			edges.add(EdgeFactory.create(new IntWritable(v),new IntWritable(t) ));
		}
		
		Collections.sort(edges,new Comparator<Edge<IntWritable, IntWritable>>(){  
            @Override  
            public int compare(Edge<IntWritable, IntWritable> b1, Edge<IntWritable, IntWritable> b2) {  
            	return b1.getValue().get() -  b2.getValue().get();
            }  
              
        });
		
	    return edges;
	}
    }
}