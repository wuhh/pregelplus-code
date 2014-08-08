import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//vid \t oid color sccTag ...

public class SCC {

    public static class SCCMapper extends Mapper<Text, Text, Text, Text> {

	private final static Text txt1 = new Text();
	private final static Text txt2 = new Text();
	Random ran = new Random();

	public void map(Text key, Text value, Context context)
		throws IOException, InterruptedException {
	    String kstr = key.toString();
	    if (!kstr.equals("-1")) {
		StringTokenizer tk = new StringTokenizer(value.toString());
		String color = tk.nextToken();
		String sccTag = tk.nextToken();
		if (!sccTag.equals("1"))
		    txt1.set(color + " " + sccTag);
		else
		    txt1.set(color + " " + sccTag + " " + ran.nextInt());
		StringBuffer buf = new StringBuffer(kstr + "^");
		while (tk.hasMoreTokens())
		    buf.append(tk.nextToken() + " ");
		txt2.set(buf.toString());
		context.write(txt1, txt2);
	    }
	}
    }

    public static class SCCReducer extends
	    Reducer<Text, Text, Text, NullWritable> {

	private final static Text txt1 = new Text();
	private final static NullWritable txt2 = NullWritable.get();

	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
	    StringTokenizer tk1 = new StringTokenizer(key.toString());
	    long color = Long.parseLong(tk1.nextToken());
	    byte sccTag = Byte.parseByte(tk1.nextToken());
	    if (color == -1)// trivial SCCs
	    {
		for (Text val : values) {
		    StringTokenizer tk = new StringTokenizer(val.toString(),
			    "^");
		    txt1.set("-1 " + tk.nextToken());// "-1 vid" means vid is a
						     // trivial SCC
		    context.write(txt1, txt2);
		}
	    } else {
		if (sccTag == 1)// single SCC
		{
		    for (Text val : values) {
			StringTokenizer tk = new StringTokenizer(
				val.toString(), "^");
			txt1.set(color + " " + tk.nextToken());// "color vid"
							       // means vid
							       // belongs to
							       // SCC_color
			context.write(txt1, txt2);
		    }
		} else {
		    twoBFS.Graph g = new twoBFS.Graph();
		    for (Text val : values) {
			StringTokenizer tk = new StringTokenizer(
				val.toString(), "^");
			long vid = Long.parseLong(tk.nextToken());
			twoBFS.Vertex v = null;
			if (tk.countTokens() > 1)
			    v = new twoBFS.Vertex(vid, tk.nextToken());
			else
			    v = new twoBFS.Vertex(vid);
			g.getV.put(vid, v);
		    }
		    Vector<twoBFS.Vertex> vec = g.sccInit();
		    Vector<Long> l = null;
		    int i = 0;
		    while ((l = g.nextScc(vec)) != null) {
			for (Long ele : l) {
			    txt1.set(color + "#" + i + " " + ele);// "color#sccID vid"
								  // means vid
								  // belongs
								  // to
								  // SCC_{color#sccID}
			    context.write(txt1, txt2);
			}
			i++;
		    }
		}
	    }
	}

    }

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = new Job(conf, "SCC");
	String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
	if (otherArgs.length != 2) {
	    System.err.println("Args: <in> <out>");
	    System.exit(2);
	}
	job.setJarByClass(SCC.class);

	job.setMapperClass(SCCMapper.class);
	job.setMapOutputKeyClass(Text.class);// cnt
	job.setMapOutputValueClass(Text.class);// vid

	job.setReducerClass(SCCReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(NullWritable.class);
	job.setNumReduceTasks(90);
	// job.setNumReduceTasks(1);//!!!!!!!!!!!!!!!!!!!!!!!!!!!

	job.setInputFormatClass(KeyValueTextInputFormat.class);
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
