package org.apache.giraph.examples;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

import org.apache.giraph.Algorithm;

@Algorithm(name = "Page rank")
public class Pagerank
		extends
		Vertex<IntWritable, PagerankWritable, NullWritable, DoubleWritable> {

	public static final int MAX_SUPERSTEPS = Integer.MAX_VALUE;
	public static final double UNIT = 1.0;
	public static double EPS = 0.01;
	private static String SUM_AGG = "sum";
	private static String CONVERGE_AGG = "converge";

	@Override
	public void compute(
			Iterable<DoubleWritable> messages) throws IOException {

		if (getSuperstep() == 0) {
			setValue(new PagerankWritable(UNIT));
		}
		if (getSuperstep() >= 1) {

			double sum = 0;
			for (DoubleWritable message : messages) {
				sum = sum + message.get();
			}
			BooleanWritable converge = getAggregatedValue(CONVERGE_AGG);
			if (converge.get()) {
				voteToHalt();
				return;
			}
			DoubleWritable residual = getAggregatedValue(SUM_AGG);
			double value = 0.15 + 0.85 * (sum + residual.get());
			double delta = Math.abs(value - getValue().getPageRank());
			setValue(new PagerankWritable(value, delta));
		}

		if (getSuperstep() < MAX_SUPERSTEPS) {
			long edges = getNumEdges();
			sendMessageToAllEdges(new DoubleWritable(getValue()
					.getPageRank() / edges));
		} else {
			voteToHalt();
		}
		if (getNumEdges() == 0) {
			aggregate(SUM_AGG, new DoubleWritable(getValue()
					.getPageRank() / getTotalNumVertices()));
		}
		aggregate(CONVERGE_AGG, new BooleanWritable(getValue()
				.getDelta() <= EPS));
	}

	public static class PagerankWorkerContext extends WorkerContext {

		private static double FINAL_SUM;
		private static boolean FINAL_CONVERGE;

		public static double getFinalSum() {
			return FINAL_SUM;
		}

		public static boolean getFinalConverge() {
			return FINAL_CONVERGE;
		}

		@Override
		public void preApplication() throws InstantiationException,
				IllegalAccessException {
		}

		@Override
		public void postApplication() {
			FINAL_SUM = this.<DoubleWritable> getAggregatedValue(SUM_AGG).get();
			FINAL_CONVERGE = this.<BooleanWritable> getAggregatedValue(
					CONVERGE_AGG).get();
		}

		@Override
		public void preSuperstep() {

		}

		@Override
		public void postSuperstep() {
		}
	}

	public static class PagerankMasterCompute extends DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			registerAggregator(SUM_AGG, DoubleSumAggregator.class);
			registerAggregator(CONVERGE_AGG, BooleanAndAggregator.class);
		}
	}
}