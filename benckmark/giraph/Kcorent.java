package org.apache.giraph.examples;

import org.apache.giraph.aggregators.IntMinAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.giraph.Algorithm;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Vector;

@Algorithm(name = "Kcorent", description = "Kcorent")
public class Kcorent extends
		Vertex<IntWritable, KcorentWritable, IntWritable, IntWritable> {

	public static final int RemoveEdgesAndCalculatePi = 0;
	public static final int KcoreT = 1;

	private static String TemporalPi = "TemporalPi";
	private static String TemporalDeg = "TemporalDeg";
	private static String UpdatedVerticesNum = "UpdatedVerticesNum";

	private int inf = (int) 1e9;

	@Override
	public void compute(Iterable<IntWritable> messages) {

		int phase = KcorentWorkerContext.getPhase();

		if (phase == RemoveEdgesAndCalculatePi) {

			int pi = KcorentWorkerContext.getPI();

			int nume = 0;

			for (Edge<IntWritable, IntWritable> edge : getEdges()) {
				if (edge.getValue().get() > pi)
					nume++;
			}

			List<Edge<IntWritable, IntWritable>> edges = Lists
					.newArrayListWithCapacity(nume);
			for (Edge<IntWritable, IntWritable> edge : getEdges()) {
				if (edge.getValue().get() > pi) {
					edges.add(EdgeFactory.create(new IntWritable(edge
							.getTargetVertexId().get()), new IntWritable(edge
							.getValue().get())));
				}
			}

			if (edges.size() > 0) {
				aggregate(TemporalPi, new IntWritable(edges.get(nume - 1)
						.getValue().get()));
				aggregate(TemporalDeg, new IntWritable(edges.size()));
			}

			this.setEdges(edges);

			int phi = this.getNumEdges();

			this.getValue().setPhi(phi);

			if (phi == 0) {
				this.voteToHalt();
			}

		} else {

			if (KcorentWorkerContext.getPI() == Integer.MAX_VALUE) {
				this.voteToHalt();
				return;
			}
			if (this.getValue().getPhi() == 0) {
				return;
			}

			int sum = 0;
			for (IntWritable message : messages) {
				sum += message.get();
			}

			int phi = this.getValue().getPhi() - sum;

			if (phi <= KcorentWorkerContext.getCURRENT_K()) {
				for (Edge<IntWritable, IntWritable> edge : getEdges()) {
					this.sendMessage(edge.getTargetVertexId(), new IntWritable(
							1));
				}

				int pi = KcorentWorkerContext.getPI();

				Vector<Integer> phis = this.getValue().getPhis();
				Vector<Integer> H = this.getValue().getH();

				if (phis.size() == 0
						|| KcorentWorkerContext.getCURRENT_K() < phis.get(phis
								.size() - 1)) {
					phis.add(KcorentWorkerContext.getCURRENT_K());
					H.add(pi);
				} else {
					H.set(H.size() - 1, pi);
				}

				this.getValue().setPhi(0);
				aggregate(UpdatedVerticesNum, new IntWritable(1));
			} else {
				this.getValue().setPhi(phi);
				aggregate(TemporalDeg, new IntWritable(phi));

			}
			// voteToHalt();
		}

	}

	public static class KcorentWorkerContext extends WorkerContext {

		private static int phase;
		private static int PI;
		private static int CURRENT_K;

		public static int getPhase() {
			return phase;
		}

		public static int getPI() {
			return PI;
		}

		public static int getCURRENT_K() {
			return CURRENT_K;
		}

		@Override
		public void preSuperstep() {

			if (this.getSuperstep() == 0)
				PI = 0;

			int updatedVerticesNum = (this
					.<IntWritable> getAggregatedValue(UpdatedVerticesNum))
					.get();

			if (this.getSuperstep() == 0) {
				phase = RemoveEdgesAndCalculatePi;
			} else {
				if (phase == RemoveEdgesAndCalculatePi) {
					PI = (this.<IntWritable> getAggregatedValue(TemporalPi))
							.get();
					phase = KcoreT;
					CURRENT_K = this.<IntWritable> getAggregatedValue(
							TemporalDeg).get();
				} else if (phase == KcoreT) {
					if (updatedVerticesNum == 0) {
						
						if(CURRENT_K == Integer.MAX_VALUE)
						{
							phase = RemoveEdgesAndCalculatePi;
						}
						else
						{
							CURRENT_K = this.<IntWritable> getAggregatedValue(
									TemporalDeg).get();
						}
					} 
				}
			}
			System.out.println("step: " + this.getSuperstep() + " phase: "
					+ phase + " updatedVerticesNum " + updatedVerticesNum
					+ " PI: " + PI + " CURRENT_K: " + CURRENT_K);
		}

		@Override
		public void postSuperstep() {

		}

		@Override
		public void preApplication() throws InstantiationException,
				IllegalAccessException {
			// TODO Auto-generated method stub

		}

		@Override
		public void postApplication() {
			// TODO Auto-generated method stub

		}
	}

	/**
	 * Master compute associated with {@link SimplePageRankComputation}. It
	 * registers required aggregators.
	 */
	public static class KcorentMasterCompute extends DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			registerAggregator(TemporalPi, IntMinAggregator.class);
			registerAggregator(TemporalDeg, IntMinAggregator.class);
			registerAggregator(UpdatedVerticesNum, IntSumAggregator.class);

		}

	}

}
