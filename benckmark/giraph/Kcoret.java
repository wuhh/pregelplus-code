package org.apache.giraph.examples;

import org.apache.giraph.aggregators.IntMinAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.giraph.Algorithm;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

@Algorithm(name = "kcoret", description = "kcoret")
public class Kcoret extends
		Vertex<IntWritable, KcoretWritable, IntWritable, PairWritable> {

	public static final int RemoveEdgesAndCalculatePi = 0;
	public static final int KcoreT = 1;

	private static String TemporalPi = "TemporalPi";
	private static String UpdatedVerticesNum = "UpdatedVerticesNum";

	private int inf = (int) 1e9;

	private int subfunc(Kcoret vertex) throws Exception {
		HashMap<Integer, Integer> P = this.getValue().getP();
		int phi = this.getValue().getPhi();

		int[] cd = new int[vertex.getValue().getPhi() + 2];
		Arrays.fill(cd, 0);

		for (Edge<IntWritable, IntWritable> edge : getEdges()) {
			if (P.get(edge.getTargetVertexId().get()) > phi)
				P.put(edge.getTargetVertexId().get(), phi);
			cd[P.get(edge.getTargetVertexId().get())]++;
		}
		for (int i = phi; i >= 1; i--) {
			cd[i] += cd[i + 1];
			if (cd[i] >= i) {
				return i;
			}
		}
		throw new Exception("subfunc");
	}

	@Override
	public void compute(Iterable<PairWritable> messages) throws IOException {

		HashMap<Integer, Integer> P = this.getValue().getP();

		int phase = KcoretWorkerContext.getPhase();

		if (phase == RemoveEdgesAndCalculatePi) {

			int phi = this.getValue().getPhi();

			int pi = KcoretWorkerContext.getPI();

			Vector<Integer> phis = this.getValue().getPhis();
			Vector<Integer> H = this.getValue().getH();
			if (this.getSuperstep() > 0) {
				if (phis.size() == 0 || phi < phis.get(phis.size() - 1)) {
					phis.add(phi);
					H.add(pi);
				} else {
					H.set(H.size() - 1, pi);
				}
			}
			
			phi = this.getNumEdges();
			this.getValue().setPhi(phi);
			
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
			}

			this.setEdges(edges);

			phi = this.getNumEdges();
			this.getValue().setPhi(phi);

			for (Edge<IntWritable, IntWritable> edge : getEdges()) {

				P.put(edge.getTargetVertexId().get(), inf);
				this.sendMessage(edge.getTargetVertexId(), new PairWritable(
						this.getId().get(), this.getValue().getPhi()));
			}

			if (phi == 0) {
				this.voteToHalt();
			}

		} else {

			if (KcoretWorkerContext.getPI() == Integer.MAX_VALUE) {
				this.voteToHalt();
				return;
			}

			for (PairWritable message : messages) {
				int u = message.getKey();
				int k = message.getValue();
				if (P.get(u) > k)
					P.put(u, k);
			}

			int x = inf;
			try {
				x = subfunc(this);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (x < this.getValue().getPhi()) {
				aggregate(UpdatedVerticesNum, new IntWritable(1));

				this.getValue().setPhi(x);

				for (Edge<IntWritable, IntWritable> edge : getEdges()) {
					if (this.getValue().getPhi() < P.get(edge
							.getTargetVertexId().get())) {
						this.sendMessage(edge.getTargetVertexId(),
								new PairWritable(this.getId().get(), this
										.getValue().getPhi()));
					}
				}
			}
			//voteToHalt();
		}

	}

	public static class KcoretWorkerContext extends WorkerContext {

		private static int phase;
		private static int PI;

		public static int getPhase() {
			return phase;
		}

		public static int getPI() {
			return PI;
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
				} else if (phase == KcoreT && updatedVerticesNum == 0) {
					phase = RemoveEdgesAndCalculatePi;
				}
			}
			System.out.println("step: " + this.getSuperstep() + " phase: "
					+ phase + " updatedVerticesNum " + updatedVerticesNum
					+ " PI: " + PI);
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
	public static class KcoretMasterCompute extends DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			registerAggregator(TemporalPi, IntMinAggregator.class);
			registerAggregator(UpdatedVerticesNum, IntSumAggregator.class);

		}

	}

}
