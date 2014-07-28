package org.apache.giraph.examples;

import org.apache.giraph.aggregators.BooleanAndAggregator;
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
import java.util.List;

@Algorithm(name = "MST", description = "MST")
public class MST
		extends
		Vertex<IntWritable, MSTVertexValueWritable, MSTMessageWritable, MSTMessageWritable> {
	public static final int SuperVertex = 0;
	public static final int PointsAtSupervertex = 1;
	public static final int PointsAtSubvertex = 2;
	public static final int Unknown = 3;
	public static final int DistributedMinEdgePicking_LocalPickingAndSendToRoot = 0;
	public static final int DistributedMinEdgePicking_RootPickMinEdge = 1;
	public static final int DistributedMinEdgePicking_RespondEdgeRoot = 2;
	public static final int DistributedMinEdgePicking_Finalize = 3;
	public static final int Supervertex_Finding = 4;
	public static final int PointerJumping_Request = 5;
	public static final int PointerJumping_Respond = 6;
	public static final int SupervertexFormation_Request = 7;
	public static final int SupervertexFormation_Respond = 8;
	public static final int SupervertexFormation_Update = 9;
	public static final int EdgeCleaning_Exchange = 10;
	public static final int EdgeCleanning_RemoveEdge = 11;

	private static String ifPointsAtSupervertexAGG = "super";
	private static String haltAlgorithmAGG = "halt";

	boolean edgecmp(MSTMessageWritable p1, MSTMessageWritable p2) {
		int p1v1 = p1.getv1(), p1v2 = p1.getv2(), p1v3 = p1.getv3();
		int p2v1 = p2.getv1(), p2v2 = p2.getv2(), p2v3 = p2.getv3();
		if (p1v3 < p2v3) {
			return true;
		} else {
			if (p1v1 > p1v2) {
				int tmp = p1v1;
				p1v1 = p1v2;
				p1v2 = tmp;
			}
			if (p2v1 > p2v2) {
				int tmp = p2v1;
				p2v1 = p2v2;
				p2v2 = tmp;
			}
			// ------
			if (p1v3 == p2v3 && p1v2 < p2v2) {
				return true;
			} else if (p1v3 == p2v3 && p1v2 == p2v2 && p1v1 < p2v1) {
				return true;
			}
		}
		return false;
	}

	private void sendMsg(int root, MSTMessageWritable edge) {
		// TODO Auto-generated method stub
		this.sendMessage(new IntWritable(root), edge);
	}

	private void sendMsg(int root, int v) {
		// TODO Auto-generated method stub
		this.sendMessage(new IntWritable(root), new MSTMessageWritable(v));
	}

	private void sendMsg(int root, int v1, int v2) {
		// TODO Auto-generated method stub
		this.sendMessage(new IntWritable(root), new MSTMessageWritable(v1, v2));
	}

	private MSTMessageWritable minElement(
			Iterable<Edge<IntWritable, MSTMessageWritable>> edges,
			Iterable<MSTMessageWritable> messages) {
		if (edges.iterator().hasNext() == false)
			return minElement(messages);
		else if (messages.iterator().hasNext() == false)
			return minElementFromEdges(edges);
		else {
			MSTMessageWritable minEdge = minElement(messages);
			for (Edge<IntWritable, MSTMessageWritable> e : edges) {
				MSTMessageWritable cur = e.getValue();
				if (edgecmp(cur, minEdge)) {
					minEdge = new MSTMessageWritable(cur.getv1(), cur.getv2(),
							cur.getv3());
				}
			}
			return minEdge;
		}
	}

	private MSTMessageWritable minElement(Iterable<MSTMessageWritable> edges) {
		// TODO Auto-generated method stub
		MSTMessageWritable minEdge = null;
		for (MSTMessageWritable cur : edges) {
			if (minEdge == null) {
				minEdge = new MSTMessageWritable(cur.getv1(), cur.getv2(),
						cur.getv3());
			} else if (edgecmp(cur, minEdge)) {
				minEdge = new MSTMessageWritable(cur.getv1(), cur.getv2(),
						cur.getv3());
			}
		}
		return minEdge;
	}

	private MSTMessageWritable minElementFromEdges(
			Iterable<Edge<IntWritable, MSTMessageWritable>> edges) {
		// TODO Auto-generated method stub
		MSTMessageWritable minEdge = null;
		for (Edge<IntWritable, MSTMessageWritable> e : edges) {
			MSTMessageWritable cur = e.getValue();
			if (minEdge == null) {
				minEdge = new MSTMessageWritable(cur.getv1(), cur.getv2(),
						cur.getv3());
			} else if (edgecmp(cur, minEdge)) {
				minEdge = new MSTMessageWritable(cur.getv1(), cur.getv2(),
						cur.getv3());
			}
		}
		return minEdge;
	}

	@Override
	public void compute(Iterable<MSTMessageWritable> messages)
			throws IOException {

		if (this.getSuperstep() == 0) {
			if (this.getNumEdges() == 0) {
				this.voteToHalt();
			}
			return;
		}

		int phase = MSTWorkerContext.getPhase();

		boolean haltAlgorithm  = (this.<BooleanWritable> getAggregatedValue(haltAlgorithmAGG)).get();
		
		if(this.getSuperstep() > 1 && phase == DistributedMinEdgePicking_LocalPickingAndSendToRoot &&   haltAlgorithm)
		{
			this.voteToHalt();
			return;
		}
		
		switch (phase) {
		case DistributedMinEdgePicking_LocalPickingAndSendToRoot:
			if (this.getValue().getType() == PointsAtSupervertex
					&& this.getNumEdges() > 0) {
				MSTMessageWritable edge = minElementFromEdges(this.getEdges());
				sendMsg(this.getValue().getRoot(), edge);
				this.getValue().setType(Unknown); // change to
													// PointsAtSupervertex when
													// new supervertex is found
			}
			break;
		case DistributedMinEdgePicking_RootPickMinEdge:
			if (this.getValue().getType() == SuperVertex) {
				MSTMessageWritable minEdge = minElement(this.getEdges(),
						messages);
				sendMsg(minEdge.getv2(), this.getId().get()); // request root;
				this.getValue().addEdge(minEdge);
			}
			break;
		case DistributedMinEdgePicking_RespondEdgeRoot:

			for (MSTMessageWritable msg : messages) {
				sendMsg(msg.getv1(), this.getValue().getRoot());
			}
			break;
		case DistributedMinEdgePicking_Finalize:
			if (this.getValue().getType() == SuperVertex) {
				int newroot = messages.iterator().next().getv1();
				MSTMessageWritable minEdge = this.getValue().getLastEdge();
				sendMsg(newroot, this.getId().get());
				this.getValue().setRoot(newroot);
			}
			break;
		case Supervertex_Finding:
			if (messages.iterator().hasNext()) {
				boolean nearvertex = false;
				for (MSTMessageWritable msg : messages) {
					if (msg.getv1() == this.getValue().getRoot()) {
						if (this.getId().get() < this.getValue().getRoot()) {
							this.getValue().setType(SuperVertex);
							this.getValue().setRoot(this.getId().get());
						} else {
							this.getValue().setType(PointsAtSupervertex);
							this.getValue().popLastEdge(); // remove duplicate
						}
						nearvertex = true;
						break;
					}
				}
				if (!nearvertex) {
					this.getValue().setType(PointsAtSubvertex);
				}
			} else {
				this.getValue().setType(PointsAtSubvertex);
			}
			break;

		case PointerJumping_Request:
			if (this.getValue().getType() == PointsAtSubvertex) {
				if (messages.iterator().hasNext()) {
					MSTMessageWritable msg = messages.iterator().next();
					this.getValue().setRoot(msg.getv1());
					if (msg.getv2() == 1) {
						this.getValue().setType(PointsAtSupervertex);
						break; // switch break;
					}
				}
				sendMsg(this.getValue().getRoot(), this.getId().get());
			}
			break;
		case PointerJumping_Respond:
			for (MSTMessageWritable msg : messages) {
				sendMsg(msg.getv1(), this.getValue().getRoot(), this.getValue()
						.getType() == SuperVertex ? 1 : 0);
			}
			if(this.getValue().getType() == PointsAtSubvertex)
			{
				aggregate(ifPointsAtSupervertexAGG, new BooleanWritable(false));
			}
			break;
		case SupervertexFormation_Request:
			if (this.getValue().getType() == Unknown) {
				sendMsg(this.getValue().getRoot(), this.getId().get()); // only
																		// one
																		// int
																		// is
																		// used.
			}
			break;
		case SupervertexFormation_Respond:
			for (MSTMessageWritable msg : messages) {
				sendMsg(msg.getv1(), this.getValue().getRoot()); // only one int
																	// is used.
			}
			break;
		case SupervertexFormation_Update:
			if (this.getValue().getType() == Unknown) {
				MSTMessageWritable msg = messages.iterator().next();
				this.getValue().setRoot(msg.getv1());
				this.getValue().setType(PointsAtSubvertex);
			}
			break;
		case EdgeCleaning_Exchange:
			for (Edge<IntWritable, MSTMessageWritable> e : this.getEdges()) {
				MSTMessageWritable cur = e.getValue();
				sendMsg(cur.getv2(), new MSTMessageWritable(this.getId().get(),
						this.getValue().getRoot(), cur.getv3()));
			}
			break;

		case EdgeCleanning_RemoveEdge:

			int length = 0;

			for (MSTMessageWritable msg : messages) {
				if (msg.getv2() != this.getValue().getRoot()) {
					length += 1;
					// updatedEdges.push_back(inttriplet(id,messages[i].v1,
					// messages[i].v3));
				}
			}
			List<Edge<IntWritable, MSTMessageWritable>> updatedEdges = Lists
					.newArrayListWithCapacity(length);

			for (MSTMessageWritable msg : messages) {
				if (msg.getv2() != this.getValue().getRoot()) {
					updatedEdges.add(EdgeFactory.create(
							new IntWritable(msg.getv1()),
							new MSTMessageWritable(this.getId().get(), msg
									.getv1(), msg.getv3())));
				}
			}
			this.setEdges(updatedEdges);
			
			if(this.getNumEdges() != 0)
			{
				aggregate(haltAlgorithmAGG, new BooleanWritable(false));
			}
			else
			{
				this.voteToHalt();
			}
			
			
			break;
		}

	}

	public static class MSTWorkerContext extends WorkerContext {

		private static int phase;

		public static int getPhase() {
			return phase;
		}

		@Override
		public void preSuperstep() {
			
			boolean ifPointsAtSupervertex  = (this.<BooleanWritable> getAggregatedValue(ifPointsAtSupervertexAGG)).get();
			if(this.getSuperstep() == 1)
			{
				phase = DistributedMinEdgePicking_LocalPickingAndSendToRoot;
			}
			else 
			{
				if(phase == PointerJumping_Respond)
				{
					if(ifPointsAtSupervertex == false)
						phase = PointerJumping_Request;
					else
					{
						phase = (phase + 1) % 12;
					}
				}
				else
				{
					phase = (phase + 1) % 12;
				}
			}
			System.out.println("step: " + this.getSuperstep() + " phase: " + phase);
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
	public static class MSTMasterCompute extends DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			registerAggregator(ifPointsAtSupervertexAGG,
					BooleanAndAggregator.class);
			registerAggregator(haltAlgorithmAGG, BooleanAndAggregator.class);
		
		}
		
	}
}
