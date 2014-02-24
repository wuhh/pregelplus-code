#include "reqresp/req-dev.h"
#include "utils/type.h"
using namespace std;

//<E> = <preorder_number, tag(tree/non-tree)>
struct MinMaxEdge_req
{
	int no;
	bool is_tree;
};

ibinstream & operator<<(ibinstream & m, const MinMaxEdge_req & v){
	m<<v.no;
	m<<v.is_tree;
	return m;
}

obinstream & operator>>(obinstream & m, MinMaxEdge_req & v){
	m>>v.no;
	m>>v.is_tree;
	return m;
}

//<V> = <color, parent, vec<E>>
struct MinMaxValue_req
{
	int nd;
	int min;
	int max;
	int little;
	int big;
	int globMin;
	int globMax;
	vector<MinMaxEdge_req> edges;
};

ibinstream & operator<<(ibinstream & m, const MinMaxValue_req & v){
	m<<v.nd;
	m<<v.min;
	m<<v.max;
	m<<v.little;
	m<<v.big;
	m<<v.globMin;
	m<<v.globMax;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, MinMaxValue_req & v){
	m>>v.nd;
	m>>v.min;
	m>>v.max;
	m>>v.little;
	m>>v.big;
	m>>v.globMin;
	m>>v.globMax;
	m>>v.edges;
	return m;
}

//====================================
//the following two updates are done in parallel
//- update min(v)/max(v) for iteraion i
//- update global(v) for iteraion (i+1)
class MinMaxVertex_req:public RVertex<intpair, MinMaxValue_req, char, intpair, IntPairHash>
{
	bool is_isolated()
	{
		//return id.v2==0 && value().nd==1; //also a correct choice
		return value().edges.size()==0;
	}

	int get_i()
	{
		return step_num()-2;
	}

	public:

		virtual intpair respond()
		{
			return intpair(value().globMin, value().globMax);
		}

		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				if(is_isolated())
				{
					//initialization
					MinMaxValue_req & val=value();
					val.min=0;
					val.max=0;
				}
				else
				{//Assumption: iteration variable i=0
					int pre=id.v2;
					MinMaxValue_req & val=value();
					//initializing little(v)/big(v) (note that 2^i=2^0=1)
					val.little=pre;
					val.big=pre+val.nd-1;
					//computing localMin(v)/localMax(v)
					int min=pre;
					int max=pre;
					vector<MinMaxEdge_req> & edges=val.edges;
					for(int i=0; i<edges.size(); i++)
					{
						if(edges[i].is_tree==0)//non-tree edge
						{
							int pre_nb=edges[i].no;
							if(pre_nb<min) min=pre_nb;
							if(pre_nb>max) max=pre_nb;
						}
					}
					//initializing globalMin(v)/globalMax(v)
					val.globMin=min;
					val.globMax=max;
					//initializing min(v)/max(v)
					val.min=val.globMin;
					val.max=val.globMax;
					//========================================
					//get the following (note that 2^i=2^0=1)
					//v.min=min(local(me), local(big(v)))
					//v.max=min(local(me), local(big(v)))
					//----------------------------------------
					int color=id.v1;
					if(val.little < val.big)
					{
						//- send request to big for local(big)
						//init purpose
						intpair tgt(color, val.big);
						request(tgt);//>>>> init_request
						//----------------------------------------
						//app logic
						if(val.big % 2 != 0)
						{//ask w=big(v)-1 for globMin(w), globMax(w)
							intpair tgt(color, val.big-1);
							request(tgt);//>>>> big_request
						}
					}
					//========================================
					//get the following (step length = 2^0 = 1)
					//v.globalMin=min(globalMin(me), globalMin(me+1))
					//v.globalMax=max(globalMax(me), globalMax(me+1))
					//- send request to (me+1)
					if((pre & 1)==0) //2^(i+1)=2
					{
						intpair tgt1(color, pre+1);
						request(tgt1);//>>>> glob_request
					}
				}
			}
			else
			{
				//end condition check
				if(!hasResps())
				{
					vote_to_halt();
					return;
				}
				//////
				if(is_isolated()) return;
				//////
				int cur_i = get_i();
				int cur_pow = (1 << cur_i);
				int pre = id.v2;
				int next_i = cur_i + 1;
				int power = (1 << next_i);
				int nxt = (power << 1);
				MinMaxValue_req & val=value();
				if(pre % power ==0)
				{//>>>> glob_request
					intpair minmax_request(id.v1, pre+cur_pow);
					intpair * minmax_resp=get_respond_safe(minmax_request);
					if(minmax_resp!=NULL)
					{
						if(val.globMin > minmax_resp->v1) val.globMin = minmax_resp->v1;
						if(val.globMax < minmax_resp->v2) val.globMax = minmax_resp->v2;
					}
				}
				if(val.little<val.big)
				{
					if(step_num()==2)
					{
						//>>>> init_request
						intpair localmax_request(id.v1, val.big);
						intpair localmax_resp=get_respond(localmax_request);
						if(val.min>localmax_resp.v1) val.min=localmax_resp.v1;//set min(v) = min{min(v), msg.localMin}
						if(val.max<localmax_resp.v2) val.max=localmax_resp.v2;//set max(v) = max{max(v), msg.localMax}
					}
					if(val.little % power != 0 && step_num()>2)
					{
						//>>>> little_request
						intpair little_request(id.v1, val.little);
						intpair little_resp=get_respond(little_request);
						if(val.min > little_resp.v1) val.min = little_resp.v1;//set min(v) = min{min(v), msg.globMin}
						if(val.max < little_resp.v2) val.max = little_resp.v2;//set max(v) = max{max(v), msg.globMax}
						val.little += cur_pow;
					}
					if(val.big % power != 0)
					{
						//>>>> big_request
						intpair big_request(id.v1, val.big-cur_pow);
						intpair big_resp=get_respond(big_request);
						if(val.min > big_resp.v1) val.min = big_resp.v1;//set min(v) = min{min(v), msg.globMin}
						if(val.max < big_resp.v2) val.max = big_resp.v2;//set max(v) = max{max(v), msg.globMax}
						val.big -= cur_pow;
					}
				}
				////////
				if((val.little < val.big) && (val.little % nxt != 0))
				{//ask w=little(v) for globMin(w), globMax(w)
					intpair tgt(id.v1, val.little);
					request(tgt);
				}
				if((val.little < val.big) && (val.big % nxt != 0))
				{//ask w=big(v) for globMin(w), globMax(w)
					intpair tgt(id.v1, val.big-power);
					request(tgt);
				}
				if(pre % nxt==0)
				{//send msg to (me+2^(i+1))
					intpair tgt(id.v1, pre+power);
					request(tgt);
				}
			}
		}
};

class MinMaxWorker_req:public RWorker<MinMaxVertex_req>
{
	char buf[100];

	public:

		virtual MinMaxVertex_req* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, " ");
			MinMaxVertex_req* v=new MinMaxVertex_req;
			v->id.v1=atoi(pch);
			pch=strtok(NULL, "\t");
			v->id.v2=atoi(pch);
			pch=strtok(NULL, " ");
			v->value().nd=atoi(pch);
			vector<MinMaxEdge_req> & edges=v->value().edges;
			while(pch=strtok(NULL, " "))
			{
				MinMaxEdge_req edge;
				edge.no=atoi(pch);
				pch=strtok(NULL, " ");
				edge.is_tree=atoi(pch);
				edges.push_back(edge);
			}
			return v;
		}

		virtual void toline(MinMaxVertex_req* v, BufferedWriter & writer)
		{//output u v \t v w color
			sprintf(buf, "%d %d\t%d %d %d ", v->id.v1, v->id.v2, v->value().nd, v->value().min, v->value().max);
			writer.write(buf);
			vector<MinMaxEdge_req> & edges=v->value().edges;
			for(int i=0; i<edges.size(); i++)
			{
				sprintf(buf, "%d %d ", edges[i].no, edges[i].is_tree);
				writer.write(buf);
			}
			writer.write("\n");
		}
};

void req_minmax(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=true;
	MinMaxWorker_req worker;
	worker.run(param);
}
