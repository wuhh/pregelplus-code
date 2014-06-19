#include "basic/pregel-dev.h"
#include <float.h>
#include "utils/type.h"
using namespace std;

int src=0;

struct SPEdge_vworker
{
	double len;
	vwpair nb;
};

ibinstream & operator<<(ibinstream & m, const SPEdge_vworker & v){
	m<<v.len;
	m<<v.nb;
	return m;
}

obinstream & operator>>(obinstream & m, SPEdge_vworker & v){
	m>>v.len;
	m>>v.nb;
	return m;
}

//====================================

struct SPValue_vworker
{
	double dist;
	int from;
	vector<SPEdge_vworker> edges;
};

ibinstream & operator<<(ibinstream & m, const SPValue_vworker & v){
	m<<v.dist;
	m<<v.from;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, SPValue_vworker & v){
	m>>v.dist;
	m>>v.from;
	m>>v.edges;
	return m;
}

//====================================

struct SPMsg_vworker
{
	double dist;
	int from;
};

ibinstream & operator<<(ibinstream & m, const SPMsg_vworker & v){
	m<<v.dist;
	m<<v.from;
	return m;
}

obinstream & operator>>(obinstream & m, SPMsg_vworker & v){
	m>>v.dist;
	m>>v.from;
	return m;
}

//====================================

class SPVertex_vworker:public Vertex<vwpair, SPValue_vworker, SPMsg_vworker, VWPairHash>
{
	public:
		void broadcast()
		{
			vector<SPEdge_vworker> & nbs=value().edges;
			for(int i=0; i<nbs.size(); i++)
			{
				SPMsg_vworker msg;
				msg.dist=value().dist+nbs[i].len;
				msg.from=id.vid;
				send_message(nbs[i].nb, msg);
			}
		}

		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				if(id.vid==src)
				{
					value().dist=0;
					value().from=-1;
					broadcast();
				}
				else
				{
					value().dist=DBL_MAX;
					value().from=-1;
				}
			}
			else
			{
				SPMsg_vworker min;
				min.dist=DBL_MAX;
				for(int i=0; i<messages.size(); i++)
				{
					SPMsg_vworker msg=messages[i];
					if(min.dist>msg.dist)
					{
						min=msg;
					}
				}
				if(min.dist<value().dist)
				{
					value().dist=min.dist;
					value().from=min.from;
					broadcast();
				}
			}
			vote_to_halt();
		}

		virtual void print(){}

};

class SPWorker_vworker:public Worker<SPVertex_vworker>
{
	char buf[1000];

	public:

		//input line:
		//vid \t numNBs nb1 len1 nb2 len2 ...
		virtual SPVertex_vworker* toVertex(char* line)
		{
			char * pch;
			SPVertex_vworker* v=new SPVertex_vworker;
			pch=strtok(line, " ");
			int id=atoi(pch);
			pch=strtok(NULL, "\t");
			v->id.vid=id;
			v->id.wid=atoi(pch);
			v->value().from=-1;
			if(id==src) v->value().dist=0;
			else
			{
				v->value().dist=DBL_MAX;
				v->vote_to_halt();
			}
			while(pch=strtok(NULL, " "))
			{
				SPEdge_vworker edge;
				edge.nb.vid=atoi(pch);
				pch=strtok(NULL, " ");
				edge.nb.wid=atoi(pch);
				pch=strtok(NULL, " ");
				edge.len=atof(pch);
				v->value().edges.push_back(edge);
			}
			return v;
		}

		//output line:
		//vid \t dist from
		virtual void toline(SPVertex_vworker* v, BufferedWriter & writer)
		{
			if(v->value().dist!=DBL_MAX) sprintf(buf, "%d\t%f %d\n", v->id.vid, v->value().dist, v->value().from);
			else sprintf(buf, "%d\tunreachable\n", v->id.vid);
			writer.write(buf);
		}
};

class SPCombiner_vworker:public Combiner<SPMsg_vworker>
{
	public:
		virtual void combine(SPMsg_vworker & old, const SPMsg_vworker & new_msg)
		{
			if(old.dist>new_msg.dist) old=new_msg;
		}
};

void vworker_sssp( string in_path, string out_path, bool use_combiner){
	src=0;

	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	SPWorker_vworker worker;
	SPCombiner_vworker combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	worker.run(param);
}
