#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

struct CCValue_vworker
{
	VertexID color;
	vector<vwpair> edges;
};

ibinstream & operator<<(ibinstream & m, const CCValue_vworker & v){
	m<<v.color;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, CCValue_vworker & v){
	m>>v.color;
	m>>v.edges;
	return m;
}

//====================================

class CCVertex_vworker:public Vertex<vwpair, CCValue_vworker, VertexID, VWPairHash>
{
	public:
		void broadcast(VertexID msg)
		{
			vector<vwpair> & nbs=value().edges;
			for(int i=0; i<nbs.size(); i++)
			{
				send_message(nbs[i], msg);
			}
		}

		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				VertexID min=id.vid;
				vector<vwpair> & nbs=value().edges;
				for(int i=0; i<nbs.size(); i++)
				{
					if(min>nbs[i].vid) min=nbs[i].vid;
				}
				value().color=min;
				broadcast(min);
				vote_to_halt();
			}
			else
			{
				VertexID min=messages[0];
				for(int i=1; i<messages.size(); i++)
				{
					if(min>messages[i]) min=messages[i];
				}
				if(min<value().color)
				{
					value().color=min;
					broadcast(min);
				}
				vote_to_halt();
			}
		}
};

class CCWorker_vworker:public Worker<CCVertex_vworker>
{
	char buf[100];

	public:
		//C version
		virtual CCVertex_vworker* toVertex(char* line)
		{
			char * pch;
			CCVertex_vworker* v=new CCVertex_vworker;
			pch=strtok(line, " ");
			v->id.vid=atoi(pch);
			pch=strtok(NULL, "\t");
			v->id.wid=atoi(pch);
			while(pch=strtok(NULL, " "))
			{
				int vid=atoi(pch);
				pch=strtok(NULL, " ");
				int wid=atoi(pch);
				v->value().edges.push_back(vwpair(vid, wid));
			}
			return v;
		}

		virtual void toline(CCVertex_vworker* v, BufferedWriter & writer)
		{
			sprintf(buf, "%d\t%d\n", v->id.vid, v->value().color);
			writer.write(buf);
		}
};

class CCCombiner_vworker:public Combiner<VertexID>
{
	public:
		virtual void combine(VertexID & old, const VertexID & new_msg)
		{
			if(old>new_msg) old=new_msg;
		}
};

void vworker_hashmin(string in_path, string out_path, bool use_combiner)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	CCWorker_vworker worker;
	CCCombiner_vworker combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	worker.run(param);
}
