#include "ghost/ghost-dev.h"
#include "utils/type.h"
using namespace std;

struct PRValue_vwghost
{
	double pr;
	int deg;
};

ibinstream & operator<<(ibinstream & m, const PRValue_vwghost & v){
	m<<v.pr;
	m<<v.deg;
	return m;
}

obinstream & operator>>(obinstream & m, PRValue_vwghost & v){
	m>>v.pr;
	m>>v.deg;
	return m;
}

//====================================

class PRVertex_vwghost:public GVertex<vwpair, PRValue_vwghost, double, DefaultGEdge<vwpair, double>, VWPairHash>
{
	public:
		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				value().pr=1.0/get_vnum();
			}
			else
			{
				double sum=0;
				for(MessageIter it=messages.begin(); it!=messages.end(); it++)
				{
					sum+=*it;
				}
				double* agg=(double*)getAgg();
				double residual=*agg/get_vnum();
				value().pr=0.15/get_vnum()+0.85*(sum+residual);
			}
			if(step_num()<ROUND)
			{
				double msg=value().pr/value().deg;
				broadcast(msg);
			}
			else vote_to_halt();
		}

};

//====================================

class PRAgg_vwghost:public Aggregator<PRVertex_vwghost, double, double>
{
	private:
		double sum;
	public:
		virtual void init(){
			sum=0;
		}

		virtual void stepPartial(PRVertex_vwghost* v)
		{
			if(v->value().deg==0) sum+=v->value().pr;
		}

		virtual void stepFinal(double* part)
		{
			sum+=*part;
		}

		virtual double* finishPartial(){ return &sum; }
		virtual double* finishFinal(){ return &sum; }
};

class PRWorker_vwghost:public GWorker<PRVertex_vwghost, PRAgg_vwghost>
{
	char buf[100];
	public:

		virtual PRVertex_vwghost* toVertex(char* line)
		{
			char * pch;
			PRVertex_vwghost* v=new PRVertex_vwghost;
			pch=strtok(line, " ");
			v->id.vid=atoi(pch);
			pch=strtok(NULL, "\t");
			v->id.wid=atoi(pch);
			EdgeContainer & edges=v->neighbors();
			while(pch=strtok(NULL, " "))
			{
				EdgeT edge;
				edge.id.vid=atoi(pch);
				pch=strtok(NULL, " ");
				edge.id.wid=atoi(pch);
				edges.push_back(edge);
			}
			v->value().deg=edges.size();
			return v;
		}

		virtual void toline(PRVertex_vwghost* v, BufferedWriter & writer)
		{
			sprintf(buf, "%d\t%f\n", v->id.vid, v->value().pr);
			writer.write(buf);
		}
};

class PRCombiner_vwghost:public Combiner<double>
{
	public:
		virtual void combine(double & old, const double & new_msg)
		{
			old+=new_msg;
		}
};

void vwghost_pagerank(string in_path, string out_path, bool use_combiner){
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	PRWorker_vwghost worker;
	PRCombiner_vwghost combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	PRAgg_vwghost agg;
	worker.setAggregator(&agg);
	worker.run(param);
}
