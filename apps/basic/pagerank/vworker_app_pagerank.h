#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

struct PRValue_vworker
{
	double pr;
	vector<vwpair> edges;
};

ibinstream & operator<<(ibinstream & m, const PRValue_vworker & v){
	m<<v.pr;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, PRValue_vworker & v){
	m>>v.pr;
	m>>v.edges;
	return m;
}

//====================================

class PRVertex_vworker:public Vertex<vwpair, PRValue_vworker, double, VWPairHash>
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
				double msg=value().pr/value().edges.size();
				for(vector<vwpair>::iterator it=value().edges.begin(); it!=value().edges.end(); it++)
				{
					send_message(*it, msg);
				}
			}
			else vote_to_halt();
		}

};

//====================================

class PRAgg_vworker:public Aggregator<PRVertex_vworker, double, double>
{
	private:
		double sum;
	public:
		virtual void init(){
			sum=0;
		}

		virtual void stepPartial(PRVertex_vworker* v)
		{
			if(v->value().edges.size()==0) sum+=v->value().pr;
		}

		virtual void stepFinal(double* part)
		{
			sum+=*part;
		}

		virtual double* finishPartial(){ return &sum; }
		virtual double* finishFinal(){ return &sum; }
};

class PRWorker_vworker:public Worker<PRVertex_vworker, PRAgg_vworker>
{
	char buf[100];
	public:

		virtual PRVertex_vworker* toVertex(char* line)
		{
			char * pch;
			PRVertex_vworker* v=new PRVertex_vworker;
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

		virtual void toline(PRVertex_vworker* v, BufferedWriter & writer)
		{
			sprintf(buf, "%d\t%f\n", v->id.vid, v->value().pr);
			writer.write(buf);
		}
};

class PRCombiner_vworker:public Combiner<double>
{
	public:
		virtual void combine(double & old, const double & new_msg)
		{
			old+=new_msg;
		}
};

void vworker_pagerank(string in_path, string out_path, bool use_combiner){
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	PRWorker_vworker worker;
	PRCombiner_vworker combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	PRAgg_vworker agg;
	worker.setAggregator(&agg);
	worker.run(param);
}
