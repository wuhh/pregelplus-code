#include "reqresp/req-dev.h"
#include "utils/type.h"
using namespace std;

struct PRValue_resp
{
	double pr;
	vector<VertexID> edges;
};

ibinstream & operator<<(ibinstream & m, const PRValue_resp & v)
{
	m << v.pr;
	m << v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, PRValue_resp & v)
{
	m >> v.pr;
	m >> v.edges;
	return m;
}

//====================================

class PRVertex_resp: public RVertex<VertexID, PRValue_resp, char, double>
{
public:
	virtual double respond()
	{
		return value().pr / value().edges.size();
	}

	virtual void compute(MessageContainer & messages)
	{

		vector<VertexID> & edges = value().edges;
		if (step_num() == 1)
		{
			value().pr = 1.0 / get_vnum();
		}
		else
		{
			double sum = 0;
			for (int i = 0; i < edges.size(); i++)
			{
				sum += get_respond(edges[i]);
			}
			double* agg = (double*) getAgg();
			double residual = *agg / get_vnum();
			value().pr = 0.15 / get_vnum() + 0.85 * (sum + residual);
		}
		if (step_num() < ROUND)
		{
			for (int i = 0; i < edges.size(); i++) exp_respond(edges[i]);
		}
		else
			vote_to_halt();
	}
};

class PRAgg_resp: public Aggregator<PRVertex_resp, double, double>
{
	private:
		double sum;
	public:
		virtual void init()
		{
			sum = 0;
		}

		virtual void stepPartial(PRVertex_resp* v)
		{
			if (v->value().edges.size() == 0)
				sum += v->value().pr;
		}

		virtual void stepFinal(double* part)
		{
			sum += *part;
		}

		virtual double* finishPartial()
		{
			return &sum;
		}
		virtual double* finishFinal()
		{
			return &sum;
		}
};



class PRWorker_resp:public RWorker<PRVertex_resp, PRAgg_resp>
{
	char buf[100];
	public:

		virtual PRVertex_resp* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			PRVertex_resp* v=new PRVertex_resp;
			v->id=atoi(pch);
			pch=strtok(NULL, " ");
			int num=atoi(pch);
			for(int i=0; i<num; i++)
			{
				pch=strtok(NULL, " ");
				v->value().edges.push_back(atoi(pch));
			}
			return v;
		}

		virtual void toline(PRVertex_resp* v, BufferedWriter & writer)
		{
			sprintf(buf, "%d\t%f\n", v->id, v->value().pr);
			writer.write(buf);
		}
};

void resp_pagerank(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path = in_path;
	param.output_path = out_path;
	param.force_write = true;
	param.native_dispatcher = false;
	PRWorker_resp worker;
	PRAgg_resp agg;
	worker.setAggregator(&agg);
	worker.run(param);
}
