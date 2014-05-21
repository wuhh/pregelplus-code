#include "basic/pregel-dev.h"
using namespace std;

struct DeltaPRValue_pregel {
    double pr;
    double delta;
    vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const DeltaPRValue_pregel& v)
{
    m << v.pr;
    m << v.delta;
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, DeltaPRValue_pregel& v)
{
    m >> v.pr;
    m >> v.delta;
    m >> v.edges;
    return m;
}

//====================================

class DeltaPRVertex_pregel : public Vertex<VertexID, DeltaPRValue_pregel, double> {
public:

    virtual void compute(MessageContainer& messages)
    {
    	if(step_num() >= 2)
    	{
    		int* agg = (int*)getAgg();
    		if(*agg == get_vnum())
    		{
    			vote_to_halt();
    			return;
    		}
    	}

    	value().delta = 0;
    	if (step_num() == 1) {
            value().pr = 0;
            value().delta = 0.15;
        }

        for (MessageIter it = messages.begin(); it != messages.end(); it++)
        {
        	value().delta  += *it;
        }

        value().pr += value().delta ;


        if(value().edges.size() > 0)
        {
        	 double updateToNeighbor = value().pr / value().edges.size();

        	 for (vector<VertexID>::iterator it = value().edges.begin(); it != value().edges.end(); it++)
        	 {
        		 send_message(*it, updateToNeighbor);
        	 }
        }

    }
};
class DeltaPRAgg_pregel : public Aggregator<DeltaPRVertex_pregel, int, int> {
private:
    int counter;

public:
    virtual void init()
    {
        counter = 0;
    }

    virtual void stepPartial(DeltaPRVertex_pregel* v)
    {
    	const double EPS = 0.01;
    	if(v->value().delta < EPS / get_vnum())
    		counter++;
    }

    virtual void stepFinal(int* part)
    {
    	counter += *part;
    }

    virtual int* finishPartial()
    {
        return &counter;
    }
    virtual int* finishFinal()
    {
    	cout << "Total: " << get_vnum() << " On converge: " << counter << endl;
        return &counter;
    }
};

class DeltaPRWorker_pregel : public Worker<DeltaPRVertex_pregel,DeltaPRAgg_pregel> {
    char buf[100];

public:
    virtual DeltaPRVertex_pregel* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        DeltaPRVertex_pregel* v = new DeltaPRVertex_pregel;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            v->value().edges.push_back(atoi(pch));
        }
        return v;
    }

    virtual void toline(DeltaPRVertex_pregel* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%f\n", v->id, v->value().pr);
        writer.write(buf);
    }
};

class DeltaPRCombiner_pregel : public Combiner<double> {
public:
    virtual void combine(double& old, const double& new_msg)
    {
        old += new_msg;
    }
};

void pregel_deltapagerank(string in_path, string out_path, bool use_combiner)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    DeltaPRWorker_pregel worker;
    DeltaPRCombiner_pregel combiner;
    if (use_combiner)
        worker.setCombiner(&combiner);
    DeltaPRAgg_pregel agg;
    worker.setAggregator(&agg);
    worker.run(param);
}


