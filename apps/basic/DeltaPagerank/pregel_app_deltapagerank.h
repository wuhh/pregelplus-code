#include "basic/pregel-dev.h"
using namespace std;

struct DeltaPRValue_pregel {
    double pr;
    vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const DeltaPRValue_pregel& v)
{
    m << v.pr;
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, DeltaPRValue_pregel& v)
{
    m >> v.pr;
    m >> v.edges;
    return m;
}

//====================================

class DeltaPRVertex_pregel : public Vertex<VertexID, DeltaPRValue_pregel, double> {
public:

    virtual void compute(MessageContainer& messages)
    {
    	const double EPS = 0.01;
    	double delta = 0;
    	if (step_num() == 1) {
            value().pr = 0;
        }

        for (MessageIter it = messages.begin(); it != messages.end(); it++)
        {
        	delta += *it;
        }

        vote_to_halt();

        if(delta < EPS / get_vnum()) return;

        double newPageRank = value().pr + delta;

        value().pr = newPageRank;


        if(value().edges.size() > 0)
        {
        	 double pr = newPageRank / value().edges.size();

        	 for (vector<VertexID>::iterator it = value().edges.begin(); it != value().edges.end(); it++)
        	 {
        		 send_message(*it, pr);
        	 }
        }

    }
};

class DeltaPRWorker_pregel : public Worker<DeltaPRVertex_pregel> {
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
    worker.run(param);
}


