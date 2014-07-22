#include "basic/pregel-dev.h"
#include <cmath>
using namespace std;

struct PRValue_pregel {
    double pr;
    double delta;
    vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const PRValue_pregel& v)
{
    m << v.pr;
    m << v.delta;
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, PRValue_pregel& v)
{
    m >> v.pr;
    m >> v.delta;
    m >> v.edges;
    return m;
}

//====================================

struct PRAggType
{
    double sum;
    int converge;
};

ibinstream& operator<<(ibinstream& m, const PRAggType& v)
{
    m << v.sum;
    m << v.converge;
    return m;
}

obinstream& operator>>(obinstream& m, PRAggType& v)
{
    m >> v.sum;
    m >> v.converge;
    return m;
}


class PRVertex_pregel : public Vertex<VertexID, PRValue_pregel, double> {
public:
    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1) {
            value().pr = 1.0;
        } else {
            double sum = 0;
            for (MessageIter it = messages.begin(); it != messages.end(); it++) {
                sum += *it;
            }
            PRAggType* agg = (PRAggType*)getAgg();
            double residual = agg->sum / get_vnum();
            value().delta = fabs(value().pr - (0.15 + 0.85 * (sum + residual)));
            value().pr = 0.15 + 0.85 * (sum + residual);
        }
        if (true || step_num() < ROUND) {
            double msg = value().pr / value().edges.size();
            for (vector<VertexID>::iterator it = value().edges.begin(); it != value().edges.end(); it++) {
                send_message(*it, msg);
            }
        } else
            vote_to_halt();
    }
};

//====================================


class PRAgg_pregel : public Aggregator<PRVertex_pregel, PRAggType, PRAggType> {
private:
    PRAggType value;
public:
    virtual void init()
    {
        value.sum = 0;
        value.converge = 0;
    }

    virtual void stepPartial(PRVertex_pregel* v)
    {
        if (v->value().edges.size() == 0)
            value.sum += v->value().pr;
        value.converge += v->value().delta > 0.01;
    }

    virtual void stepFinal(PRAggType* part)
    {
        value.sum += part->sum;
        value.converge += part->converge;
    }

    virtual PRAggType* finishPartial()
    {
        return &value;
    }
    virtual PRAggType* finishFinal()
    {
        if(step_num() > 1 && value.converge == 0)
            forceTerminate();
        cout << "sum: " << value.sum  << " converge: " << value.converge << endl;
        return &value;
    }
};

class PRWorker_pregel : public Worker<PRVertex_pregel, PRAgg_pregel> {
    char buf[100];

public:
    virtual PRVertex_pregel* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        PRVertex_pregel* v = new PRVertex_pregel;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            v->value().edges.push_back(atoi(pch));
        }
        return v;
    }

    virtual void toline(PRVertex_pregel* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%f\n", v->id, v->value().pr);
        writer.write(buf);
    }
};

class PRCombiner_pregel : public Combiner<double> {
public:
    virtual void combine(double& old, const double& new_msg)
    {
        old += new_msg;
    }
};

void pregel_pagerank(string in_path, string out_path, bool use_combiner)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    PRWorker_pregel worker;
    PRCombiner_pregel combiner;
    if (use_combiner)
        worker.setCombiner(&combiner);
    PRAgg_pregel agg;
    worker.setAggregator(&agg);
    worker.run(param);
}

