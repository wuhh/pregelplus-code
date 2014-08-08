#include "ghost/ghost-dev.h"
#include <algorithm>
#include <numeric>
#include <cmath>
#include <cstdlib>
using namespace std;

struct PRValue_ghost {
    double pr;
    double delta;
    int deg;
};

ibinstream& operator<<(ibinstream& m, const PRValue_ghost& v)
{
    m << v.pr;
    m << v.delta;
    m << v.deg;
    return m;
}

obinstream& operator>>(obinstream& m, PRValue_ghost& v)
{
    m >> v.pr;
    m >> v.delta;
    m >> v.deg;
    return m;
}

//====================================

struct PRAggType {
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

//====================================

class PRVertex_ghost : public GVertex<VertexID, PRValue_ghost, double> {
public:
    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1) {
            value().pr = 1.0;
            value().delta = 1.0; //larger than 0.05 is ok
        } else {
            double pr = 0.15 + 0.85 * accumulate(messages.begin(), messages.end(), 0.0);
            value().delta = fabs(value().pr - pr);
            value().pr = pr;
        }
        if (value().deg > 0) {
            double msg = value().pr / value().deg;
            broadcast(msg);
        }
    }
};

//====================================

class PRAgg_ghost : public Aggregator<PRVertex_ghost, int, int> {
private:
    int sum;

public:
    virtual void init()
    {
        sum = 0;
    }

    virtual void stepPartial(PRVertex_ghost* v)
    {
        sum += v->value().delta > 0.05;
    }

    virtual void stepFinal(int* part)
    {
        sum += *part;
    }

    virtual int* finishPartial()
    {
        return &sum;
    }
    virtual int* finishFinal()
    {
        cout << "Not Converged #: " << sum << endl;
        if (sum == 0) {
            forceTerminate();
        }
        return &sum;
    }
};

class PRWorker_ghost : public GWorker<PRVertex_ghost, PRAgg_ghost> {
    char buf[100];

public:
    virtual PRVertex_ghost* toVertex(char* line)
    {
        int length = strlen(line);
        if (length == 0)
            return 0;

        char* pch;
        pch = strtok(line, "\t");
        PRVertex_ghost* v = new PRVertex_ghost;
        v->id = atoi(pch);
        v->value().deg = 0;
        EdgeContainer& edges = v->neighbors();
        while (pch = strtok(NULL, "\t")) {
            EdgeT edge;
            edge.id = atoi(pch);
            edges.push_back(edge);
            v->value().deg++;
        }
        return v;
    }

    virtual void toline(PRVertex_ghost* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%f\n", v->id, v->value().pr);
        writer.write(buf);
    }
};

class PRCombiner_ghost : public Combiner<double> {
public:
    virtual void combine(double& old, const double& new_msg)
    {
        old += new_msg;
    }
};

void ghost_pagerank(string in_path, string out_path, bool use_combiner)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    PRWorker_ghost worker;
    PRCombiner_ghost combiner;
    if (use_combiner)
        worker.setCombiner(&combiner);
    PRAgg_ghost agg;
    worker.setAggregator(&agg);
    worker.run(param);
}
