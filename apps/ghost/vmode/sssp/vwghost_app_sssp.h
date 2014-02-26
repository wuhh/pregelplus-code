#include "ghost/ghost-dev.h"
#include <float.h>
#include "utils/type.h"
using namespace std;

int src = 0;

//====================================

struct SPMsg_vwghost {
    double dist;
    int from;

    SPMsg_vwghost(double _dist = 0, int _from = 0)
        : dist(_dist)
        , from(_from)
    {
    }
};

ibinstream& operator<<(ibinstream& m, const SPMsg_vwghost& v)
{
    m << v.dist;
    m << v.from;
    return m;
}

obinstream& operator>>(obinstream& m, SPMsg_vwghost& v)
{
    m >> v.dist;
    m >> v.from;
    return m;
}

struct SPGEdge : public GEdge<vwpair, SPMsg_vwghost, double> {

    void relay(MessageType& msg)
    {
        msg.dist += eval;
    }
};

class SPVertex_vwghost : public GVertex<vwpair, SPMsg_vwghost, SPMsg_vwghost, SPGEdge, VWPairHash> {
public:
    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1) {
            if (id.vid == src) {
                //value().dist = 0; //done during init
                //value().from = -1;
                broadcast(SPMsg_vwghost(value().dist, id.vid));
            } //else {
            //value().dist = DBL_MAX;
            //value().from = -1;
            //}
        } else {
            SPMsg_vwghost min;
            min.dist = DBL_MAX;
            for (int i = 0; i < messages.size(); i++) {
                SPMsg_vwghost msg = messages[i];
                if (min.dist > msg.dist) {
                    min = msg;
                }
            }
            if (min.dist < value().dist) {
                value().dist = min.dist;
                value().from = min.from;
                broadcast(SPMsg_vwghost(value().dist, id.vid));
            }
        }
        vote_to_halt();
    }
};

class SPWorker_vwghost : public GWorker<SPVertex_vwghost> {
    char buf[100];

public:
    virtual SPVertex_vwghost* toVertex(char* line)
    {
        char* pch;
        SPVertex_vwghost* v = new SPVertex_vwghost;
        pch = strtok(line, " ");
        int id = atoi(pch);
        v->id.vid = id;
        pch = strtok(NULL, "\t");
        v->id.wid = atoi(pch);
        v->value().from = -1;
        if (id == src)
            v->value().dist = 0;
        else {
            v->value().dist = DBL_MAX;
            v->vote_to_halt();
        }
        EdgeContainer& edges = v->neighbors();
        while (pch = strtok(NULL, " ")) {
            EdgeT edge;
            edge.id.vid = atoi(pch);
            pch = strtok(NULL, " ");
            edge.id.wid = atoi(pch);
            pch = strtok(NULL, " ");
            edge.eval = atof(pch);
            edges.push_back(edge);
        }
        return v;
    }

    virtual void toline(SPVertex_vwghost* v, BufferedWriter& writer)
    {
        if (v->value().dist != DBL_MAX)
            sprintf(buf, "%d\t%f %d\n", v->id.vid, v->value().dist, v->value().from);
        else
            sprintf(buf, "%d\tunreachable\n", v->id);
        writer.write(buf);
    }
};

class SPCombiner_vwghost : public Combiner<SPMsg_vwghost> {
public:
    virtual void combine(SPMsg_vwghost& old, const SPMsg_vwghost& new_msg)
    {
        if (old.dist > new_msg.dist)
            old = new_msg;
    }
};

void vwghost_sssp(int srcID, string in_path, string out_path, bool use_combiner)
{
    src = srcID;

    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    SPWorker_vwghost worker;
    SPCombiner_vwghost combiner;
    if (use_combiner)
        worker.setCombiner(&combiner);
    worker.run(param);
}
