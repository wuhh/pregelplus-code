#include "basic/pregel-dev.h"
#include <sstream>
using namespace std;
int SRC;
struct bfsValue {
    int hop;
    vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const bfsValue& v)
{
    m << v.hop;
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, bfsValue& v)
{
    m >> v.hop;
    m >> v.edges;
    return m;
}

//====================================

class bfsVertex : public Vertex<VertexID, bfsValue, VertexID> {
public:
    virtual void compute(MessageContainer& messages)
    {
        vector<VertexID>& edges = value().edges;
        if (step_num() == 1) {
            if (id == SRC) {
                value().hop = 0;
                for (int i = 0; i < edges.size(); i++) {
                    send_message(edges[i], value().hop + 1);
                }

            } else {

                value().hop = INT_MAX;
            }
        } else {
            int minHop = value().hop;
            for (int i = 0; i < messages.size(); i++) {
                minHop = min(minHop, messages[i]);
            }
            if (minHop < value().hop) {
                value().hop = minHop;
                for (int i = 0; i < edges.size(); i++) {
                    send_message(edges[i], value().hop + 1);
                }
            }
        }
        vote_to_halt();
    }
};

class bfsWorker : public Worker<bfsVertex> {
    char buf[100];

public:
    //C version
    virtual bfsVertex* toVertex(char* line)
    {
        bfsVertex* v = new bfsVertex;
        istringstream ssin(line);
        ssin >> v->id;
        int num;
        ssin >> num;
        for (int i = 0; i < num; i++) {
            int nb;
            ssin >> nb;
            v->value().edges.push_back(nb);
        }
        v->value().hop = INT_MAX;
        return v;
    }

    virtual void toline(bfsVertex* v, BufferedWriter& writer)
    {
        if (v->value().hop != INT_MAX) {
            sprintf(buf, "%d\t%d\n", v->id, v->value().hop);
            writer.write(buf);
        }
    }
};

class bfsCombiner : public Combiner<VertexID> {
public:
    virtual void combine(VertexID& old, const VertexID& new_msg)
    {
        if (old > new_msg)
            old = new_msg;
    }
};

void pregel_bfs(string in_path, string out_path, bool use_combiner, int src)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    bfsWorker worker;
    bfsCombiner combiner;
    if (use_combiner)
        worker.setCombiner(&combiner);
    SRC = src;
    worker.run(param);
}
