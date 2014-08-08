#include "basic/pregel-dev.h"
#include <set>
using namespace std;

//input file
//v \t in_num in1 in2 ... out_num out1 out2 ...

//output file
//v \t num nb1 nb2 ...

struct ToUGValue {
    vector<VertexID> in_edges;
    vector<VertexID> out_edges;
};

ibinstream& operator<<(ibinstream& m, const ToUGValue& v)
{
    m << v.in_edges;
    m << v.out_edges;
    return m;
}

obinstream& operator>>(obinstream& m, ToUGValue& v)
{
    m >> v.in_edges;
    m >> v.out_edges;
    return m;
}

//====================================
class ToUGVertex : public Vertex<VertexID, ToUGValue, VertexID> {
public:
    virtual void compute(MessageContainer& messages)
    {
        vector<VertexID>& in_nbs = value().in_edges;
        vector<VertexID>& out_nbs = value().out_edges;
        if (step_num() == 1) {
            set<VertexID> nbs;
            for (int i = 0; i < in_nbs.size(); i++) {
                nbs.insert(in_nbs[i]);
            }
            for (int i = 0; i < out_nbs.size(); i++) {
                nbs.insert(out_nbs[i]);
            }
            for (set<VertexID>::iterator it = nbs.begin(); it != nbs.end(); it++) {
                send_message(*it, id);
            }
        } else {
            in_nbs.clear(); //to get UG neighbors;
            out_nbs.clear(); //useless
            for (int i = 0; i < messages.size(); i++) {
                VertexID msg = messages[i];
                in_nbs.push_back(msg);
            }
        }
        vote_to_halt();
    }
};

class ToUGWorker : public Worker<ToUGVertex> {
    char buf[100];

public:
    //C version
    virtual ToUGVertex* toVertex(char* line)
    {
        //directed graph: v \t in_num in1 in2 ... out_num out1 out2 ...
        char* pch;
        pch = strtok(line, "\t");
        ToUGVertex* v = new ToUGVertex;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            v->value().in_edges.push_back(atoi(pch));
        }
        pch = strtok(NULL, " ");
        num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            v->value().out_edges.push_back(atoi(pch));
        }
        return v;
    }

    virtual void toline(ToUGVertex* v, BufferedWriter& writer)
    {
        vector<VertexID>& in_edges = v->value().in_edges;
        sprintf(buf, "%d \t%d ", v->id, in_edges.size());
        writer.write(buf);
        for (int i = 0; i < in_edges.size(); i++) {
            sprintf(buf, "%d ", in_edges[i]);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void toUG(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    ToUGWorker worker;
    worker.run(param);
}
