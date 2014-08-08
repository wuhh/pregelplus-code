#include "basic/pregel-dev.h"
using namespace std;

/*
 * Input: vid \t num_nb nb1 nb2 ...
 * Output: vid \t -2 0 num_in in1 in2 num_out out1 out2
 * plus -1 \t 0
 */

bool OutputControlNode = false;

struct ToGraphValue_scc {
    vector<VertexID> in_edges;
    vector<VertexID> out_edges;
};

ibinstream& operator<<(ibinstream& m, const ToGraphValue_scc& v)
{
    m << v.in_edges;
    m << v.out_edges;
    return m;
}

obinstream& operator>>(obinstream& m, ToGraphValue_scc& v)
{
    m >> v.in_edges;
    m >> v.out_edges;
    return m;
}

//====================================

class ToGraphVertex_scc : public Vertex<VertexID, ToGraphValue_scc, VertexID> {
public:
    virtual void compute(MessageContainer& messages)
    {
        vector<VertexID>& in_edges = value().in_edges;
        vector<VertexID>& out_edges = value().out_edges;

        if (step_num() == 1) {
            for (int i = 0; i < out_edges.size(); i++) {
                send_message(out_edges[i], id);
            }
        } else {
            in_edges.swap(messages);
            vote_to_halt();
        }
    }
};

class ToGraphWorker_scc : public Worker<ToGraphVertex_scc> {
    char buf[100];

public:
    //C version
    virtual ToGraphVertex_scc* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        ToGraphVertex_scc* v = new ToGraphVertex_scc;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            v->value().out_edges.push_back(atoi(pch));
        }
        return v;
    }

    virtual void toline(ToGraphVertex_scc* v, BufferedWriter& writer)
    {
        if (OutputControlNode == false && _my_rank == 0) {
            sprintf(buf, "-1\t0\n");
            writer.write(buf);
            OutputControlNode = true;
        }

        vector<VertexID>& in_edges = v->value().in_edges;
        vector<VertexID>& out_edges = v->value().out_edges;
        sprintf(buf, "%d\t-2 0", v->id);
        writer.write(buf);

        sprintf(buf, " %d", in_edges.size());
        writer.write(buf);
        for (int i = 0; i < in_edges.size(); i++) {
            sprintf(buf, " %d", in_edges[i]);
            writer.write(buf);
        }
        sprintf(buf, " %d", out_edges.size());
        writer.write(buf);
        for (int i = 0; i < out_edges.size(); i++) {
            sprintf(buf, " %d", out_edges[i]);
            writer.write(buf);
        }
        sprintf(buf, "\n");
        writer.write(buf);
    }
};

void pregel_tosccgraphformat(string in_path, string out_path)
{
    OutputControlNode = false;
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    ToGraphWorker_scc worker;
    worker.run(param);
}
