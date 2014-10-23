#include "basic/pregel-dev.h"
#include <sstream>
using namespace std;

struct CCValue_pregel {
    vector<VertexID> edges;
    vector<VertexID> in_edges;
};

ibinstream& operator<<(ibinstream& m, const CCValue_pregel& v)
{
    m << v.edges;
    m << v.in_edges;
    return m;
}

obinstream& operator>>(obinstream& m, CCValue_pregel& v)
{
    m >> v.edges;
    m >> v.in_edges;
    return m;
}

//====================================

class CCVertex_pregel : public Vertex<size_t, CCValue_pregel, VertexID> {
public:
    void broadcast(VertexID msg)
    {
        vector<VertexID>& nbs = value().edges;
        for (int i = 0; i < nbs.size(); i++) {
            send_message(nbs[i], msg);
        }
    }

    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1) {
            broadcast(id);
        } else {
            value().in_edges.swap(messages);
            vote_to_halt();
        }
    }
};

class CCWorker_pregel : public Worker<CCVertex_pregel> {
    char buf[100];

public:
    //C version
    virtual CCVertex_pregel* toVertex(char* line)
    {
        CCVertex_pregel* v = new CCVertex_pregel;
        istringstream ssin(line);
        ssin >> v->id;
        int num;
        ssin >> num;
        for (int i = 0; i < num; i++) {
            int nb;
            ssin >> nb;
            v->value().edges.push_back(nb);
        }
        return v;
    }

    virtual void toline(CCVertex_pregel* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%", v->id);
        writer.write(buf);
        vector<VertexID>& nbs = v->value().edges;
        sort(nbs.begin(),nbs.end());
        sprintf(buf, "%d", nbs.size());
        writer.write(buf);
        for(int i = 0; i < nbs.size(); i ++)
        {
            sprintf(buf, " %d", nbs[i]);
            writer.write(buf);
        }
        vector<VertexID>& in_nbs = v->value().in_edges;
        sort(in_nbs.begin(),in_nbs.end());
        sprintf(buf, " %d", in_nbs.size());
        writer.write(buf);
        for(int i = 0; i < in_nbs.size(); i ++)
        {
            sprintf(buf, " %d", in_nbs[i]);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void pregel_UGTODG(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    CCWorker_pregel worker;
    worker.run(param);
}
