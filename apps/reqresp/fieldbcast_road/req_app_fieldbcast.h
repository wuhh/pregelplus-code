#include "reqresp/req-dev.h"
#include "utils/type.h"
using namespace std;

struct FieldValue_req {
    int worker;
    vector<intpair> edges;
    vector<double> dis;
};

ibinstream& operator<<(ibinstream& m, const FieldValue_req& v)
{
    m << v.worker;
    m << v.edges;
    m << v.dis;
    return m;
}

obinstream& operator>>(obinstream& m, FieldValue_req& v)
{
    m >> v.worker;
    m >> v.edges;
    m >> v.dis;
    return m;
}

//====================================

class FieldVertex_req : public RVertex<VertexID, FieldValue_req, char, int> {
public:
    virtual int respond()
    {
        return value().worker;
    }

    virtual void compute(MessageContainer& messages)
    {
        vector<intpair>& edges = value().edges;
        if (step_num() == 1) {
            for (int i = 0; i < edges.size(); i++)
                request(edges[i].v1);
        } else {
            for (int i = 0; i < edges.size(); i++)
                edges[i].v2 = get_respond(edges[i].v1);
            vote_to_halt();
        }
    }
};

class FieldWorker_req : public RWorker<FieldVertex_req> {
    char buf[100];

public:
    virtual FieldVertex_req* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        FieldVertex_req* v = new FieldVertex_req;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        v->value().worker = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        vector<intpair>& edges = v->value().edges;
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            int vid = atoi(pch);
            edges.push_back(intpair(vid, -1));
            pch = strtok(NULL, " ");
            v->value().dis.push_back(atof(pch));
        }
        return v;
    }

    virtual void toline(FieldVertex_req* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d %d\t", v->id, v->value().worker);
        writer.write(buf);
        vector<intpair>& edges = v->value().edges;
        for (int i = 0; i < edges.size(); i++) {
            sprintf(buf, "%d %d %lf ", edges[i].v1, edges[i].v2,v->value().dis[i]);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void req_fieldbcast(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    FieldWorker_req worker;
    worker.run(param);
}
