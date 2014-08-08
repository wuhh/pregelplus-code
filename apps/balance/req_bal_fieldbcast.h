#include "reqresp/req-dev.h"
#include "utils/type.h"
using namespace std;

//input line format: newID (id, sub) \t in_num in1 in2 ... out_num out1 out2 ...
//here ini/outi are (id, subscript_id) pairs

//output line format: v \t old_vid in_num in1 in2 ... out_num out1 out2 ...
//vids are integers

struct BALValue_req {
    int newID;
    vector<intpair> in_edges;
    vector<intpair> out_edges;
};

ibinstream& operator<<(ibinstream& m, const BALValue_req& v)
{
    m << v.newID;
    m << v.in_edges;
    m << v.out_edges;
    return m;
}

obinstream& operator>>(obinstream& m, BALValue_req& v)
{
    m >> v.newID;
    m >> v.in_edges;
    m >> v.out_edges;
    return m;
}

//====================================

class BALVertex_req : public RVertex<intpair, BALValue_req, char, int, IntPairHash> {
public:
    virtual int respond()
    {
        return value().newID;
    }

    virtual void compute(MessageContainer& messages)
    {
        vector<intpair>& in_edges = value().in_edges;
        vector<intpair>& out_edges = value().out_edges;
        if (step_num() == 1) {
            for (int i = 0; i < in_edges.size(); i++)
                request(in_edges[i]);
            for (int i = 0; i < out_edges.size(); i++)
                request(out_edges[i]);
        } else {
            vector<intpair> in_edges1;
            for (int i = 0; i < in_edges.size(); i++) {
                in_edges1.push_back(intpair(get_respond(in_edges[i]), -1));
            }
            in_edges.swap(in_edges1);
            vector<intpair> out_edges1;
            for (int i = 0; i < out_edges.size(); i++) {
                out_edges1.push_back(intpair(get_respond(out_edges[i]), -1));
            }
            out_edges.swap(out_edges1);
            vote_to_halt();
        }
    }
};

class BALWorker_req : public RWorker<BALVertex_req> {
    char buf[100];

public:
    virtual BALVertex_req* toVertex(char* line)
    {
        BALVertex_req* v = new BALVertex_req;
        char* pch;
        pch = strtok(line, " ");
        v->value().newID = atoi(pch);
        pch = strtok(NULL, " ");
        v->id.v1 = atoi(pch);
        pch = strtok(NULL, "\t");
        v->id.v2 = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        vector<intpair>& in_edges = v->value().in_edges;
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            int vid1 = atoi(pch);
            pch = strtok(NULL, " ");
            int vid2 = atoi(pch);
            in_edges.push_back(intpair(vid1, vid2));
        }
        pch = strtok(NULL, " ");
        num = atoi(pch);
        vector<intpair>& out_edges = v->value().out_edges;
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            int vid1 = atoi(pch);
            pch = strtok(NULL, " ");
            int vid2 = atoi(pch);
            out_edges.push_back(intpair(vid1, vid2));
        }
        return v;
    }

    virtual void toline(BALVertex_req* v, BufferedWriter& writer)
    {
        //format: newID \t oldID in-adj-list out-adj-list
        //oldID=-1 for dummy nodes
        int oldID = -1;
        if (v->id.v2 == 0)
            oldID = v->id.v1;
        sprintf(buf, "%d\t%d ", v->value().newID, oldID);
        writer.write(buf);
        vector<intpair>& in_edges = v->value().in_edges;
        int num = in_edges.size();
        sprintf(buf, "%d ", num);
        writer.write(buf);
        for (int i = 0; i < num; i++) {
            sprintf(buf, "%d ", in_edges[i].v1);
            writer.write(buf);
        }
        vector<intpair>& out_edges = v->value().out_edges;
        num = out_edges.size();
        sprintf(buf, "%d ", num);
        writer.write(buf);
        for (int i = 0; i < num; i++) {
            sprintf(buf, "%d ", out_edges[i].v1);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void bal_fieldbcast(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    BALWorker_req worker;
    worker.run(param);
}
