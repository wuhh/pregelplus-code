#include "reqresp/req-dev.h"
#include "utils/type.h"
using namespace std;

//input line format: newID (id, sub) \t old_vid in_num in1 in2 ... out_num out1 out2 ... (get mapping relation here: newID <-> old_vid)
//here ini/outi are (id, subscript_id) pairs

//output line format: v \t in_num in1 in2 ... out_num out1 out2 ... (here, old_vid is not there to make input to BCC consistent)
//vids are integers

struct BAL2Value_req {
    int newID;
    vector<intpair> in_edges;
    vector<intpair> out_edges;
};

ibinstream& operator<<(ibinstream& m, const BAL2Value_req& v)
{
    m << v.newID;
    m << v.in_edges;
    m << v.out_edges;
    return m;
}

obinstream& operator>>(obinstream& m, BAL2Value_req& v)
{
    m >> v.newID;
    m >> v.in_edges;
    m >> v.out_edges;
    return m;
}

//====================================

class BAL2Vertex_req : public RVertex<intpair, BAL2Value_req, char, int, IntPairHash> {
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

class BAL2Worker_req : public RWorker<BAL2Vertex_req> {
    char buf[100];

public:
    virtual BAL2Vertex_req* toVertex(char* line)
    {
        BAL2Vertex_req* v = new BAL2Vertex_req;
        char* pch;
        pch = strtok(line, " ");
        v->value().newID = atoi(pch);
        pch = strtok(NULL, " ");
        v->id.v1 = atoi(pch);
        pch = strtok(NULL, "\t");
        v->id.v2 = atoi(pch);
        pch = strtok(NULL, " "); //filter out old_vid
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

    virtual void toline(BAL2Vertex_req* v, BufferedWriter& writer)
    {
        //format: newID \t in-adj-list out-adj-list
        sprintf(buf, "%d\t", v->value().newID);
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

void bal_fieldbcast2(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    BAL2Worker_req worker;
    worker.run(param);
}
