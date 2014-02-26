#include "basic/pregel-dev.h"
#include "utils/type.h"
#include <queue>
using namespace std;

//input file
//directed graph: v \t old_vid in_num in1 in2 ... out_num out1 out2 ...

//output file
//v subscript_id \t old_vid in_num in1 in2 ... out_num out1 out2 ... (subscript_id=0 for non-dummy vertex)
//here ini/outi are now (id, subscript_id) pairs

struct OutOverflowValue_pregel {
    int old_vid;
    vector<intpair> in_edges;
    vector<intpair> out_edges;
};

ibinstream& operator<<(ibinstream& m, const OutOverflowValue_pregel& v)
{
    m << v.old_vid;
    m << v.in_edges;
    m << v.out_edges;
    return m;
}

obinstream& operator>>(obinstream& m, OutOverflowValue_pregel& v)
{
    m >> v.old_vid;
    m >> v.in_edges;
    m >> v.out_edges;
    return m;
}

//====================================
//msg={{vid, 0}, {vid, sub}}, we record <vid, sub>
class OutOverflowVertex_pregel : public Vertex<intpair, OutOverflowValue_pregel,
                                               intpair, IntPairHash> {
public:
    static int th;

    static void setDegThreshold(int threshold)
    {
        th = threshold;
    }

    OutOverflowVertex_pregel* getV(int id, int nid)
    {
        OutOverflowVertex_pregel* v = new OutOverflowVertex_pregel();
        v->id.v1 = id;
        v->id.v2 = nid;
        return v;
    }

    void addVertices(vector<OutOverflowVertex_pregel*> list)
    {
        for (int i = 0; i < list.size(); i++)
            add_vertex(list[i]);
    }

    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1) {
            vector<intpair>& out_edges = value().out_edges;
            if (out_edges.size() <= th)
                return; // no overflow

            int vid = id.v1;
            int nxtNewID = 1;

            // add bottom level dummy vertex
            queue<OutOverflowVertex_pregel*> q;
            vector<OutOverflowVertex_pregel*> vertices; // for adding

            for (int i = 0; i < out_edges.size(); i += th) {
                OutOverflowVertex_pregel* v = getV(vid, nxtNewID++);
                vertices.push_back(v);
                for (int j = i; j < min(i + th, (int)out_edges.size()); j++) {
                    v->value().out_edges.push_back(out_edges[j]);
                    intpair msg;
                    msg.v1 = vid;
                    msg.v2 = v->id.v2;
                    send_message(value().out_edges[j], msg);
                }
                q.push(v);
            }
            //clear in_egdes
            out_edges.clear();

            // start bfs, maybe need to send msgs
            while (!q.empty()) {
                if (q.size() <= th) // top level
                {
                    while (!q.empty()) {
                        OutOverflowVertex_pregel* cur = q.front();
                        q.pop();
                        cur->value().in_edges.push_back(id);
                        out_edges.push_back(cur->id);
                    }
                } else {
                    OutOverflowVertex_pregel* v = getV(vid, nxtNewID++);
                    vertices.push_back(v);
                    for (int i = 0; i < th; i++) {
                        OutOverflowVertex_pregel* cur = q.front();
                        q.pop();
                        cur->value().in_edges.push_back(v->id);
                        v->value().out_edges.push_back(cur->id);
                    }
                }
            }
            addVertices(vertices);
        } else {
            vector<intpair>& in_edges = value().in_edges;
            for (int i = 0; i < messages.size(); i++) {
                intpair msg = messages[i];
                int dstID = msg.v1;
                int newID_sub = msg.v2;
                for (int j = 0; j < in_edges.size(); j++) {
                    intpair& dst = in_edges[j];
                    if (dst.v1 == dstID)
                        dst.v2 = newID_sub;
                }
            }
        }
        vote_to_halt();
    }
};

int OutOverflowVertex_pregel::th = 100;

class OutOverflowWorker_pregel : public Worker<OutOverflowVertex_pregel> {
    char buf[100];
    bool directed;

public:
    void setDirected(bool tag)
    {
        directed = tag;
    }

    //C version
    virtual OutOverflowVertex_pregel* toVertex(char* line)
    {

        //directed graph: v \t old_vid in_num in1 in2 ... out_num out1 out2 ...
        char* pch;
        pch = strtok(line, "\t");
        OutOverflowVertex_pregel* v = new OutOverflowVertex_pregel;
        v->id.v1 = atoi(pch);
        v->id.v2 = 0;
        pch = strtok(NULL, " ");
        v->value().old_vid = atoi(pch);

        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            intpair nb(atoi(pch), 0);
            v->value().in_edges.push_back(nb);
        }
        pch = strtok(NULL, " ");
        num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            intpair nb(atoi(pch), 0);
            v->value().out_edges.push_back(nb);
        }
        return v;
    }

    virtual void toline(OutOverflowVertex_pregel* v, BufferedWriter& writer)
    {
        vector<intpair>& in_edges = v->value().in_edges;
        sprintf(buf, "%d %d\t%d %d ", v->id.v1, v->id.v2, v->value().old_vid, in_edges.size());
        writer.write(buf);
        for (int i = 0; i < in_edges.size(); i++) {
            sprintf(buf, "%d %d ", in_edges[i].v1, in_edges[i].v2);
            writer.write(buf);
        }
        vector<intpair>& out_edges = v->value().out_edges;
        sprintf(buf, "%d ", out_edges.size());
        writer.write(buf);
        for (int i = 0; i < out_edges.size(); i++) {
            sprintf(buf, "%d %d ", out_edges[i].v1, out_edges[i].v2);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void pregel_outoverflow(string in_path, string out_path, int threshold)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    OutOverflowWorker_pregel worker;
    OutOverflowVertex_pregel::setDegThreshold(threshold);
    worker.run(param);
}
