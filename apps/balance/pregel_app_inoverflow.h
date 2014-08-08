#include "basic/pregel-dev.h"
#include "utils/type.h"
#include <queue>
using namespace std;

//input file
//undirected graph: v \t num dst1 dst2 ...
//directed graph: v \t in_num in1 in2 ... out_num out1 out2 ...

//output file
//v subscript_id \t in_num in1 in2 ... out_num out1 out2 ... (subscript_id=0 for non-dummy vertex)
//here ini/outi are now (id, subscript_id) pairs

//Suppose v=5 overflows, then v's new ID is (VID, subscript_id)=(5, 0)
//newly generated dummy vertices have new ID (5, 1), (5, 2), ...

//I: (VID, NewID)
struct InOverflowValue_pregel {
    vector<intpair> in_edges;
    vector<intpair> out_edges;
};

ibinstream& operator<<(ibinstream& m, const InOverflowValue_pregel& v)
{
    m << v.in_edges;
    m << v.out_edges;
    return m;
}

obinstream& operator>>(obinstream& m, InOverflowValue_pregel& v)
{
    m >> v.in_edges;
    m >> v.out_edges;
    return m;
}

//====================================
//msg={{vid, 0}, {vid, sub}}, we record <vid, sub>
class InOverflowVertex_pregel : public Vertex<intpair, InOverflowValue_pregel,
                                              intpair, IntPairHash> {
public:
    static int th;

    static void setDegThreshold(int threshold)
    {
        th = threshold;
    }

    InOverflowVertex_pregel* getV(int id, int nid)
    {
        InOverflowVertex_pregel* v = new InOverflowVertex_pregel();
        v->id.v1 = id;
        v->id.v2 = nid;
        return v;
    }

    void addVertices(vector<InOverflowVertex_pregel*> list)
    {
        for (int i = 0; i < list.size(); i++)
            add_vertex(list[i]);
    }

    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1) {
            vector<intpair>& in_edges = value().in_edges;
            if (in_edges.size() <= th)
                return; // no overflow

            int vid = id.v1;
            int nxtNewID = 1;

            // add bottom level dummy vertex
            queue<InOverflowVertex_pregel*> q;
            vector<InOverflowVertex_pregel*> vertices; // for adding

            for (int i = 0; i < in_edges.size(); i += th) {
                InOverflowVertex_pregel* v = getV(vid, nxtNewID++);
                vertices.push_back(v);
                for (int j = i; j < min(i + th, (int)in_edges.size()); j++) {
                    v->value().in_edges.push_back(in_edges[j]);
                    intpair msg;
                    msg.v1 = vid;
                    msg.v2 = v->id.v2;
                    send_message(value().in_edges[j], msg);
                }
                q.push(v);
            }
            //clear in_egdes
            in_edges.clear();

            // start bfs, maybe need to send msgs
            while (!q.empty()) {
                if (q.size() <= th) // top level
                {
                    while (!q.empty()) {
                        InOverflowVertex_pregel* cur = q.front();
                        q.pop();
                        cur->value().out_edges.push_back(id);
                        in_edges.push_back(cur->id);
                    }
                } else {
                    InOverflowVertex_pregel* v = getV(vid, nxtNewID++);
                    vertices.push_back(v);
                    for (int i = 0; i < th; i++) {
                        InOverflowVertex_pregel* cur = q.front();
                        q.pop();
                        cur->value().out_edges.push_back(v->id);
                        v->value().in_edges.push_back(cur->id);
                    }
                }
            }
            addVertices(vertices);
        } else {
            vector<intpair>& out_edges = value().out_edges;
            for (int i = 0; i < messages.size(); i++) {
                intpair msg = messages[i];
                int dstID = msg.v1;
                int newID_sub = msg.v2;
                for (int j = 0; j < out_edges.size(); j++) {
                    intpair& dst = out_edges[j];
                    if (dst.v1 == dstID)
                        dst.v2 = newID_sub;
                }
            }
        }
        vote_to_halt();
    }
};

int InOverflowVertex_pregel::th = 100;

class InOverflowWorker_pregel : public Worker<InOverflowVertex_pregel> {
    char buf[100];
    bool directed;

public:
    void setDirected(bool tag)
    {
        directed = tag;
    }

    //C version
    virtual InOverflowVertex_pregel* toVertex(char* line)
    {

        if (directed) {
            //directed graph: v \t in_num in1 in2 ... out_num out1 out2 ...
            char* pch;
            pch = strtok(line, "\t");
            InOverflowVertex_pregel* v = new InOverflowVertex_pregel;
            v->id.v1 = atoi(pch);
            v->id.v2 = 0;
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
        } else {
            //undirected graph: v \t num dst1 dst2 ...
            //cout << line << endl;
            char* pch;
            pch = strtok(line, "\t");
            InOverflowVertex_pregel* v = new InOverflowVertex_pregel;
            v->id.v1 = atoi(pch);
            v->id.v2 = 0;
            pch = strtok(NULL, " ");
            int num = atoi(pch);
            //cout << num << endl;
            for (int i = 0; i < num; i++) {
                pch = strtok(NULL, " ");
                intpair nb(atoi(pch), 0);
                v->value().in_edges.push_back(nb);
                v->value().out_edges.push_back(nb);
            }
            return v;
        }
    }

    virtual void toline(InOverflowVertex_pregel* v, BufferedWriter& writer)
    {
        vector<intpair>& in_edges = v->value().in_edges;
        sprintf(buf, "%d %d\t%d ", v->id.v1, v->id.v2, in_edges.size());
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

void pregel_inoverflow(string in_path, string out_path, int threshold,
                       bool directed)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    InOverflowWorker_pregel worker;
    InOverflowVertex_pregel::setDegThreshold(threshold);
    worker.setDirected(directed);
    worker.run(param);
}
