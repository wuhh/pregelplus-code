#include "basic/pregel-dev.h"
#include "utils/type.h"
#include <sstream>
#include <algorithm>
#include <cassert>
using namespace std;
const int inf = 1000000000;
struct kcoreValue {
    vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const kcoreValue& v)
{
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, kcoreValue& v)
{
    m >> v.edges;
    return m;
}

class kcoreVertex : public Vertex<VertexID, kcoreValue, intpair> {
public:
    int phi;
    hash_map<int,int> P;
    int subfunc(kcoreVertex* v)
    {
        vector<VertexID>& edges = v->value().edges;

        vector<int> cd(phi + 2, 0);
        for (int i = 0; i < edges.size(); i++) {

            if (P[edges[i]] > phi)
                P[edges[i]] = phi;
            cd[P[edges[i]]]++;
        }
        for (int i = phi; i >= 1; i--) {
            cd[i] += cd[i + 1];
            if (cd[i] >= i)
                return i;
        }
        assert(0);
    }
    virtual void compute(MessageContainer& messages)
    {
        vector<VertexID>& edges = value().edges;
        if (step_num() == 1) {
            phi = edges.size();
            for (int i = 0; i < edges.size(); i++) {
                send_message(edges[i], intpair(id, phi));
                P[ edges[i] ] = inf;
            }
        } else {
            for (int i = 0; i < messages.size(); i++) {
                int u = messages[i].v1;
                int k = messages[i].v2;
                if(P[u] > k)
                {
                    P[u] = k;
                }
            }

            int x = subfunc(this); //

            if (x < phi) {
                phi = x;
                for (int i = 0; i < edges.size(); i++) {
                    if (phi < P[i]) {
                        send_message(edges[i], intpair(id, phi));
                    }
                }
            }
        }
        vote_to_halt();
    }
};

class kcoreWorker : public Worker<kcoreVertex> {
    char buf[100];

public:
    //C version
    virtual kcoreVertex* toVertex(char* line)
    {
        kcoreVertex* v = new kcoreVertex;
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

    virtual void toline(kcoreVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d\n", v->id, v->phi);
        writer.write(buf);
    }
};

void pregel_kcore(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    kcoreWorker worker;

    worker.run(param);
}
