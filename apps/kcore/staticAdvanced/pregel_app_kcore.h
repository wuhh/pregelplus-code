#include "basic/pregel-dev.h"
#include "utils/type.h"
#include <sstream>
#include <algorithm>
#include <cassert>
using namespace std;
const int inf = 1000000000;
struct kcoreValue {
    int K;
    vector<VertexID> edges;
    vector<int> p;
};

ibinstream& operator<<(ibinstream& m, const kcoreValue& v)
{
    m << v.K;
    m << v.edges;
    m << v.p;
    return m;
}

obinstream& operator>>(obinstream& m, kcoreValue& v)
{
    m >> v.K;
    m >> v.edges;
    m >> v.p;
    return m;
}

//====================================
bool intpaircmp(const intpair& p1, const intpair& p2)
{
    return p1.v1 < p2.v1; //id
}
class kcoreVertex : public Vertex<VertexID, kcoreValue, intpair> {
public:
    int subfunc(kcoreVertex* v)
    {
        vector<VertexID>& edges = v->value().edges;
        vector<int>& p = v->value().p;
        int& K = v->value().K;

        vector<int> cd(K + 2, 0);

        for (int i = 0; i < edges.size(); i++) {
            if (p[i] > K)
                p[i] = K;
            cd[p[i]]++;
        }
        for (int i = K; i >= 1; i--) {
            cd[i] += cd[i + 1];
            if (cd[i] >= i)
                return i;
        }
        assert(0);
    }
    virtual void compute(MessageContainer& messages)
    {
        vector<VertexID>& edges = value().edges;
        vector<int>& p = value().p;
        int& K = value().K;
        if (step_num() == 1) {
            sort(edges.begin(), edges.end());
            p = vector<int>(edges.size(), inf);
            for (int i = 0; i < edges.size(); i++) {
                send_message(edges[i], intpair(id, K));
            }
        } else {
            sort(messages.begin(), messages.end(), intpaircmp);
            // To be consistent with edges list;
            for (int i = 0, j = 0; i < messages.size(); i++) {
                while (edges[j] < messages[i].v1)
                    j++;
                if (messages[i].v2 < p[j])
                    p[j] = messages[i].v2;
            }

            int x = subfunc(this); //

            if (x < K) {
                K = x;
                for (int i = 0; i < edges.size(); i++) {
                    if (K < p[i]) {
                        send_message(edges[i], intpair(id, K));
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
        v->value().K = v->value().edges.size();
        return v;
    }

    virtual void toline(kcoreVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d\n", v->id, v->value().K);
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
