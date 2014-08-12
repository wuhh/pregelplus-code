#include "basic/pregel-dev.h"
#include "utils/type.h"
#include <sstream>
#include <algorithm>
#include <cassert>
using namespace std;
const int inf = 1000000000;
struct kcoreValue {
    vector<intpair> K; // K, T
    vector<intpair> edges; // vid, no of edges
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
bool intpaircmp(const intpair& p1, const intpair& p2)
{
    return p1.v2 > p2.v2; //id
}
class kcoreVertex : public Vertex<VertexID, kcoreValue, intpair> {
public:
    int currentT;

    int subfunc(kcoreVertex* v)
    {
        vector<intpair>& edges = v->value().edges;
        vector<int>& p = v->value().p;
        int K = v->value().K.back().v1;
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
        vector<intpair>& Kvec = value().K;
        vector<intpair>& edges = value().edges;
        vector<int>& p = value().p;

        if (step_num() == 1) {
            if (Kvec.size() >= 2) {
                int lastone = Kvec.size() - 1, lasttwo = Kvec.size() - 2;
                if (Kvec[lastone].v1 == Kvec[lasttwo].v1) {
                    swap(Kvec[lastone], Kvec[lasttwo]);
                    Kvec.pop_back();
                }
            }
            sort(value().edges.begin(), value().edges.end(), intpaircmp);
            return;
        } else if (step_num() == 2) {
            currentT = *((int*)getAgg());
            while (phase_num() > 1 && value().edges.size() > 0 && value().edges.back().v2 == currentT)
                value().edges.pop_back();
            return;
        } else if (step_num() == 3) {
            currentT = *((int*)getAgg());
        }

        if (step_num() == 3) {
            if (Kvec.size() == 0)
                Kvec.push_back(intpair(edges.size(), currentT));
            else
                Kvec.push_back(intpair(min((int)edges.size(), Kvec.back().v1), currentT));
            int K = Kvec.back().v1;

            sort(edges.begin(), edges.end());
            p = vector<int>(edges.size(), inf);
            for (int i = 0; i < edges.size(); i++) {
                send_message(edges[i].v1, intpair(id, K));
            }
        } else {
            int K = Kvec.back().v1;
            sort(messages.begin(), messages.end());
            // To be consistent with edges list;
            for (int i = 0, j = 0; i < messages.size(); i++) {
                while (edges[j].v1 < messages[i].v1)
                    j++;
                if (messages[i].v2 < p[j])
                    p[j] = messages[i].v2;
            }

            int x = subfunc(this); //

            if (x < K) {
                K = x;
                for (int i = 0; i < edges.size(); i++) {
                    if (K < p[i]) {
                        send_message(edges[i].v1, intpair(id, K));
                    }
                }

                Kvec.back().v1 = K;
            }
        }
        vote_to_halt();
    }
};
class kcoreAgg : public Aggregator<kcoreVertex, int, int> {
private:
    int currentT;

public:
    virtual void init()
    {
        currentT = inf;
    }

    virtual void stepPartial(kcoreVertex* v)
    {
        if (v->value().edges.size() != 0) {
            currentT = min(currentT, v->value().edges.back().v2);
        }
    }

    virtual void stepFinal(int* part)
    {
        currentT = min(currentT, *part);
    }

    virtual int* finishPartial()
    {
        return &currentT;
    }
    virtual int* finishFinal()
    {
        if (currentT == inf)
            forceTerminate();
        return &currentT;
    }
};
class kcoreWorker : public Worker<kcoreVertex, kcoreAgg> {
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
            int nb, m;
            ssin >> nb >> m;
            for (int j = 0; j < m; j++) {
                int t;
                ssin >> t;
            }
            v->value().edges.push_back(intpair(nb, m));
        }

        return v;
    }

    virtual void toline(kcoreVertex* v, BufferedWriter& writer)
    {
        vector<intpair>& Kvec = v->value().K;
        if (Kvec.size() >= 2) {
            int lastone = Kvec.size() - 1, lasttwo = Kvec.size() - 2;
            if (Kvec[lastone].v1 == Kvec[lasttwo].v1) {
                swap(Kvec[lastone], Kvec[lasttwo]);
                Kvec.pop_back();
            }
        }
        sprintf(buf, "%d", v->id);
        writer.write(buf);
        for (int i = 0; i < v->value().K.size(); i++) {
            if (v->value().K[i].v1 != 0) {
                sprintf(buf, "%s%d %d", i == 0 ? "\t" : " ", v->value().K[i].v1, v->value().K[i].v2);
                writer.write(buf);
            }
        }
        writer.write("\n");
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
    kcoreAgg agg;
    worker.setAggregator(&agg);
    worker.run(param, inf);
}
