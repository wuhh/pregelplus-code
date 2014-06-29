#include "basic/pregel-dev.h"
#include "utils/type.h"
#include <sstream>
#include <algorithm>
#include <cassert>
using namespace std;
const int inf = 1000000000;
struct kcoreT1Value {
    vector<intpair> K; // K, T
    vector<intpair> edges; // vid, no of edges
    vector<intpair> static_edges; // vid, no of edges
};

ibinstream& operator<<(ibinstream& m, const kcoreT1Value& v)
{
    m << v.K;
    m << v.edges;
    m << v.static_edges;
    return m;
}

obinstream& operator>>(obinstream& m, kcoreT1Value& v)
{
    m >> v.K;
    m >> v.edges;
    m >> v.static_edges;
    return m;
}
bool intpaircmp(const intpair& p1, const intpair& p2)
{
    return p1.v2 > p2.v2; //id
}
class kcoreT1Vertex : public Vertex<VertexID, kcoreT1Value, int> {
public:
    int currentT;
    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1) {
            return;
        } else if (step_num() == 2) {
            currentT = *((int*)getAgg());
            while (phase_num() > 1 && value().static_edges.size() > 0 && value().static_edges.back().v2 == currentT)
                value().static_edges.pop_back();
            value().edges = value().static_edges;
            return;
        } else if (step_num() == 3) {
            currentT = *((int*)getAgg());
        }
        vector<intpair> newEdges;
        vector<intpair>& edges = value().edges;
        hash_set<VertexID> st;
        for (int i = 0; i < messages.size(); i++) {
            st.insert(messages[i]);
        }
        for (int i = 0; i < edges.size(); i++) {
            if (st.count(edges[i].v1) == 0)
                newEdges.push_back(edges[i]);
        }
        edges.swap(newEdges);

        /* calculate k-core */

        int globalK = step_num() == 3 ? 1 : *((int*)getAgg());
        if (edges.size() < globalK) {
            for (int i = 0; i < edges.size(); i++) {
                send_message(edges[i].v1, id);
            }
            if (value().K.size() == 0) {
                value().K.push_back(intpair(globalK - 1, currentT));
            } else {
                if (globalK - 1 == value().K.back().v1)
                    value().K.back().v2 = currentT;
                else
                    value().K.push_back(intpair(globalK - 1, currentT));
            }
            value().edges.clear();
            vote_to_halt();
        }
    }
};
class kcoreT1Agg : public Aggregator<kcoreT1Vertex, int, int> {
private:
    int K;
    int currentT;
    int allLessK;

public:
    virtual void init()
    {
        if (step_num() <= 3) {
            currentT = inf;
            K = 1;
        } else {
            K = *((int*)getAgg());
        }
        allLessK = true;
    }

    virtual void stepPartial(kcoreT1Vertex* v)
    {
        if (step_num() <= 2) {
            if (v->value().static_edges.size() != 0) {
                currentT = min(currentT, v->value().static_edges.back().v2);
            }
        } else {
            if (v->value().edges.size() < K)
                allLessK = false;
        }
    }

    virtual void stepFinal(int* part)
    {
        if (step_num() <= 2) {
            currentT = min(currentT, *part);
        } else {
            allLessK = *part && allLessK;
        }
    }

    virtual int* finishPartial()
    {
        if (step_num() <= 2) {
            return &currentT;
        } else {
            return &allLessK;
        }
    }
    virtual int* finishFinal()
    {
        if (step_num() == 1) {
            return &currentT;
        } else if (step_num() == 2) {
            if (_my_rank == 0)
                cout << "Current T: " << currentT << endl;
            if (currentT == inf)
                forceTerminate();
            return &currentT;
        } else {
            if (allLessK)
                K += 1;
            if (_my_rank == 0)
                cout << "Core number: " << K << endl;
            return &K;
        }
    }
};
class kcoreT1Worker : public Worker<kcoreT1Vertex, kcoreT1Agg> {
    char buf[100];

public:
    //C version
    virtual kcoreT1Vertex* toVertex(char* line)
    {
        kcoreT1Vertex* v = new kcoreT1Vertex;
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
            v->value().static_edges.push_back(intpair(nb, m));
        }
        sort(v->value().static_edges.begin(), v->value().static_edges.end(), intpaircmp);
        return v;
    }

    virtual void toline(kcoreT1Vertex* v, BufferedWriter& writer)
    {
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

void pregel_kcoreT1(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    kcoreT1Worker worker;
    kcoreT1Agg agg;
    worker.setAggregator(&agg);
    worker.run(param, inf);
}
