#include "basic/pregel-dev.h"
#include <cmath>
using namespace std;

struct BipartiteMatchingValue {
    int left;
    int matchTo;
    std::vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const BipartiteMatchingValue& v)
{
    m << v.left;
    m << v.matchTo;
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, BipartiteMatchingValue& v)
{
    m >> v.left;
    m >> v.matchTo;
    m >> v.edges;
    return m;
}

//====================================

class BipartiteMatchingVertex : public Vertex<VertexID, BipartiteMatchingValue, int> {
public:
    virtual void compute(MessageContainer& messages)
    {
        std::vector<VertexID>& edges = value().edges;
        if (step_num() % 4 == 1) {
            if (value().left == 1 && value().matchTo == -1) // left not matched
            {
                for (int i = 0; i < edges.size(); i++) {
                    send_message(edges[i], id); // request
                }
            }
            vote_to_halt();
        } else if (step_num() % 4 == 2) {
            if (value().left == 0 && value().matchTo == -1) //right  not matched
            {
                if (messages.size() > 0) {
                    send_message(messages[0], id); // ask for granting
                }
            }
            vote_to_halt();
        } else if (step_num() % 4 == 3) {
            if (value().left == 1 && value().matchTo == -1) // left not matched
            {
                if (messages.size() > 0) {
                    value().matchTo = messages[0];
                    send_message(messages[0], id); // grant
                }
            }
            vote_to_halt();
        } else if (step_num() % 4 == 0) {
            if (value().left == 0 && value().matchTo == -1) //right  not matched
            {
                if (messages.size() == 1) {
                    value().matchTo = messages[0]; // update
                }
            }
            vote_to_halt();
        }
    }
};

class BipartiteMatchingWorker : public Worker<BipartiteMatchingVertex> {
    char buf[100];

public:
    // vid \t left=1 num v1 v2
    virtual BipartiteMatchingVertex* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        BipartiteMatchingVertex* v = new BipartiteMatchingVertex;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        v->value().left = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            v->value().edges.push_back(atoi(pch));
        }
        v->value().matchTo = -1;

        return v;
    }

    virtual void toline(BipartiteMatchingVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d\n", v->id, v->value().matchTo);
        writer.write(buf);
    }
};

void pregel_bipartitematching(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    BipartiteMatchingWorker worker;
    worker.run(param);
}
