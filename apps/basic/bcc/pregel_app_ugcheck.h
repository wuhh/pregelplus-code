#include "basic/pregel-dev.h"
using namespace std;

struct CCValue_ppa {
    int color;
    vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const CCValue_ppa& v)
{
    m << v.color;
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, CCValue_ppa& v)
{
    m >> v.color;
    m >> v.edges;
    return m;
}

//====================================
template <class T>
void print_vector(const T& v)
{
    for (int i = 0; i < v.size(); i++) {
        cout << v[i] << " ";
    }
    //cout << endl;
}

class CCVertex_ppa : public Vertex<VertexID, CCValue_ppa, VertexID> {
public:
    void broadcast(VertexID msg)
    {
        vector<VertexID>& nbs = value().edges;
        for (int i = 0; i < nbs.size(); i++) {
            send_message(nbs[i], msg);
        }
    }

    virtual void compute(MessageContainer& messages)
    {
        vote_to_halt();
        return;
        if (step_num() == 1) {
            broadcast(id);
            vote_to_halt();
        } else {
            sort(value().edges.begin(), value().edges.end());
            sort(messages.begin(), messages.end());
            if (value().edges != messages) {
                cout << "I am : " << id;
                cout << " Out neighbor: ";
                print_vector(value().edges);
                cout << " In neighbor: ";
                print_vector(messages);
                cout << endl;
            }
            vote_to_halt();
        }
    }
};

class CCWorker_ppa : public Worker<CCVertex_ppa> {
    char buf[100];

public:
    //C version
    virtual CCVertex_ppa* toVertex(char* line)
    {

        char* pch;
        pch = strtok(line, "\t");
        CCVertex_ppa* v = new CCVertex_ppa;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            v->value().edges.push_back(atoi(pch));
        }

        return v;
    }

    virtual void toline(CCVertex_ppa* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d ", v->id, v->value().color);
        writer.write(buf);
        vector<int>& nbs = v->value().edges;
        for (int i = 0; i < nbs.size(); i++) {
            sprintf(buf, "%d ", nbs[i]);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

class CCCombiner_ppa : public Combiner<VertexID> {
public:
    virtual void combine(VertexID& old, const VertexID& new_msg)
    {
        if (old > new_msg)
            old = new_msg;
    }
};

void ppa_hashmin(string in_path, string out_path, bool use_combiner)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    CCWorker_ppa worker;
    CCCombiner_ppa combiner;
    if (use_combiner)
        worker.setCombiner(&combiner);
    worker.run(param);
}
