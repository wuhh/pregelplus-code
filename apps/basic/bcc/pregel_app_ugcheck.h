#include "basic/pregel-dev.h"
using namespace std;

struct UGValue_ppa {
    vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const UGValue_ppa& v)
{
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, UGValue_ppa& v)
{
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

class UGVertex_ppa : public Vertex<VertexID, UGValue_ppa, VertexID> {
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
            messages.erase(unique(messages.begin(), messages.end()), messages.end());
            if (value().edges != messages) {
                cout << "I am : " << id;
                cout << " Out neighbor: ";
                print_vector(value().edges);
                cout << " In neighbor: ";
                print_vector(messages);
                cout << endl;
            }
            value().edges.swap(messages);
            vote_to_halt();
        }
    }
};

class UGWorker_ppa : public Worker<UGVertex_ppa> {
    char buf[100];

public:
    //C version
    virtual UGVertex_ppa* toVertex(char* line)
    {

        char* pch;
        pch = strtok(line, "\t");
        UGVertex_ppa* v = new UGVertex_ppa;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            v->value().edges.push_back(atoi(pch));
        }

        return v;
    }

    virtual void toline(UGVertex_ppa* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d", v->id, v->value().edges.size());
        writer.write(buf);
        vector<int>& nbs = v->value().edges;
        for (int i = 0; i < nbs.size(); i++) {
            sprintf(buf, " %d", nbs[i]);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void ppa_ugcheck(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    UGWorker_ppa worker;
    worker.run(param);
}
