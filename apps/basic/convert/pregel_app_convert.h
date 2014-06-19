#include "basic/pregel-dev.h"
using namespace std;

struct ConvertValue {
    vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const ConvertValue& v)
{
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, ConvertValue& v)
{
    m >> v.edges;
    return m;
}

//====================================

class ConvertVertex : public Vertex<VertexID, ConvertValue, VertexID> {
public:
    virtual void compute(MessageContainer& messages)
    {
        vote_to_halt();
    }
};

class ConvertWorker : public Worker<ConvertVertex> {
    char buf[100];

public:
    //C version
    virtual ConvertVertex* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        ConvertVertex* v = new ConvertVertex;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            v->value().edges.push_back(atoi(pch));
        }
        return v;
    }

    virtual void toline(ConvertVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d", v->id, v->value().edges.size());
        writer.write(buf);
        for (int i = 0; i < v->value().edges.size(); i++) {
            sprintf(buf, " %d 1", v->value().edges[i]);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void pregel_convert(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    ConvertWorker worker;
    worker.run(param);
}
