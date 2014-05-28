#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

struct UGTODGValue_pregel
{
    vector<intpair> edges;
};

ibinstream& operator<<(ibinstream& m, const UGTODGValue_pregel& v)
{
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, UGTODGValue_pregel& v)
{
    m >> v.edges;
    return m;
}

//====================================

class UGTODGVertex_pregel : public Vertex<VertexID, UGTODGValue_pregel, VertexID>
{
public:
    virtual void compute(MessageContainer& messages)
    {
    	vector<intpair>& nbs = value().edges;
        if (step_num() == 1)
        {

            for (int i = 0; i < nbs.size(); i++)
            {
                send_message(nbs[i].v1, id);
            }
            vote_to_halt();
        }
        else
        {
            for (int i = 0; i < messages.size(); i++)
            {
            	nbs.push_back(intpair(messages[i],0));
            }
            vote_to_halt();
        }
    }
};

class UGTODGWorker_pregel : public Worker<UGTODGVertex_pregel>
{
    char buf[100];

public:
    //C version
    virtual UGTODGVertex_pregel* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        UGTODGVertex_pregel* v = new UGTODGVertex_pregel;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++)
        {
            pch = strtok(NULL, " ");
            v->value().edges.push_back(intpair(atoi(pch),1));
        }
        return v;
    }

    virtual void toline(UGTODGVertex_pregel* v, BufferedWriter& writer)
    {
    	vector<intpair>& nbs = v->value().edges;
        sprintf(buf, "%d\t%d", v->id, nbs.size());
        writer.write(buf);

        for(int i = 0; i < nbs.size(); i ++)
        {
        	sprintf(buf, " %d %d", nbs[i].v1, nbs[i].v2);
        	writer.write(buf);
        }

        writer.write("\n");
    }
};


void pregel_UGTODG(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    UGTODGWorker_pregel worker;
    worker.run(param);
}
