#include "basic/pregel-dev.h"
using namespace std;

struct CCValue_pregel {
    vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const CCValue_pregel& v)
{
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, CCValue_pregel& v)
{
    m >> v.edges;
    return m;
}

//====================================

class CCVertex_pregel : public Vertex<VertexID, CCValue_pregel, VertexID> {
public:

    virtual void compute(MessageContainer& messages)
    {
    	vote_to_halt();
    }
};

class CCWorker_pregel : public Worker<CCVertex_pregel> {

public:
    //C version
    virtual CCVertex_pregel* toVertex(char* line)
    {
    }

    virtual void toline(CCVertex_pregel* v, BufferedWriter& writer)
    {
    }
};


void pregel_hashmin(string in_path, string out_path, bool use_combiner)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    CCWorker_pregel worker;
    worker.run(param);
}
