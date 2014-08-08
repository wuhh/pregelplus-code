#include "ghost/ghost-dev.h"
#include "utils/type.h"
using namespace std;

class CCVertex_vwghost : public GVertex<vwpair, VertexID, VertexID, DefaultGEdge<vwpair, VertexID>, VWPairHash> {
public:
    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1) {
            // Initialize in toVertex
            broadcast(value());
            vote_to_halt();
        } else {
            VertexID min = messages[0];
            for (int i = 1; i < messages.size(); i++) {
                if (min > messages[i])
                    min = messages[i];
            }
            if (min < value()) {
                value() = min;
                broadcast(min);
            }
            vote_to_halt();
        }
    }
};

class CCWorker_vwghost : public GWorker<CCVertex_vwghost> {
    char buf[100];

public:
    virtual CCVertex_vwghost* toVertex(char* line)
    {
        char* pch;
        CCVertex_vwghost* v = new CCVertex_vwghost;
        pch = strtok(line, " ");
        v->id.vid = atoi(pch);
        pch = strtok(NULL, "\t");
        v->id.wid = atoi(pch);
        EdgeContainer& edges = v->neighbors();
        VertexID min = v->id.vid;
        while (pch = strtok(NULL, " ")) {
            EdgeT edge;
            int eid = edge.id.vid = atoi(pch);
            pch = strtok(NULL, " ");
            edge.id.wid = atoi(pch);
            if (eid < min)
                min = eid;
            edges.push_back(edge);
        }
        v->value() = min;
        return v;
    }

    virtual void toline(CCVertex_vwghost* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d\n", v->id.vid, v->value());
        writer.write(buf);
    }
};

class CCCombiner_vwghost : public Combiner<VertexID> {
public:
    virtual void combine(VertexID& old, const VertexID& new_msg)
    {
        if (old > new_msg)
            old = new_msg;
    }
};

void vwghost_hashmin(string in_path, string out_path, bool use_combiner)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    CCWorker_vwghost worker;
    CCCombiner_vwghost combiner;
    if (use_combiner)
        worker.setCombiner(&combiner);
    worker.run(param);
}
