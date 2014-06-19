#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

struct DGTOLDGValue_vworker {
    vector<vwpair> inedges;
    vector<vwpair> outedges;
};

ibinstream& operator<<(ibinstream& m, const DGTOLDGValue_vworker& v)
{
    m << v.inedges;
    m << v.outedges;
    return m;
}

obinstream& operator>>(obinstream& m, DGTOLDGValue_vworker& v)
{
    m >> v.inedges;
    m >> v.outedges;
    return m;
}

//====================================

class DGTOLDGVertex_vworker : public Vertex<vwpair, DGTOLDGValue_vworker, intpair, VWPairHash> {
public:
    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1) {
            vector<vwpair>& nbs = value().outedges;
            for (int i = 0; i < nbs.size(); i++) {
                send_message(nbs[i], intpair(id.vid, id.wid));
            }
            vote_to_halt();
        } else {
            vector<vwpair>& nbs = value().inedges;
            for (int i = 0; i < messages.size(); i++) {
                nbs.push_back(vwpair(messages[i].v1, messages[i].v2));
            }
            vote_to_halt();
        }
    }
};

class DGTOLDGWorker_vworker : public Worker<DGTOLDGVertex_vworker> {
    char buf[100];

public:
    //C version
    virtual DGTOLDGVertex_vworker* toVertex(char* line)
    {
        char* pch;
        DGTOLDGVertex_vworker* v = new DGTOLDGVertex_vworker;
        pch = strtok(line, " ");
        v->id.vid = atoi(pch);
        pch = strtok(NULL, "\t");
        v->id.wid = atoi(pch);
        while (pch = strtok(NULL, " ")) {
            int vid = atoi(pch);
            pch = strtok(NULL, " ");
            int wid = atoi(pch);
            v->value().outedges.push_back(vwpair(vid, wid));
        }
        return v;
    }

    virtual void toline(DGTOLDGVertex_vworker* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d %d\t%d", v->id.vid, v->id.wid, v->value().inedges.size() + v->value().outedges.size());
        writer.write(buf);

        for (int i = 0; i < v->value().inedges.size(); i++) {
            sprintf(buf, " %d %d %d", v->value().inedges[i].vid, v->value().inedges[i].wid, 0);
            writer.write(buf);
        }
        for (int i = 0; i < v->value().outedges.size(); i++) {
            sprintf(buf, " %d %d %d", v->value().outedges[i].vid, v->value().outedges[i].wid, 1);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void vworker_DGTOLDG(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    DGTOLDGWorker_vworker worker;
    worker.run(param);
}
