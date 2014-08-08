#include "basic/pregel-dev.h"
#include "utils/type.h"
#include "scc_config.h"
#include <map>
using namespace std;

struct SetPair {
    int color;
    set<int> minForward;
    set<int> minBackward;

    SetPair()
    {
    }
    SetPair(int c, const set<int>& f, const set<int>& b)
    {
        color = c;
        minForward = f;
        minBackward = b;
    }

    static bool intersected(const set<int>& h1, const set<int>& h2)
    {
        for (set<int>::iterator it = h1.begin(); it != h1.end(); it++) {
            if (h2.count(*it)) {
                return true;
            }
        }
        return false;
    }

    bool isSCC() const
    {
        return intersected(minForward, minBackward);
    }
    inline bool operator==(const SetPair& rhs) const
    {
        return color == rhs.color && this->minForward == rhs.minForward && this->minBackward == rhs.minBackward;
    }
    bool operator<(const SetPair& rhs) const
    {
        if (this->color == rhs.color) {
            if (this->minForward == rhs.minForward)
                return this->minBackward < rhs.minBackward;
            else
                return this->minForward < rhs.minForward;
        } else {
            return this->color < rhs.color;
        }
    }
};

ibinstream& operator<<(ibinstream& m, const SetPair& v)
{
    m << v.color;
    m << v.minForward;
    m << v.minBackward;
    return m;
}

obinstream& operator>>(obinstream& m, SetPair& v)
{
    m >> v.color;
    m >> v.minForward;
    m >> v.minBackward;
    return m;
}

struct MultiGDAggValue_scc {
    int nxtColor;
    map<SetPair, int> cntMap;
    map<SetPair, int> colorMap;
};
ibinstream& operator<<(ibinstream& m, const MultiGDAggValue_scc& v)
{

    m << v.nxtColor;
    m << v.cntMap;
    m << v.colorMap;
    return m;
}

obinstream& operator>>(obinstream& m, MultiGDAggValue_scc& v)
{

    m >> v.nxtColor;
    m >> v.cntMap;
    m >> v.colorMap;
    return m;
}

struct MultiGDecomValue_scc {
    int color;
    //color=-1, means trivial SCC
    //color=-2, means not assigned
    //otherwise, color>=0
    int sccTag;
    set<VertexID> minForward;
    set<VertexID> minBackward;
    vector<VertexID> in_edges;
    vector<VertexID> out_edges;
};

ibinstream& operator<<(ibinstream& m, const MultiGDecomValue_scc& v)
{
    m << v.color;
    m << v.sccTag;
    m << v.minForward;
    m << v.minBackward;
    m << v.in_edges;
    m << v.out_edges;
    return m;
}

obinstream& operator>>(obinstream& m, MultiGDecomValue_scc& v)
{
    m >> v.color;
    m >> v.sccTag;
    m >> v.minForward;
    m >> v.minBackward;
    m >> v.in_edges;
    m >> v.out_edges;
    return m;
}

//====================================

class MultiGDecomVertex_scc : public Vertex<VertexID, MultiGDecomValue_scc,
                                            intpair> {
public:
    void bcast_to_in_nbs(intpair msg)
    {
        vector<VertexID>& nbs = value().in_edges;
        for (int i = 0; i < nbs.size(); i++) {
            send_message(nbs[i], msg);
        }
    }

    void bcast_to_out_nbs(intpair msg)
    {
        vector<VertexID>& nbs = value().out_edges;
        for (int i = 0; i < nbs.size(); i++) {
            send_message(nbs[i], msg);
        }
    }

    void bcast_to_all_nbs(intpair msg)
    {
        bcast_to_in_nbs(msg);
        bcast_to_out_nbs(msg);
    }

    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1) {
            if (id != -1) {
                if (value().sccTag != 0) {
                    vote_to_halt();
                }
            }
        } else if (step_num() == 2) {
            MultiGDAggValue_scc* agg = (MultiGDAggValue_scc*)getAgg();

            if (id == -1) {

                value().color = agg->nxtColor; //@@@@@@@@@@@@@@@@@@@
            } else if (value().sccTag == 0) {
                SetPair pair = SetPair(value().color, value().minForward, value().minBackward);
                if (SetPair::intersected(value().minForward, value().minBackward)) {
                    value().sccTag = 1;
                } else {
                    int cnt = agg->cntMap[pair];
                    if (cnt <= SCC_THRESHOLD) {
                        value().sccTag = -1;
                    }
                }
                int newColor = agg->colorMap[pair];
                value().color = newColor;
                bcast_to_all_nbs(intpair(id, newColor));
            }
        } else {
            map<int, int> map;
            for (int i = 0; i < messages.size(); i++) {
                intpair& message = messages[i];
                map[message.v1] = message.v2;
            }
            vector<VertexID>& in_edges = value().in_edges;
            vector<VertexID> in_new;
            for (int i = 0; i < in_edges.size(); i++) {
                int nbColor = map[in_edges[i]];
                if (nbColor == value().color)
                    in_new.push_back(in_edges[i]);
            }
            in_edges.swap(in_new);
            vector<VertexID>& out_edges = value().out_edges;
            vector<VertexID> out_new;
            for (int i = 0; i < out_edges.size(); i++) {
                int nbColor = map[out_edges[i]];
                if (nbColor == value().color)
                    out_new.push_back(out_edges[i]);
            }
            out_edges.swap(out_new);
            vote_to_halt();
        }
    }
};

class MultiGDAgg_scc : public Aggregator<MultiGDecomVertex_scc,
                                         MultiGDAggValue_scc, MultiGDAggValue_scc> {
private:
    MultiGDAggValue_scc state;

public:
    virtual void init()
    {
        state.nxtColor = -1;
    }

    virtual void stepPartial(MultiGDecomVertex_scc* v)
    {
        const MultiGDecomValue_scc& val = v->value();
        if (step_num() == 1) {
            if (v->id == -1) {
                state.nxtColor = v->value().color;

            } else if (val.sccTag == 0) {
                SetPair pair = SetPair(val.color, val.minForward, val.minBackward);
                if (state.cntMap.count(pair) == 0) {
                    state.cntMap[pair] = 1;

                } else {
                    state.cntMap[pair] += 1;
                }
            }
        } else if (step_num() == 3) {
            if (v->id == -1) {
                state.nxtColor = val.color;
            }
        }
    }

    virtual void stepFinal(MultiGDAggValue_scc* part)
    {
        if (step_num() == 1) {
            if (part->nxtColor != -1) {
                state.nxtColor = part->nxtColor;
            }
            for (map<SetPair, int>::iterator it = part->cntMap.begin();
                 it != part->cntMap.end();
                 it++) {
                const SetPair& key = it->first;
                int value = it->second;
                if (state.cntMap.count(key) == 0) {

                    state.cntMap[key] = value;
                } else {
                    state.cntMap[key] += value;
                }
            }
        }
    }

    virtual MultiGDAggValue_scc* finishPartial()
    {
        return &state;
    }
    virtual MultiGDAggValue_scc* finishFinal()
    {
        if (step_num() == 1) {
            int max = -1;
            int nxtColor = state.nxtColor;

            for (map<SetPair, int>::iterator it = state.cntMap.begin(); it != state.cntMap.end(); it++) {
                const SetPair& key = it->first;
                int cnt = it->second;

                if (!key.isSCC() && cnt > SCC_THRESHOLD && cnt > max) {
                    max = cnt;
                    //state.maxPair = key;
                }
                state.colorMap[key] = nxtColor;
                nxtColor++;
            }
            state.nxtColor = nxtColor;
            cout << "%%%%%%%%%%%%%% Max Subgraph Size = " << max << " %%%%%%%%%%%%" << endl;
        }
        return &state;
    }
};

//====================================

class MultiGDecomWorker_scc : public Worker<MultiGDecomVertex_scc,
                                            MultiGDAgg_scc> {
    char buf[100];

public:
    //C version
    virtual MultiGDecomVertex_scc* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        MultiGDecomVertex_scc* v = new MultiGDecomVertex_scc;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        v->value().color = atoi(pch);
        if (v->id == -1)
            return v;
        pch = strtok(NULL, " ");
        v->value().sccTag = atoi(pch);

        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            v->value().minForward.insert(atoi(pch));
        }

        pch = strtok(NULL, " ");
        num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            v->value().minBackward.insert(atoi(pch));
        }

        pch = strtok(NULL, " ");
        num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            v->value().in_edges.push_back(atoi(pch));
        }

        pch = strtok(NULL, " ");
        num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            v->value().out_edges.push_back(atoi(pch));
        }
        return v;
    }

    virtual void toline(MultiGDecomVertex_scc* v, BufferedWriter& writer)
    {
        if (v->id == -1) {
            sprintf(buf, "-1\t%d\n", v->value().color);
            writer.write(buf);
            return;
        }
        vector<VertexID>& in_edges = v->value().in_edges;
        vector<VertexID>& out_edges = v->value().out_edges;

        sprintf(buf, "%d\t%d %d", v->id, v->value().color, v->value().sccTag);
        writer.write(buf);

        sprintf(buf, " %d", in_edges.size());
        writer.write(buf);

        for (int i = 0; i < in_edges.size(); i++) {
            sprintf(buf, " %d", in_edges[i]);
            writer.write(buf);
        }
        sprintf(buf, " %d", out_edges.size());
        writer.write(buf);
        for (int i = 0; i < out_edges.size(); i++) {
            sprintf(buf, " %d", out_edges[i]);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void scc_multiGDecom(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    MultiGDecomWorker_scc worker;
    MultiGDAgg_scc agg;
    worker.setAggregator(&agg);
    worker.run(param);
}
