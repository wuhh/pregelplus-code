#include "basic/pregel-dev.h"
#include "scc_config.h"
#include <set>
using namespace std;

struct PoolSet
{
    void add(int id)
    {
        if (pool.size() < SCC_K)
        {
            pool.push_back(id);
        }
        else
        {
            double p = 1.0 * rand() / RAND_MAX;
            if (p <= SCC_REPLACE_RATE)
            {
                int pos = rand() % pool.size();
                pool[pos] = id;
            }
        }
    }
    bool contains(int id)
    {
        for (int i = 0; i < pool.size(); i++)
        {
            if (pool[i] == id)
                return true;
        }
        return false;
    }
    vector<int> pool;
};
ibinstream &
operator<<(ibinstream & m, const PoolSet & v)
{
    m << v.pool;
    return m;
}

obinstream &
operator>>(obinstream & m, PoolSet & v)
{
    m >> v.pool;
    return m;
}
struct MultiLabelValue_scc
{
    int color;
    //color=-1, means trivial SCC
    //color=-2, means not assigned
    //otherwise, color>=0
    int sccTag;
    hash_set<VertexID> minForward;
    hash_set<VertexID> minBackward;
    vector<VertexID> in_edges;
    vector<VertexID> out_edges;
};

ibinstream &
operator<<(ibinstream & m, const MultiLabelValue_scc & v)
{
    m << v.color;
    m << v.sccTag;
    m << v.minForward;
    m << v.minBackward;
    m << v.in_edges;
    m << v.out_edges;
    return m;
}

obinstream &
operator>>(obinstream & m, MultiLabelValue_scc & v)
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

class MultiLabelVertex_scc: public Vertex<VertexID, MultiLabelValue_scc,
            VertexID>
{
public:
    void bcast_to_in_nbs(VertexID msg)
    {
        vector<VertexID> & nbs = value().in_edges;
        for (int i = 0; i < nbs.size(); i++)
        {
            send_message(nbs[i], msg);
        }
    }

    void bcast_to_out_nbs(VertexID msg)
    {
        vector<VertexID> & nbs = value().out_edges;
        for (int i = 0; i < nbs.size(); i++)
        {
            send_message(nbs[i], msg);
        }
    }

    virtual void compute(MessageContainer & messages)
    {
        if (step_num() == 1)
        {
            // for Agg to sample
            return;
        }
        else if (step_num() == 2)
        {
            hash_map<int, PoolSet>* agg = (hash_map<int, PoolSet>*) getAgg();
            MultiLabelValue_scc& val = value();

            if (id != -1 & val.sccTag == 0)
            {
                int color = val.color;
                if ((*agg)[color].contains(id))
                {
                    val.minForward.insert(id);
                    val.minBackward.insert(id);
                    bcast_to_out_nbs(id);
                    bcast_to_in_nbs(-id - 1);
                }
            }
            vote_to_halt();
        }
        else
        {
            MultiLabelValue_scc& val = value();
            if (val.sccTag == 0)
            {
                for (int i = 0; i < messages.size(); i++)
                {
                    int cur = messages[i];
                    if (cur < 0)
                    {
                        cur = -cur - 1; //backward src
                        if (!val.minBackward.count(cur))
                        {
                            val.minBackward.insert(cur);
                            bcast_to_in_nbs(-cur - 1);
                        }
                    }
                    else
                    {
                        if (!val.minForward.count(cur))
                        {
                            val.minForward.insert(cur);
                            bcast_to_out_nbs(cur);
                        }
                    }
                }
            }
            vote_to_halt();
        }
    }
};

class MultiLabelAgg_scc: public Aggregator<MultiLabelVertex_scc,
            hash_map<int, PoolSet>, hash_map<int, PoolSet> >
{
private:
    hash_map<int, PoolSet> state;
public:
    virtual void init()
    {}

    virtual void stepPartial(MultiLabelVertex_scc* v)
    {
        if(step_num() == 1)
        {
            const MultiLabelValue_scc& val = v->value();

            if(v->id != -1 & val.sccTag == 0)
            {
                int color = val.color;
                if(state.count(color) == 0)
                {
                    state[color] = PoolSet();
                }
                state[color].add(v->id);
            }
        }
    }

    virtual void stepFinal(hash_map<int, PoolSet>* part)
    {
        if(step_num() == 1)
        {
            for(hash_map<int, PoolSet>::iterator it = part->
                    begin();
                    it !=part->end() ;
                    it ++)
            {
                int key = it->first;
                const PoolSet& value = it->second;

                if(state.count(key) == 0)
                {
                    state[key] = value;
                }
                else
                {
                    PoolSet& myValue = state[key];
                    for(int i =0 ;i < value.pool.size() ; i ++)
                    {
                        //myValue.add(value.pool[i]);
                    }
                }
            }
        }
    }

    virtual hash_map<int, PoolSet>* finishPartial()
    {
        return &state;
    }
    virtual hash_map<int, PoolSet>* finishFinal()
    {
        return &state;
    }

};

class MultiLabelWorker_scc: public Worker<MultiLabelVertex_scc,
            MultiLabelAgg_scc>
{
    char buf[100];

public:
    //C version
    virtual MultiLabelVertex_scc*
    toVertex(char* line)
    {
        char * pch;
        pch = strtok(line, "\t");
        MultiLabelVertex_scc* v = new MultiLabelVertex_scc;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        v->value().color = atoi(pch);
        if (v->id == -1)
        {
            return v;
        }
        pch = strtok(NULL, " ");
        v->value().sccTag = atoi(pch);

        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++)
        {
            pch = strtok(NULL, " ");
            v->value().in_edges.push_back(atoi(pch));
        }
        pch = strtok(NULL, " ");
        num = atoi(pch);
        for (int i = 0; i < num; i++)
        {
            pch = strtok(NULL, " ");
            v->value().out_edges.push_back(atoi(pch));
        }
        return v;
    }

    virtual void toline(MultiLabelVertex_scc* v, BufferedWriter & writer)
    {
        if (v->id == -1)
        {
            sprintf(buf, "-1\t%d\n", v->value().color);
            writer.write(buf);
            return;
        }
        vector<VertexID> & in_edges = v->value().in_edges;
        vector<VertexID> & out_edges = v->value().out_edges;
        hash_set<VertexID>& minForward = v->value().minForward;
        hash_set<VertexID>& minBackward = v->value().minBackward;

        sprintf(buf, "%d\t%d %d", v->id, v->value().color, v->value().sccTag);
        writer.write(buf);

        sprintf(buf, " %d", minForward.size());
        writer.write(buf);

        for (hash_set<VertexID>::iterator it = minForward.begin();
                it != minForward.end(); it++)
        {
            sprintf(buf, " %d", *it);
            writer.write(buf);
        }

        sprintf(buf, " %d", minBackward.size());
        writer.write(buf);

        for (hash_set<VertexID>::iterator it = minBackward.begin();
                it != minBackward.end(); it++)
        {
            sprintf(buf, " %d", *it);
            writer.write(buf);
        }

        sprintf(buf, " %d", in_edges.size());
        writer.write(buf);

        for (int i = 0; i < in_edges.size(); i++)
        {
            sprintf(buf, " %d", in_edges[i]);
            writer.write(buf);
        }
        sprintf(buf, " %d", out_edges.size());
        writer.write(buf);
        for (int i = 0; i < out_edges.size(); i++)
        {
            sprintf(buf, " %d", out_edges[i]);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void scc_multilabel(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    MultiLabelAgg_scc agg;
    MultiLabelWorker_scc worker;
    worker.setAggregator(&agg);
    worker.run(param);
}
