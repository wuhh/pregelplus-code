#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;
//int src = 44881114;
int src = 0;

int dst = -1;
struct ReachValue_vworker
{
    char tag;
    vector<vwpair> in_edges;
    vector<vwpair> out_edges;
};

ibinstream & operator<<(ibinstream & m, const ReachValue_vworker & v)
{
    m<<v.tag;
    m<<v.in_edges;
    m<<v.out_edges;
    return m;
}

obinstream & operator>>(obinstream & m, ReachValue_vworker & v)
{
    m>>v.tag;
    m>>v.in_edges;
    m>>v.out_edges;
    return m;
}

//====================================

class ReachVertex_vworker:public Vertex<vwpair, ReachValue_vworker, char, VWPairHash>
{
public:

    virtual void compute(MessageContainer & messages)
    {
        if (step_num() == 1)
        {
            char& mytag = value().tag;
            if (mytag == 2) //v->dst
            {
                vector<vwpair>& nbs = value().in_edges;
                for (int i = 0; i < nbs.size(); i++)
                {
                    send_message(nbs[i], mytag);
                }
            } else if (mytag == 1) //src->v
            {
                vector<vwpair>& nbs = value().out_edges;
                for (int i = 0; i < nbs.size(); i++)
                {
                    send_message(nbs[i], mytag);
                }
            }
        }
        else
        {
            char tag = 0;
            for (MessageIter it = messages.begin(); it != messages.end(); it++)
            {
                tag |= (*it);
            }
            char& mytag = value().tag;
            if ((tag | mytag) != mytag)
            {
                mytag |= tag;
                if (mytag == 3)
                {
                    forceTerminate();
                }
                else if (tag == 2) //v->dst
                {
                    vector<vwpair>& nbs = value().in_edges;
                    for (int i = 0; i < nbs.size(); i++)
                    {
                        send_message(nbs[i], mytag);
                    }
                } else if (tag == 1) //src->v
                {
                    vector<vwpair>& nbs = value().out_edges;
                    for (int i = 0; i < nbs.size(); i++)
                    {
                        send_message(nbs[i], mytag);
                    }
                }
            }
        }
        vote_to_halt();
    }
};

class ReachWorker_vworker:public Worker<ReachVertex_vworker>
{
    char buf[100];

public:
    //C version
    virtual ReachVertex_vworker* toVertex(char* line)
    {
        char * pch;
        ReachVertex_vworker* v=new ReachVertex_vworker;
        pch=strtok(line, " ");
        v->id.vid=atoi(pch);
        pch=strtok(NULL, "\t");
        v->id.wid=atoi(pch);
        pch=strtok(NULL, " ");
        int num = atoi(pch);
        while(num--)
        {
            pch=strtok(NULL, " ");
            int vid=atoi(pch);
            pch=strtok(NULL, " ");
            int wid=atoi(pch);
            pch=strtok(NULL, " ");
            int d = atoi(pch);
            if(d == 0)
                v->value().in_edges.push_back(vwpair(vid, wid));
            else
                v->value().out_edges.push_back(vwpair(vid, wid));
        }
        ////////
        if (v->id.vid == src)
            v->value().tag = 1;
        else if (v->id.vid  == dst)
            v->value().tag = 2;
        else {
            v->value().tag = 0;
            v->vote_to_halt();
        }

        return v;
    }

    virtual void toline(ReachVertex_vworker* v, BufferedWriter & writer)
    {
        if (v->value().tag == 1)
        {
            sprintf(buf, "%d\n", v->id.vid);
            writer.write(buf);
        }
    }
};

class ReachCombiner : public Combiner<char> {
public:
    virtual void combine(char& old, const char& new_msg)
    {
        old |= new_msg;
    }
};

void vworker_reach(string in_path, string out_path, bool use_combiner)
{
    WorkerParams param;
    param.input_path=in_path;
    param.output_path=out_path;
    param.force_write=true;
    param.native_dispatcher=false;
    ReachWorker_vworker worker;
    ReachCombiner combiner;
    if(use_combiner)
        worker.setCombiner(&combiner);
    worker.run(param);
}
