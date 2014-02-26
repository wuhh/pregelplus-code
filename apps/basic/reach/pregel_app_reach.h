#include "basic/pregel-dev.h"
using namespace std;

int src=0;
int dst=-1;

struct ReachValue
{
    char tag;
    vector<VertexID> in_edges;
    vector<VertexID> out_edges;
};

ibinstream & operator<<(ibinstream & m, const ReachValue & v)
{
    m<<v.tag;
    m<<v.in_edges;
    m<<v.out_edges;
    return m;
}

obinstream & operator>>(obinstream & m, ReachValue & v)
{
    m>>v.tag;
    m>>v.in_edges;
    m>>v.out_edges;
    return m;
}

class ReachVertex:public Vertex<VertexID, ReachValue, char>
{
public:
    virtual void compute(MessageContainer & messages)
    {
        if(step_num()==1)
        {
            char & mytag=value().tag;
            if(mytag==2)   //v->dst
            {
                vector<VertexID> & nbs=value().in_edges;
                for(int i=0; i<nbs.size(); i++)
                {
                    send_message(nbs[i], mytag);
                }
            }
            else if(mytag==1)     //src->v
            {
                vector<VertexID> & nbs=value().out_edges;
                for(int i=0; i<nbs.size(); i++)
                {
                    send_message(nbs[i], mytag);
                }
            }
        }
        else
        {
            char tag=0;
            for(MessageIter it=messages.begin(); it!=messages.end(); it++)
            {
                tag|=(*it);
            }
            char & mytag=value().tag;
            if((tag|mytag) != mytag)
            {
                mytag|=tag;
                if(mytag==3)
                {
                    forceTerminate();
                }
                else if(tag==2)     //v->dst
                {
                    vector<VertexID> & nbs=value().in_edges;
                    for(int i=0; i<nbs.size(); i++)
                    {
                        send_message(nbs[i], mytag);
                    }
                }
                else if(tag==1)     //src->v
                {
                    vector<VertexID> & nbs=value().out_edges;
                    for(int i=0; i<nbs.size(); i++)
                    {
                        send_message(nbs[i], mytag);
                    }
                }
            }
        }
        vote_to_halt();
    }

    virtual void print() {} //no use
};

class ReachWorker:public Worker<ReachVertex>
{
    char buf[1024];
public:
    //input format: vid \t num_inNB nb1 nb2 ... \t num_outNB nb1 nb2 ...
    //output format: no output if not reachable
    virtual ReachVertex* toVertex(char* line)
    {
        char * pch;
        pch=strtok(line, "\t");
        ReachVertex* v=new ReachVertex;
        v->id=atoi(pch);
        vector<VertexID> & in_edges=v->value().in_edges;
        pch=strtok(NULL, " ");
        int indegree=atoi(pch);
        for(int i=0; i<indegree; i++)
        {
            pch=strtok(NULL, " ");
            in_edges.push_back(atoi(pch));
        }
        vector<VertexID> & out_edges=v->value().out_edges;
        pch=strtok(NULL, " ");
        int outdegree=atoi(pch);
        for(int i=0; i<outdegree; i++)
        {
            pch=strtok(NULL, " ");
            out_edges.push_back(atoi(pch));
        }
        ////////
        if(v->id==src) v->value().tag=1;
        else if(v->id==dst) v->value().tag=2;
        else
        {
            v->value().tag=0;
            v->vote_to_halt();
        }
        return v;
    }

    virtual void toline(ReachVertex* v, BufferedWriter& writer)
    {
        if(v->value().tag==1)
        {
            sprintf(buf, "%d\n", v->id);
            writer.write(buf);
        }
    }
};

class ReachCombiner:public Combiner<char>
{
public:
    virtual void combine(char & old, const char & new_msg)
    {
        old|=new_msg;
    }
};

void pregel_reach(string in_path, string out_path, bool use_combiner)
{
    WorkerParams param;
    param.input_path=in_path;
    param.output_path=out_path;
    param.force_write=true;
    param.native_dispatcher=false;
    ReachWorker worker;
    ReachCombiner combiner;
    if(use_combiner) worker.setCombiner(&combiner);
    worker.run(param);
}
