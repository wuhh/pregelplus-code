#include "basic/pregel-dev.h"
#include <sstream>
using namespace std;

struct kcoreValue {
    int K;
    vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const kcoreValue& v)
{
    m << v.K;
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, kcoreValue& v)
{
    m >> v.K;
    m >> v.edges;
    return m;
}

//====================================

class kcoreVertex : public Vertex<VertexID, kcoreValue, VertexID> {
public:

    virtual void compute(MessageContainer& messages)
    {
    	if(step_num() == 1)
   	    	return;
    	/* update adj list */

    	vector<VertexID> newEdges;
    	vector<VertexID>& edges = value().edges;
    	hash_set<VertexID> st;
    	for(int i = 0 ;i < messages.size(); i ++)
    	{
    		st.insert(messages[i]);
    	}
    	for(int i = 0 ;i < edges.size() ; i ++)
    	{
    		if(st.count(edges[i]) == 0)
    			newEdges.push_back(edges[i]);
    	}
    	edges.swap(newEdges);

    	/* calculate k-core */

    	int globalK = *((int*)getAgg());
    	if(edges.size() < globalK)
    	{
    		for(int i = 0 ;i < edges.size(); i ++)
    		{
    			send_message(edges[i],id);
    		}
    		value().K = globalK-1;
    		value().edges.clear();
        	vote_to_halt();
    	}

    }
};
class kcoreAgg : public Aggregator<kcoreVertex, bool, int> {
private:
    int K;
    bool allLessK;
public:
    virtual void init()
    {
        if(step_num() == 1)
        	K = 1;
        else
        	K = *((int*)getAgg());
        allLessK = true;
    }

    virtual void stepPartial(kcoreVertex* v)
    {
        if (v->value().edges.size() < K)
        	allLessK = false;
    }

    virtual void stepFinal(bool* part)
    {
    	allLessK = *part && allLessK;
    }

    virtual bool* finishPartial()
    {
        return &allLessK;
    }
    virtual int* finishFinal()
    {
    	if(allLessK)
    		K += 1;
    	if(_my_rank == 0) cout << "Core number: " << K << endl;
        return &K;
    }
};
class kcoreWorker : public Worker<kcoreVertex,kcoreAgg> {
    char buf[100];

public:
    //C version
    virtual kcoreVertex* toVertex(char* line)
    {
    	kcoreVertex* v = new kcoreVertex;
        istringstream ssin(line);
        ssin >> v->id;
        int num;
        ssin >> num;
        v->value().K = -1;
        for (int i = 0; i < num; i++) {
            int nb;
            ssin >> nb;
            v->value().edges.push_back(nb);
        }
        return v;
    }

    virtual void toline(kcoreVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d\n", v->id, v->value().K);
        writer.write(buf);
    }
};


void pregel_kcore(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    kcoreAgg agg;
    kcoreWorker worker;
    worker.setAggregator(&agg);
    worker.run(param);
}