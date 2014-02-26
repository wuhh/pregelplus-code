//app: PageRank

#include "Vertex.h"
#include "Worker.h"
#include <iostream>
#include "Global.h"
#include <set>
using namespace std;

struct CCValue
{
	vector<VertexID> edges;
};

Marshall & operator<<(Marshall & m, const CCValue & v){
	m<<v.edges;
	return m;
}

Unmarshall & operator>>(Unmarshall & m, CCValue & v){
	m>>v.edges;
	return m;
}

//====================================

class CCVertex:public Vertex<VertexID, CCValue, VertexID>
{
	public:
		virtual void compute(MessageContainer & messages)
		{
		    vote_to_halt();
		}

		virtual void print()
		{
		}

};

class CCWorker:public Worker<CCVertex>
{

	public:
		//C version
		virtual CCVertex* toVertex(char* line)
		{
            char * pch;
			pch=strtok(line, "\t");
			CCVertex* v=new CCVertex;
			v->id()=atoi(pch);
			pch=strtok(NULL, " ");
			//color will be assigned in Superstep 1
			set<int> nset;
            while(pch=strtok(NULL, " "))
			{
			    char* pc=strtok(NULL, " ");
                if(pc == NULL)
                    break;
                nset.insert(atoi(pch));
			}
            for(set<int>::iterator it = nset.begin(); it != nset.end() ; it++)
            {
		        v->value().edges.push_back(*it);
            }
			return v;
		}

		virtual void toline(CCVertex* v, Writer* writer)
		{
            if(v->id() == -1)
                return;
            char buf[1024];
			sprintf(buf, "%d\t%d 1", v->id(), v->id());
            writer->write(buf);

            for(int i = 0 ;i < v->value().edges.size() ; i ++)
            {
                sprintf(buf," %d",v->value().edges[i]);
                writer->write(buf);
            }

            writer->finalize();
		}
};

class CCCombiner:public Combiner<VertexID>
{
	public:
		virtual void combine(VertexID & old, const VertexID & new_msg)
		{
			if(old>new_msg) old=new_msg;
		}
};

int main(int argc, char* argv[]){
	WorkerParams param;
	param.input_path="/btc/over";
	param.output_path="/btc/processed";
	param.force_write=true;
	param.native_dispatcher=false;
	CCWorker worker;
	CCCombiner combiner;
	worker.setCombiner(&combiner);
	worker.run(param);
	return 0;
}
