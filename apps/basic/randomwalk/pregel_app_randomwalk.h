#include "basic/pregel-dev.h"
#include <cmath>
#include <ctime>
#include <cstdlib>
using namespace std;
const int TOPK = 10;
const int L = 100;
const int R = (int)(0.15 * L);

struct RandomWalkValue {
    hash_map<VertexID, int> counts;
    vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const RandomWalkValue& v)
{
    m << v.counts;
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, RandomWalkValue& v)
{
    m >> v.counts;
    m >> v.edges;
    return m;
}

double myrand()
{
    return 1.0 * rand() / RAND_MAX;
}

int geom(double e)
{
    int len = 1;
    while (myrand() < e) {
        len++;
    }
    return len;
}

//====================================

struct RandomWalkAggType {
    int lambdaTotalLength; // lambda0 + lambda1 + ... +  lambda(N-1)
    int round; // from 0 to N-1
    int lambda;
    int length;
};

ibinstream& operator<<(ibinstream& m, const RandomWalkAggType& v)
{
    m << v.lambdaTotalLength;
    m << v.round;
    m << v.lambda;
    m << v.length;
    return m;
}

obinstream& operator>>(obinstream& m, RandomWalkAggType& v)
{
    m >> v.lambdaTotalLength;
    m >> v.round;
    m >> v.lambda;
    m >> v.length;
    return m;
}

// two types of messages. Positive means a source vertex are visiting this vertex. Negative (-vid - 1)means counter + 1

class RandomWalkVertex : public Vertex<VertexID, RandomWalkValue, int> {
public:
    int negate(int v)
    {
        return -v - 1;
    }

    VertexID uniformSampleFromNeighbors()
    {
        return value().edges[rand() % value().edges.size()];
    }

    void insert(int vid)
    {
        value().counts[vid] += 1;
    }

    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1) {
            return; // for Agg init
        }

        RandomWalkAggType* agg = (RandomWalkAggType*)getAgg();

        if (agg->length == 0) {
            if (value().edges.size() > 0) {
                int nextVertexToGo = uniformSampleFromNeighbors();
                send_message(nextVertexToGo, id);
                insert(nextVertexToGo);
            }
        } else if (agg->length < agg->lambda) {
            for (int i = 0; i < messages.size(); i++) {
                if (messages[i] < 0) {
                    int vid = negate(messages[i]);
                    insert(vid);
                } else {
                    if (value().edges.size() > 0) {
                        int nextVertexToGo = uniformSampleFromNeighbors();
                        send_message(nextVertexToGo, messages[i]);
                        send_message(messages[i], negate(nextVertexToGo));
                    }
                }
            }
        } else {
            for (int i = 0; i < messages.size(); i++) {
                int vid = negate(messages[i]);
                insert(vid);
            }
        }
    }
};

class RandomWalkAgg : public Aggregator<RandomWalkVertex, RandomWalkAggType, RandomWalkAggType> {
private:
    RandomWalkAggType value;

public:
    virtual void init()
    {
    }

    virtual void stepPartial(RandomWalkVertex* v)
    {
    }

    virtual void stepFinal(RandomWalkAggType* part)
    {
    }

    virtual RandomWalkAggType* finishPartial()
    {
        return &value;
    }
    virtual RandomWalkAggType* finishFinal()
    {
        if (step_num() == 1) {
            value.lambda = geom(1 - 0.15);
            value.lambdaTotalLength = value.lambda;
            value.round = 0;
            value.length = 0;
        } else {
            RandomWalkAggType* agg = (RandomWalkAggType*)getAgg();

            if (agg->length == agg->lambda) {
                if (agg->round == R - 1) {
                    forceTerminate();
                } else {
                    value.lambda = geom(1 - 0.15);
                    value.lambdaTotalLength += value.lambda;
                    value.round = agg->round + 1;
                    value.length = 0;
                }
            } else {
                value.lambda = agg->lambda;
                value.lambdaTotalLength = agg->lambdaTotalLength;
                value.round = agg->round;
                value.length = agg->length + 1;
            }
        }
        if (_my_rank == 0) {
            cout << "Cur Lambda: " << value.lambda
                 << " Length: " << value.length
                 << " Total Lambda: " << value.lambdaTotalLength
                 << " Cur Round: " << value.round
                 << " Total Round: " << R << endl;
        }
        return &value;
    }
};

class RandomWalkWorker : public Worker<RandomWalkVertex, RandomWalkAgg> {
    char buf[100];

public:
    virtual RandomWalkVertex* toVertex(char* line)
    {
        /*
        char* pch;
        pch = strtok(line, "\t");
        RandomWalkVertex* v = new RandomWalkVertex;
        v->id = atoi(pch);
        while (pch = strtok(NULL, "\t")) {
            v->value().edges.push_back(atoi(pch));
        }
        */

        char* pch;
        pch = strtok(line, "\t");
        RandomWalkVertex* v = new RandomWalkVertex;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        while (num--) {
            pch = strtok(NULL, " ");
            v->value().edges.push_back(atoi(pch));
        }

        return v;
    }

    virtual void toline(RandomWalkVertex* v, BufferedWriter& writer)
    {
        int sum = ((RandomWalkAggType*)getAgg())->lambdaTotalLength;
        hash_map<VertexID, int>& counts = v->value().counts;
        vector<pair<int, int> > topkResults;
        for (hash_map<VertexID, int>::iterator it = counts.begin(); it != counts.end(); it++) {
            topkResults.push_back(make_pair(it->second, it->first));
        }
        sort(topkResults.begin(), topkResults.end());
        reverse(topkResults.begin(), topkResults.end());

        sprintf(buf, "%d", v->id);
        writer.write(buf);

        for (int i = 0; i < topkResults.size(); i++) {
            if (i == TOPK)
                break;

            int vid = topkResults[i].second;
            double pagerank = 1.0 * topkResults[i].first / sum;
            int visit = topkResults[i].first;
            //sprintf(buf, " %d %f", vid, pagerank);
            sprintf(buf, " %d %d", vid, visit);
            writer.write(buf);
        }
        writer.write("\n");
    }
};
void pregel_randomwalk(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    RandomWalkWorker worker;
    RandomWalkAgg agg;
    srand(time(0));
    worker.setAggregator(&agg);
    worker.run(param);
}
