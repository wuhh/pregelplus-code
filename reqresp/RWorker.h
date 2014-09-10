#ifndef RWORKER_H
#define RWORKER_H

#include <vector>
#include "utils/global.h"
#include "RMessageBuffer.h"
#include <string>
#include "utils/communication.h"
#include "utils/io.h"
#include "utils/Combiner.h"
#include "utils/Aggregator.h"
using namespace std;

long long step_resp_num;

bool hasResps() //should not be called in superstep 1
{
    return step_resp_num > 0;
}

template <class RVertexT, class AggregatorT = DummyAgg> //user-defined RVertexT
class RWorker {
    typedef vector<RVertexT*> VertexContainer;
    typedef typename VertexContainer::iterator VertexIter;

    typedef typename RVertexT::KeyType KeyT;
    typedef typename RVertexT::MessageType MessageT;
    typedef typename RVertexT::HashType HashT;
    typedef typename RVertexT::RespondType RespondT;
    typedef RMessageBuffer<RVertexT> MessageBufT;
    typedef typename MessageBufT::MessageContainerT MessageContainerT;
    typedef typename MessageBufT::Map Map;
    typedef typename MessageBufT::MapIter MapIter;
    typedef typename MessageBufT::ReqSet ReqSet;
    typedef typename ReqSet::iterator RSetIter;
    typedef typename MessageBufT::ReqSets ReqSets;
    typedef typename MessageBufT::RespMap RespMap;
    typedef typename MessageBufT::RespMaps RespMaps;
    typedef typename MessageBufT::VMap VMap;

    typedef typename AggregatorT::PartialType PartialT;
    typedef typename AggregatorT::FinalType FinalT;

public:
    RWorker()
    {
        //init_workers();//put to run.cpp
        message_buffer = new MessageBufT;
        global_message_buffer = message_buffer;
        active_count = 0;
        combiner = NULL;
        global_combiner = NULL;
        aggregator = NULL;
        global_aggregator = NULL;
        global_agg = NULL;
        //step_resp_num=0;//step_resp_num should not be used in superstep 1
    }

    void setCombiner(Combiner<MessageT>* cb)
    {
        combiner = cb;
        global_combiner = cb;
    }

    void setAggregator(AggregatorT* ag)
    {
        aggregator = ag;
        global_aggregator = ag;
        global_agg = new FinalT;
    }

    virtual ~RWorker()
    {
        for (int i = 0; i < vertexes.size(); i++)
            delete vertexes[i];
        delete message_buffer;
        if (getAgg() != NULL)
            delete (FinalT*)global_agg;
        //worker_finalize();//put to run.cpp
        worker_barrier(); //newly added for ease of multi-job programming in run.cpp
    }

    //==================================
    //sub-functions
    void sync_graph()
    {
        //ResetTimer(4);
        //set send buffer
        vector<VertexContainer> _loaded_parts(_num_workers);
        for (int i = 0; i < vertexes.size(); i++) {
            RVertexT* v = vertexes[i];
            _loaded_parts[hash(v->id)].push_back(v);
        }
        //exchange vertices to add
        all_to_all(_loaded_parts);

        //delete sent vertices
        for (int i = 0; i < vertexes.size(); i++) {
            RVertexT* v = vertexes[i];
            if (hash(v->id) != _my_rank)
                delete v;
        }
        vertexes.clear();
        //collect vertices to add
        for (int i = 0; i < _num_workers; i++) {
            vertexes.insert(vertexes.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
        }
        _loaded_parts.clear();
        //StopTimer(4);
        //PrintTimer("Reduce Time",4);
    };

    void active_compute()
    {
        active_count = 0;
        MessageBufT* mbuf = (MessageBufT*)get_message_buffer();
        vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
        for (int i = 0; i < vertexes.size(); i++) {
            if (v_msgbufs[i].size() == 0) {
                if (vertexes[i]->is_active()) {
                    vertexes[i]->compute(v_msgbufs[i]);
                    AggregatorT* agg = (AggregatorT*)get_aggregator();
                    if (agg != NULL)
                        agg->stepPartial(vertexes[i]);
                    if (vertexes[i]->is_active())
                        active_count++;
                }
            } else {
                vertexes[i]->activate();
                vertexes[i]->compute(v_msgbufs[i]);
                v_msgbufs[i].clear(); //clear used msgs
                AggregatorT* agg = (AggregatorT*)get_aggregator();
                if (agg != NULL)
                    agg->stepPartial(vertexes[i]);
                if (vertexes[i]->is_active())
                    active_count++;
            }
        }
    }

    void all_compute()
    {
        active_count = 0;
        MessageBufT* mbuf = (MessageBufT*)get_message_buffer();
        vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
        for (int i = 0; i < vertexes.size(); i++) {
            vertexes[i]->activate();
            vertexes[i]->compute(v_msgbufs[i]);
            v_msgbufs[i].clear(); //clear used msgs
            AggregatorT* agg = (AggregatorT*)get_aggregator();
            if (agg != NULL)
                agg->stepPartial(vertexes[i]);
            if (vertexes[i]->is_active())
                active_count++;
        }
    }

    void respond_reqs()
    {
        //new implementaion: scan reqs
        ReqSets& sets = message_buffer->in_req_sets;
        VMap& vmap = message_buffer->vmap;
        for (int i = 0; i < _num_workers; i++) {
            ReqSet& set = sets[i];
            for (RSetIter it = set.begin(); it != set.end(); it++) {
                KeyT vid = *it;
                typename VMap::iterator vit = vmap.find(vid);
                if (vit != vmap.end())
                    message_buffer->add_respond_implicit(vid, i, vit->second->respond());
            }
        }
    }

    inline void add_vertex(RVertexT* vertex)
    {
        vertexes.push_back(vertex);
        if (vertex->is_active())
            active_count++;
    }

    void agg_sync()
    {
        AggregatorT* agg = (AggregatorT*)get_aggregator();
        if (agg != NULL) {
            if (_my_rank != MASTER_RANK) { //send partialT to aggregator
                //gathering PartialT
                PartialT* part = agg->finishPartial();
                //------------------------ strategy choosing BEGIN ------------------------
                StartTimer(COMMUNICATION_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                ibinstream m;
                m << part;
                int sendcount = m.size();
                StopTimer(SERIALIZATION_TIMER);
                int total = all_sum(sendcount);
                StopTimer(COMMUNICATION_TIMER);
                //------------------------ strategy choosing END ------------------------
                if (total <= AGGSWITCH)
                    slaveGather(*part);
                else {
                    send_ibinstream(m, MASTER_RANK);
                }
                //scattering FinalT
                slaveBcast(*((FinalT*)global_agg));
            } else {
                //------------------------ strategy choosing BEGIN ------------------------
                int total = all_sum(0);
                //------------------------ strategy choosing END ------------------------
                //gathering PartialT
                if (total <= AGGSWITCH) {
                    vector<PartialT*> parts(_num_workers);
                    masterGather(parts);
                    for (int i = 0; i < _num_workers; i++) {
                        if (i != MASTER_RANK) {
                            PartialT* part = parts[i];
                            agg->stepFinal(part);
                            delete part;
                        }
                    }
                } else {
                    for (int i = 0; i < _num_workers; i++) {
                        if (i != MASTER_RANK) {
                            obinstream um = recv_obinstream(i);
                            PartialT* part;
                            um >> part;
                            agg->stepFinal(part);
                            delete part;
                        }
                    }
                }
                //scattering FinalT
                FinalT* final = agg->finishFinal();
                //cannot set "global_agg=final" since MASTER_RANK works as a slave, and agg->finishFinal() may change
                *((FinalT*)global_agg) = *final; //deep copy
                masterBcast(*((FinalT*)global_agg));
            }
        }
    }

    //user-defined graphLoader ==============================
    virtual RVertexT* toVertex(char* line) = 0; //this is what user specifies!!!!!!

    void load_vertex(RVertexT* v)
    { //called by load_graph
        add_vertex(v);
    }

    //user-defined graphDumper ==============================
    virtual void toline(RVertexT* v, BufferedWriter& writer) = 0; //this is what user specifies!!!!!!

    void dumpGraph(const char* outpath)
    {

        ResetTimer(WORKER_TIMER);

        hdfsFS fs = getHdfsFS();
        BufferedWriter* writer = new BufferedWriter(outpath, fs, _my_rank);

        for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
            writer->check();
            VertexT* v = *it;
            toline(v, *writer);
        }

        delete writer;
        hdfsDisconnect(fs);

        StopTimer(WORKER_TIMER);
        PrintTimer("Dump Time", WORKER_TIMER);
    }
    
    void loadGraph(const char* inpath, bool native_dispatcher)
    {
        // Timer Initialization
        init_timers();
        ResetTimer(WORKER_TIMER);

        vector<vector<string> >* arrangement = NULL;
        if (_my_rank == MASTER_RANK) {
            if (native_dispatcher)
                arrangement = dispatchLocality(inpath);
            else
                arrangement = dispatchRan(inpath);

            masterScatter(*arrangement);
            vector<string>& assignedSplits = (*arrangement)[0];
            loadFiles(assignedSplits);
            delete arrangement;
        } else {
            vector<string> assignedSplits;
            slaveScatter(assignedSplits);
            //reading assigned splits (map)
            loadFiles(assignedSplits);
        }

        //send vertices according to hash_id (reduce)
        sync_graph();

        // Msg Buffer Initialization
        message_buffer->init(vertexes);

        //barrier for data loading
        worker_barrier(); //@@@@@@@@@@@@@
        StopTimer(WORKER_TIMER);
        PrintTimer("Load Time", WORKER_TIMER);
    }
    
    //=======================================================

    int checkIODirectory(const char* input, const char* output, bool force)
    {
        //check path + init
        if (_my_rank == MASTER_RANK) {
            if (dirCheck(input) == -1)
                return -1;
            if (dirCheck(output, 1, force) == -1)
                return -1;
        }
        return 0;
    }
    
    void compute()
    {
        init_timers();
        ResetTimer(WORKER_TIMER);
        //supersteps
        global_step_num = 0;
        long long global_msg_num = 0, global_vadd_num = 0, global_req_num = 0, global_resp_num = 0;
        while (true) {
            global_step_num++;
            ResetTimer(4);
            //===================
            char bits_bor = all_bor(global_bor_bitmap);
            if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1)
                break;
            get_vnum() = all_sum(vertexes.size());
            int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
            if (wakeAll == 0) {
                active_vnum() = all_sum(active_count);
                if (active_vnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0)
                    break; //all_halt AND no_msg
            } else
                active_vnum() = get_vnum();
            //===================
            AggregatorT* agg = (AggregatorT*)get_aggregator();
            if (agg != NULL)
                agg->init();
            //===================
            clearBits();
            if (wakeAll == 1)
                all_compute();
            else
                active_compute();
            message_buffer->combine();
            //=================== added for RWorker ===================
            //bool hasRR=all_lor(message_buffer->hasReqResp());//@@@@@@@@@@@@
            bool hasRR = all_bor(message_buffer->hasReqResp()); //@@@@@@@@@@@@
            if (hasRR) {
                long long step_req_num = master_sum_LL(message_buffer->get_total_req());
                message_buffer->sync_reqs();
                if (_my_rank == MASTER_RANK) {
                    global_req_num += step_req_num;
                    cout << "-- " << step_req_num << " reqs exchanged" << endl;
                }
                respond_reqs();
                step_resp_num = all_sum_LL(message_buffer->get_total_resp()); //all_sum_LL instead of master_sum_LL
                message_buffer->sync_resps();
                if (_my_rank == MASTER_RANK) {
                    global_resp_num += step_resp_num;
                    if (_my_rank == MASTER_RANK)
                        cout << "-- " << step_resp_num << " resps exchanged" << endl;
                }
            }
            //=================== added for RWorker ===================

            long long step_msg_num = master_sum_LL(message_buffer->get_total_msg());
            long long step_vadd_num = master_sum_LL(message_buffer->get_total_vadd());
            if (_my_rank == MASTER_RANK) {
                global_msg_num += step_msg_num;
                global_vadd_num += step_vadd_num;
            }
            vector<RVertexT*>& to_add = message_buffer->sync_messages();
            agg_sync();
            for (int i = 0; i < to_add.size(); i++)
                add_vertex(to_add[i]);
            to_add.clear();
            //===================
            worker_barrier();
            StopTimer(4);
            if (_my_rank == MASTER_RANK) {
                cout << "Superstep " << global_step_num << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
                cout << "* #msgs: " << step_msg_num << ", #vadd: " << step_vadd_num << endl;
            }
        }
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Communication Time", COMMUNICATION_TIMER);
        PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
        PrintTimer("- Transfer Time", TRANSFER_TIMER);
        PrintTimer("Total Computational Time", WORKER_TIMER);
        if (_my_rank == MASTER_RANK)
            cout << "Total #msgs=" << global_msg_num << ", Total #vadd=" << global_vadd_num << ", Total #reqs=" << global_req_num << ", Total #resps=" << global_resp_num << endl;

    }
    // run the worker
    void run(const WorkerParams& params)
    {
        //================== Data Loading ====================================

        // Check IO Directory
        checkIODirectory(params.input_path.c_str(), params.output_path.c_str(),
                         params.force_write);

        // Load Graphs and Sync Vertices
        loadGraph(params.input_path.c_str(), params.native_dispatcher);
        
        //================== Compute ====================================
        compute();

        //================== Dumping ====================================

        dumpGraph(params.output_path.c_str());
    }

private:
    HashT hash;
    VertexContainer vertexes;
    int active_count;

    MessageBufT* message_buffer;
    Combiner<MessageT>* combiner;
    AggregatorT* aggregator;
};

#endif