#ifndef WORKER_H
#define WORKER_H

#include <vector>
#include "utils/global.h"
#include "MessageBuffer.h"
#include <string>
#include "utils/communication.h"
#include "utils/io.h"
#include "utils/Combiner.h"
#include "utils/Aggregator.h"
using namespace std;

template <class VertexT, class AggregatorT = DummyAgg> //user-defined VertexT
class Worker {
    typedef vector<VertexT*> VertexContainer;
    typedef typename VertexContainer::iterator VertexIter;

    typedef typename VertexT::KeyType KeyT;
    typedef typename VertexT::MessageType MessageT;
    typedef typename VertexT::HashType HashT;

    typedef MessageBuffer<VertexT> MessageBufT;
    typedef typename MessageBufT::MessageContainerT MessageContainerT;
    typedef typename MessageBufT::Map Map;
    typedef typename MessageBufT::MapIter MapIter;

    typedef typename AggregatorT::PartialType PartialT;
    typedef typename AggregatorT::FinalType FinalT;

public:
    Worker()
    {
        //init_workers();//put to run.cpp
        message_buffer = new MessageBuffer<VertexT>;
        global_message_buffer = message_buffer;
        active_count = 0;
        combiner = NULL;
        global_combiner = NULL;
        aggregator = NULL;
        global_aggregator = NULL;
        global_agg = NULL;
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

    virtual ~Worker()
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
            VertexT* v = vertexes[i];
            _loaded_parts[hash(v->id)].push_back(v);
        }
        //exchange vertices to add
        all_to_all(_loaded_parts);

        //delete sent vertices
        for (int i = 0; i < vertexes.size(); i++) {
            VertexT* v = vertexes[i];
            if (hash(v->id) != _my_rank)
                delete v;
        }
        vertexes.clear();
        //collect vertices to add
        for (int i = 0; i < _num_workers; i++) {
            vertexes.insert(vertexes.end(), _loaded_parts[i].begin(),
                            _loaded_parts[i].end());
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
    void activate_all()
    {
        for (int i = 0; i < vertexes.size(); i++) {
            if (!vertexes[i]->is_active()) {
                vertexes[i]->activate();
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

    inline void add_vertex(VertexT* vertex)
    {

        vertexes.push_back(vertex);
        if (vertex->is_active())
            active_count++;
    }

    void agg_sync()
    {
        AggregatorT* agg = (AggregatorT*)get_aggregator();
        if (agg != NULL) {
            if (_my_rank != MASTER_RANK) //send partialT to aggregator
            {
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
    virtual VertexT* toVertex(const char* line) = 0; //this is what user specifies!!!!!!

    void loadFiles(const vector<string>& assignedSplits)
    {
        hdfsFS fs = getHdfsFS();
        for (int i = 0; i < assignedSplits.size(); i++) {
            BufferedReader reader(assignedSplits[i].c_str(), fs);
            const char* line = 0;
            while ((line = reader.getLine()) != NULL) {
                VertexT* v = toVertex(line);
                if (v != NULL)
                    add_vertex(v);
            }
        }
        hdfsDisconnect(fs);
    }

    void loadGraph(const vector<string>& inputs, bool native_dispatcher)
    {
        // Timer Initialization
        init_timers();
        ResetTimer(WORKER_TIMER);

        vector<vector<string> >* arrangement = NULL;
        if (_my_rank == MASTER_RANK) {
            if (native_dispatcher)
                arrangement = dispatchLocality(inputs);
            else
                arrangement = dispatchRan(inputs);

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

    //user-defined graphDumper ==============================
    virtual void toline(VertexT* v, BufferedWriter& writer) = 0; //this is what user specifies!!!!!!

    void dumpGraph(const char* outpath)
    {

        ResetTimer(WORKER_TIMER);

        hdfsFS fs = getHdfsFS();
        BufferedWriter writer(outpath, fs, _my_rank);

        for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
            writer.check();

            VertexT* v = *it;
            toline(v, writer);
        }

        hdfsDisconnect(fs);

        StopTimer(WORKER_TIMER);
        PrintTimer("Dump Time", WORKER_TIMER);
    }

    int checkIODirectory(const vector<string>& inputs, const char* output,
                         bool force)
    {
        //check path + init
        if (_my_rank == MASTER_RANK) {
            for (int i = 0; i < inputs.size(); i++) {
                if (dirCheck(inputs[i].c_str()) == -1)
                    return -1;
            }

            if (dirCheck(output, 1, force) == -1)
                return -1;
        }
        return 0;
    }

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

    void compute(int num_phases = 1)
    {
        init_timers();
        ResetTimer(WORKER_TIMER);

        clearBits();
        long long global_msg_num = 0, global_vadd_num = 0;

        for (global_phase_num = 1; global_phase_num <= num_phases;
             global_phase_num++) {

            if (_my_rank == MASTER_RANK && num_phases > 1) {
                cout << "################ Phase " << global_phase_num
                     << " ################" << endl;
            }

            // global variables initialization
            global_step_num = 0;

            while (true) { //supersteps
                global_step_num++;
                ResetTimer(SUPERSTEP_TIMER);
                // bitmap sync
                char bits_bor = all_bor(global_bor_bitmap);
                // check forceTerminate
                if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1)
                    break;
                // update vnum
                get_vnum() = all_sum(vertexes.size());

                // check wakeAll
                int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
                if (wakeAll) {
                    active_vnum() = get_vnum();
                } else {
                    active_vnum() = all_sum(active_count);
                    if (active_vnum() == 0
                        && getBit(HAS_MSG_ORBIT, bits_bor) == 0)
                        break; //all_halt AND no_msg
                }
                // Aggregator Initialization
                AggregatorT* agg = (AggregatorT*)get_aggregator();
                if (agg != NULL)
                    agg->init();

                // clear Bits and compute
                clearBits();
                if (wakeAll == 1)
                    all_compute();
                else
                    active_compute();
                // Messages combine
                message_buffer->combine();

                // step_msg & step_vadd statistics
                long long step_msg_num = master_sum_LL(
                    message_buffer->get_total_msg());
                long long step_vadd_num = master_sum_LL(
                    message_buffer->get_total_vadd());

                if (_my_rank == MASTER_RANK) {
                    global_msg_num += step_msg_num;
                    global_vadd_num += step_vadd_num;
                }

                // Sync new added vertices and aggregator

                vector<VertexT*>& to_add = message_buffer->sync_messages();
                agg_sync();
                for (int i = 0; i < to_add.size(); i++)
                    add_vertex(to_add[i]);
                to_add.clear();

                // Output superstep info

                worker_barrier();
                StopTimer(SUPERSTEP_TIMER);
                if (_my_rank == MASTER_RANK) {
                    cout << "Superstep " << global_step_num
                         << " done. Time elapsed: " << get_timer(4)
                         << " seconds" << endl;
                    cout << "#msgs: " << step_msg_num << ", #vadd: "
                         << step_vadd_num << endl;
                }
            }

            if (num_phases > 1) {
                activate_all();
            }
        }
        StopTimer(WORKER_TIMER);
        PrintTimer("Communication Time", COMMUNICATION_TIMER);
        PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
        PrintTimer("- Transfer Time", TRANSFER_TIMER);
        PrintTimer("Total Computational Time", WORKER_TIMER);
        if (_my_rank == MASTER_RANK)
            cout << "Total #msgs=" << global_msg_num << ", Total #vadd="
                 << global_vadd_num << endl;
    }

    void run(const MultiInputParams& params)
    {
        //================== Data Loading ====================================

        // Check IO Directory
        checkIODirectory(params.input_paths, params.output_path.c_str(),
                         params.force_write);

        // Load Graphs and Sync Vertices
        loadGraph(params.input_paths, params.native_dispatcher);

        //================== Compute ====================================
        compute();

        //================== Dumping ====================================

        dumpGraph(params.output_path.c_str());
    }

    void run(const WorkerParams& params, int num_phases = 1)
    {

        //================== Data Loading ====================================

        // Check IO Directory
        checkIODirectory(params.input_path.c_str(), params.output_path.c_str(),
                         params.force_write);

        // Load Graphs and Sync Vertices
        loadGraph(params.input_path.c_str(), params.native_dispatcher);

        //================== Compute ====================================
        compute(num_phases);

        //================== Dumping ====================================

        dumpGraph(params.output_path.c_str());
    }

private:
    HashT hash;
    VertexContainer vertexes;
    int active_count;

    MessageBuffer<VertexT>* message_buffer;
    Combiner<MessageT>* combiner;
    AggregatorT* aggregator;
};

#endif
