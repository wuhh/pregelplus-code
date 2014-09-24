#include <vector>
#include <string>
#include <fstream>
#include <cassert>

#include <graphlab.hpp>


const int inf = 1e9;
int pi, max_pi;

struct vertex_data : graphlab::IS_POD_TYPE {
    std::vector< std::pair<int, int> > phis;
    int phi;
    bool updated;
    vertex_data()
        : phis()
    {
        updated = false;
    }
    
    void save(graphlab::oarchive& oarc) const{
        oarc << phis.size();
        for(int i = 0; i < phis.size(); i ++)
        {
            oarc << phis[i].first << phis[i].second;
        }
        oarc << phi;
        oarc << updated;
    }
    void load(graphlab::iarchive& iarc) {
        phis.clear();
        size_t size;
        iarc >> size;
        for(int i = 0; i < size ; i ++)
        {
            int u, v;
            iarc >> u >> v;
            phis.push_back(std::make_pair(u,v));
        }
        iarc >> phi;
        iarc >> updated;
    }
};

typedef int edge_data;
typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;

struct kcoret_gatherer {
    std::vector<int> P;

    kcoret_gatherer(): P() {}

    explicit kcoret_gatherer(int k): P(1, k){
        if(k == -1)
            P.clear(); 
    }

    kcoret_gatherer& operator+=(const kcoret_gatherer& other){
        const std::vector<int>& op = other.P;
        for(int i = 0; i < op.size(); i ++){
            P.push_back(op[i]);
        }
        return *this;
    }

    void save(graphlab::oarchive& oarc) const{
        oarc << P.size();
        for(int i = 0; i < P.size(); i ++)
        {
            oarc << P[i];
        }
    }
    void load(graphlab::iarchive& iarc) {
        P.clear();
        size_t size;
        iarc >> size;
        for(int i = 0; i < size ; i ++)
        {
            int k;
            iarc >> k;
            P.push_back(k);
        }
    }
};


// gather type is graphlab::empty, then we use message model
class kcoret : public graphlab::ivertex_program<graph_type, kcoret_gatherer>,
    public graphlab::IS_POD_TYPE {
            
            int subfunc(vertex_type& vertex, const gather_type& total)
            {
                const std::vector<int>& P = total.P;
                std::vector<int> cd(vertex.data().phi + 2, 0);
                for (int i = 0; i < P.size(); i++) {
                    cd[ std::min(P[i], vertex.data().phi) ]++;
                }
                for (int i = vertex.data().phi; i >= 1; i--) {
                    cd[i] += cd[i + 1];
                    if (cd[i] >= i)
                        return i;
                }
                assert(0);
            }


        public:
            bool changed;

            edge_dir_type gather_edges(icontext_type& context,
                    const vertex_type& vertex) const
            {
                return graphlab::IN_EDGES;
            }
            kcoret_gatherer gather(icontext_type& context, const vertex_type& vertex,
                    edge_type& edge) const {
                if(edge.data() < pi) // filtering
                   return kcoret_gatherer(-1);
                else
                   return kcoret_gatherer(edge.source().data().phi);
            }
            void apply(icontext_type& context, vertex_type& vertex,
                    const gather_type& total)
            {
                if(total.P.size() == 0)
                {
                    vertex.data().updated = false;
                    return;
                }

                changed = false;
                int x = subfunc(vertex, total);
                if(x < vertex.data().phi)
                {
                    vertex.data().phi = x;
                    changed = true;
                }
            }

            edge_dir_type scatter_edges(icontext_type& context,
                    const vertex_type& vertex) const
            {
                if (changed)
                    return graphlab::OUT_EDGES;
                else
                    return graphlab::NO_EDGES;
            }

            void scatter(icontext_type& context, const vertex_type& vertex,
                    edge_type& edge) const
            {
                context.signal(edge.target());
            }
    };

void set_kcoret_initialvalues(graph_type::vertex_type& vdata) {
    vdata.data().phi = vdata.num_out_edges(); // num_out_edges should equal to num_in_edges
    vdata.data().updated = true;
}

void add_phi(graph_type::vertex_type& vdata) {
    
    if(vdata.data().updated == false) return;
    if(vdata.data().phis.size() == 0 || vdata.data().phi < vdata.data().phis.back().second)
        vdata.data().phis.push_back( std::make_pair(pi, vdata.data().phi ) ); // num_out_edges should equal to num_in_edges
    else
        vdata.data().phis.back().first = pi;
}


struct kcoret_writer {
    std::string save_vertex(const graph_type::vertex_type& vtx)
    {
        std::stringstream strm;
        strm << vtx.id() << "\t";
        std::vector< std::pair<int,int> > phis = vtx.data().phis;
        for(int i = 0 ;i < phis.size(); i ++)
        {
            if(i != 0)
                strm << " ";
            strm << phis[i].first << " " << phis[i].second;
        }
        strm << "\n";
        return strm.str();
    }
    std::string save_edge(graph_type::edge_type e)
    {
        return "";
    }
};

bool line_parser(graph_type& graph, const std::string& filename,
        const std::string& textline)
{
    std::istringstream ssin(textline);
    graphlab::vertex_id_type vid;
    ssin >> vid;
    int out_nb;
    ssin >> out_nb;
    if (out_nb == 0)
        graph.add_vertex(vid);
    while (out_nb--) {
        graphlab::vertex_id_type other_vid;
        int t,val;
        ssin >> other_vid >> t;
        for(int i = 0 ;i < t ; i ++)
            ssin >> val;
        graph.add_edge(vid, other_vid, t);
    }
    return true;
}


struct max_t: public graphlab::IS_POD_TYPE 
{
    int value;
    max_t(): value(0) {}
    max_t(int _value): value(_value) {}
    max_t& operator+=(const max_t& other){
        value = std::max(value, other.value);
        return *this;
    }
};

struct min_t: public graphlab::IS_POD_TYPE 
{
    int value;
    min_t(): value(inf) {}
    min_t(int _value): value(_value) {}
    min_t& operator+=(const min_t& other){
        value = std::min(value, other.value);
        return *this;
    }
};


max_t get_max_pi(const graph_type::edge_type& edge) {
    return max_t(edge.data());
}
min_t get_min_pi(const graph_type::edge_type& edge) {
    return min_t(edge.data());
}
min_t get_next_pi(const graph_type::edge_type& edge) {
    if(edge.data() > pi)
        return min_t(edge.data());
    else
        return min_t();
}

int main(int argc, char** argv)
{
    graphlab::mpi_tools::init(argc, argv);

    char* input_file = argv[1];
    char* output_file = argv[2];
    std::string exec_type = "synchronous";
    graphlab::distributed_control dc;
    global_logger().set_log_level(LOG_INFO);

    graphlab::timer t;
    t.start();
    graph_type graph(dc);
    graph.load(input_file, line_parser);
    graph.finalize();


    max_pi = graph.map_reduce_edges<max_t>(get_max_pi).value;

    dc.cout() << "Loading graph in " << t.current_time() << " seconds"
        << std::endl;
    
    
    t.start();
    
    for(pi = graph.map_reduce_edges<min_t>(get_min_pi).value; pi <= max_pi; pi = graph.map_reduce_edges<min_t>(get_next_pi).value)
    {
        graph.transform_vertices(set_kcoret_initialvalues);
        
        graphlab::omni_engine<kcoret> engine(dc, graph, exec_type);
        engine.signal_all();
        engine.start();
        
        graph.transform_vertices(add_phi);
    }

    dc.cout() << "Finished Running engine in " << t.current_time()
        << " seconds." << std::endl;

    t.start();

    graph.save(output_file, kcoret_writer(), false, // set to true if each output file is to be gzipped
            true, // whether vertices are saved
            false); // whether edges are saved
    dc.cout() << "Dumping graph in " << t.current_time() << " seconds"
        << std::endl;

    graphlab::mpi_tools::finalize();
}
