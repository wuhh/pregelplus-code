#include <vector>
#include <string>
#include <fstream>
#include <cassert>

#include <graphlab.hpp>

const int inf = 1e9;
int pi, max_pi;
int CURRENT_K;

struct vertex_data {
    std::vector<std::pair<int, int> > phis;
    int phi;
    bool updated;
    vertex_data()
        : phis()
    {
        updated = false;
    }

    void save(graphlab::oarchive& oarc) const
    {
        oarc << phis.size();
        for (int i = 0; i < phis.size(); i++) {
            oarc << phis[i].first << phis[i].second;
        }
        oarc << phi;
        oarc << updated;
    }
    void load(graphlab::iarchive& iarc)
    {
        phis.clear();
        size_t size;
        iarc >> size;
        for (int i = 0; i < size; i++) {
            int u, v;
            iarc >> u >> v;
            phis.push_back(std::make_pair(u, v));
        }
        iarc >> phi;
        iarc >> updated;
    }
};

typedef int edge_data;
typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;

struct kcorent_gatherer {
    std::vector<int> P;

    kcorent_gatherer()
        : P()
    {
    }

    explicit kcorent_gatherer(int k)
        : P(1, k)
    {
        if (k == -1)
            P.clear();
    }

    kcorent_gatherer& operator+=(const kcorent_gatherer& other)
    {
        const std::vector<int>& op = other.P;
        for (int i = 0; i < op.size(); i++) {
            P.push_back(op[i]);
        }
        return *this;
    }

    void save(graphlab::oarchive& oarc) const
    {
        oarc << P.size();
        for (int i = 0; i < P.size(); i++) {
            oarc << P[i];
        }
    }
    void load(graphlab::iarchive& iarc)
    {
        P.clear();
        size_t size;
        iarc >> size;
        for (int i = 0; i < size; i++) {
            int k;
            iarc >> k;
            P.push_back(k);
        }
    }
};

class InitDeg : public graphlab::ivertex_program<graph_type, int>,
                public graphlab::IS_POD_TYPE {
public:
    edge_dir_type gather_edges(icontext_type& context,
                               const vertex_type& vertex) const
    {
        return graphlab::IN_EDGES;
    }
    int gather(icontext_type& context, const vertex_type& vertex,
               edge_type& edge) const
    {
        if (edge.data() < pi) // filtering
            return 0;
        else
            return 1;
    }
    void apply(icontext_type& context, vertex_type& vertex,
               const gather_type& total)
    {
        vertex.data().phi = total;
    }

    edge_dir_type scatter_edges(icontext_type& context,
                                const vertex_type& vertex) const
    {
        return graphlab::NO_EDGES;
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
                 edge_type& edge) const
    {
    }
};

class kcorent : public graphlab::ivertex_program<graph_type, graphlab::empty, int>, public graphlab::IS_POD_TYPE {
    void add_phi(vertex_type& vdata)
    {
        if (vdata.data().phis.size() == 0 || CURRENT_K < vdata.data().phis.back().second)
            vdata.data().phis.push_back(std::make_pair(pi, CURRENT_K)); // num_out_edges should equal to num_in_edges
        else
            vdata.data().phis.back().first = pi;
    }

public:
    int msg;
    bool just_deleted;

    kcorent()
        : msg(0)
        , just_deleted(false)
    {
    }

    void init(icontext_type& context, const vertex_type& vertex,
              const message_type& message)
    {
        msg = message;
        just_deleted = false;
    }

    edge_dir_type gather_edges(icontext_type& context,
                               const vertex_type& vertex) const
    {
        return graphlab::NO_EDGES;
    }

    void apply(icontext_type& context, vertex_type& vertex,
               const gather_type& unused)
    {
        if (vertex.data().phi > 0) {
            vertex.data().phi -= msg;
            if (vertex.data().phi <= CURRENT_K) {
                just_deleted = true;
                add_phi(vertex);
                vertex.data().phi = 0;
            }
        }
    }

    edge_dir_type scatter_edges(icontext_type& context,
                                const vertex_type& vertex) const
    {
        return just_deleted ? graphlab::OUT_EDGES : graphlab::NO_EDGES;
    }

    void scatter(icontext_type& context,
                 const vertex_type& vertex,
                 edge_type& edge) const
    {
        if (edge.target().data().phi > 0 && edge.data() >= pi) {
            context.signal(edge.target(), 1);
        }
    }
};

void set_kcorent_initialvalues(graph_type::vertex_type& vdata)
{
    vdata.data().phi = vdata.num_out_edges(); // num_out_edges should equal to num_in_edges
    vdata.data().updated = true;
}

struct kcorent_writer {
    std::string save_vertex(const graph_type::vertex_type& vtx)
    {
        std::stringstream strm;
        strm << vtx.id() << "\t";
        std::vector<std::pair<int, int> > phis = vtx.data().phis;
        for (int i = 0; i < phis.size(); i++) {
            if (i != 0)
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
        int t, val;
        ssin >> other_vid >> t;
        for (int i = 0; i < t; i++)
            ssin >> val;
        graph.add_edge(vid, other_vid, t);
    }
    return true;
}

struct max_t : public graphlab::IS_POD_TYPE {
    int value;
    max_t()
        : value(0)
    {
    }
    max_t(int _value)
        : value(_value)
    {
    }
    max_t& operator+=(const max_t& other)
    {
        value = std::max(value, other.value);
        return *this;
    }
};

struct min_t : public graphlab::IS_POD_TYPE {
    int value;
    min_t()
        : value(inf)
    {
    }
    min_t(int _value)
        : value(_value)
    {
    }
    min_t& operator+=(const min_t& other)
    {
        value = std::min(value, other.value);
        return *this;
    }
};

max_t get_max_pi(const graph_type::edge_type& edge)
{
    return max_t(edge.data());
}
min_t get_min_pi(const graph_type::edge_type& edge)
{
    return min_t(edge.data());
}
min_t get_next_pi(const graph_type::edge_type& edge)
{
    if (edge.data() > pi)
        return min_t(edge.data());
    else
        return min_t();
}

typedef graphlab::synchronous_engine<kcorent> engine_type;

graphlab::empty signal_vertices_at_k(engine_type::icontext_type& ctx,
                                     const graph_type::vertex_type& vertex)
{
    if (vertex.data().phi > 0 && vertex.data().phi <= CURRENT_K) {
        ctx.signal(vertex, 0);
    }
    return graphlab::empty();
}

size_t count_active_vertices(const graph_type::vertex_type& vertex)
{
    return vertex.data().phi > 0;
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

    for (pi = graph.map_reduce_edges<min_t>(get_min_pi).value; pi <= max_pi; pi = graph.map_reduce_edges<min_t>(get_next_pi).value) {

        graphlab::omni_engine<InitDeg> engine(dc, graph, exec_type);
        engine.signal_all();
        engine.start();

        for (CURRENT_K = 0;; CURRENT_K++) {
            graphlab::omni_engine<kcorent> subengine(dc, graph, exec_type);
            subengine.map_reduce_vertices<graphlab::empty>(signal_vertices_at_k);
            subengine.start();

            size_t numv = graph.map_reduce_vertices<size_t>(count_active_vertices);
            dc.cout() << "current pi:  " << pi << " current_k: " << CURRENT_K << " numv: " << numv << std::endl;

            if (numv == 0)
                break;
        }
    }

    dc.cout() << "Finished Running engine in " << t.current_time()
              << " seconds." << std::endl;

    t.start();

    graph.save(output_file, kcorent_writer(), false, // set to true if each output file is to be gzipped
               true, // whether vertices are saved
               false); // whether edges are saved
    dc.cout() << "Dumping graph in " << t.current_time() << " seconds"
              << std::endl;

    graphlab::mpi_tools::finalize();
}
