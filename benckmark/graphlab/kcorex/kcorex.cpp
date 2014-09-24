#include <vector>
#include <string>
#include <fstream>
#include <cassert>

#include <graphlab.hpp>

struct vertex_data : graphlab::IS_POD_TYPE {
    int phi;
    vertex_data()
        : phi(-1)
    {
    }
};

typedef graphlab::empty edge_data;
typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;

struct kcorex_gatherer {
    std::vector<int> P;

    kcorex_gatherer()
        : P()
    {
    }

    explicit kcorex_gatherer(int k)
        : P(1, k)
    {
    }

    kcorex_gatherer& operator+=(const kcorex_gatherer& other)
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

// gather type is graphlab::empty, then we use message model
class kcorex : public graphlab::ivertex_program<graph_type, kcorex_gatherer>,
               public graphlab::IS_POD_TYPE {

    int subfunc(vertex_type& vertex, const gather_type& total)
    {
        const std::vector<int>& P = total.P;
        std::vector<int> cd(vertex.data().phi + 2, 0);
        for (int i = 0; i < P.size(); i++) {
            cd[std::min(P[i], vertex.data().phi)]++;
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
    kcorex_gatherer gather(icontext_type& context, const vertex_type& vertex,
                           edge_type& edge) const
    {
        return kcorex_gatherer(edge.source().data().phi);
    }
    void apply(icontext_type& context, vertex_type& vertex,
               const gather_type& total)
    {
        if (vertex.num_out_edges() == 0)
            return;

        changed = false;
        int x = subfunc(vertex, total);
        if (x < vertex.data().phi) {
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

void set_kcorex_initialvalues(graph_type::vertex_type& vdata)
{
    vdata.data().phi = vdata.num_out_edges(); // num_out_edges should equal to num_in_edges
}

struct kcorex_writer {
    std::string save_vertex(const graph_type::vertex_type& vtx)
    {
        std::stringstream strm;
        strm << vtx.id() << "\t" << vtx.data().phi << "\n";
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
        ssin >> other_vid;
        graph.add_edge(vid, other_vid);
    }
    return true;
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

    graph.transform_vertices(set_kcorex_initialvalues);

    dc.cout() << "Loading graph in " << t.current_time() << " seconds"
              << std::endl;

    graphlab::omni_engine<kcorex> engine(dc, graph, exec_type);
    engine.signal_all();
    engine.start();

    dc.cout() << "Finished Running engine in " << engine.elapsed_seconds()
              << " seconds." << std::endl;

    t.start();

    graph.save(output_file, kcorex_writer(), false, // set to true if each output file is to be gzipped
               true, // whether vertices are saved
               false); // whether edges are saved
    dc.cout() << "Dumping graph in " << t.current_time() << " seconds"
              << std::endl;

    graphlab::mpi_tools::finalize();
}
