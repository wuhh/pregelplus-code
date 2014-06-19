#include <vector>
#include <string>
#include <fstream>

#include <graphlab.hpp>
#include <cassert>

typedef double pagerank_type;

const pagerank_type EPS = 0.01;
int ROUND;
struct vertex_data : graphlab::IS_POD_TYPE {
    pagerank_type pagerank;
    vertex_data(pagerank_type pagerank = 1.0)
        : pagerank(pagerank)
    {
    }
};

typedef graphlab::empty edge_data;

typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;

struct sum_pagerank_type : graphlab::IS_POD_TYPE {
    pagerank_type pagerank;
    sum_pagerank_type(pagerank_type pagerank = 0)
        : pagerank(pagerank)
    {
    }
    sum_pagerank_type& operator+=(const sum_pagerank_type& other)
    {
        pagerank += other.pagerank;
        return *this;
    }
};

// gather type is graphlab::empty, then we use message model
class pagerank : public graphlab::ivertex_program<graph_type, graphlab::empty,
                                                  sum_pagerank_type>,
                 public graphlab::IS_POD_TYPE {
    pagerank_type sum_pagerank;

public:
    pagerank()
        : sum_pagerank(0)
    {
    }
    void init(icontext_type& context, const vertex_type& vertex,
              const sum_pagerank_type& msg)
    {
        sum_pagerank = msg.pagerank;
    }

    edge_dir_type gather_edges(icontext_type& context,
                               const vertex_type& vertex) const
    {
        return graphlab::NO_EDGES;
    }

    void apply(icontext_type& context, vertex_type& vertex,
               const graphlab::empty& empty)
    {
        if (context.iteration() < ROUND) {
            context.signal(vertex);
        }

        if (context.iteration() == 0) {
            return;
        }

        pagerank_type new_pagerank = 0.15 + 0.85 * sum_pagerank;

        vertex.data().pagerank = new_pagerank;
    }

    edge_dir_type scatter_edges(icontext_type& context,
                                const vertex_type& vertex) const
    {
        if (context.iteration() < ROUND)
            return graphlab::OUT_EDGES;
        else
            return graphlab::NO_EDGES;
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
                 edge_type& edge) const
    {
        const vertex_type other = edge.target();
        pagerank_type value = vertex.data().pagerank / vertex.num_out_edges();
        assert(other.id() != vertex.id());
        const sum_pagerank_type msg(value);
        context.signal(other, msg);
    }
};

struct pagerank_writer {
    std::string save_vertex(const graph_type::vertex_type& vtx)
    {
        std::stringstream strm;
        strm << vtx.id() << "\t" << vtx.data().pagerank << "\n";
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
    graph.add_vertex(vid);
    int out_nb;
    ssin >> out_nb;
    if (out_nb == 0)
        graph.add_vertex(vid);

    while (out_nb--) {
        graphlab::vertex_id_type other_vid;
        ssin >> other_vid;
        if (vid != other_vid)
            graph.add_edge(vid, other_vid);
    }
    return true;
}

double map_rank(const graph_type::vertex_type& v)
{
    return v.data().pagerank;
}

int main(int argc, char** argv)
{
    graphlab::mpi_tools::init(argc, argv);

    char* input_file = "hdfs://master:9000/pullgel/twitter";
    char* output_file = "hdfs://master:9000/exp/twitter";
    ROUND = 10;
    graphlab::distributed_control dc;
    global_logger().set_log_level(LOG_INFO);

    graphlab::timer t;
    t.start();
    graph_type graph(dc);
    graph.load(input_file, line_parser);
    graph.finalize();

    dc.cout() << "Loading graph in " << t.current_time() << " seconds" << std::endl;
    std::string exec_type = "synchronous";

    graphlab::omni_engine<pagerank> engine(dc, graph, exec_type);

    engine.signal_all();
    engine.start();

    dc.cout() << "Finished Running engine in " << engine.elapsed_seconds()
              << " seconds." << std::endl;

    const double total_rank = graph.map_reduce_vertices<double>(map_rank);
    std::cout << "Total rank: " << total_rank << std::endl;

    t.start();

    graph.save(output_file, pagerank_writer(), false, // set to true if each output file is to be gzipped
               true, // whether vertices are saved
               false); // whether edges are saved
    dc.cout() << "Dumping graph in " << t.current_time() << " seconds" << std::endl;

    graphlab::mpi_tools::finalize();
}
