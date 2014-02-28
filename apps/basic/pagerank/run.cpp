//#include "pregel_app_pagerank.h"
#include "vworker_app_pagerank.h"

int main(int argc, char* argv[])
{
    init_workers();
    vworker_pagerank("/ldg/webuk", "/exp/pagerank_out", true);
    worker_finalize();
    return 0;
}
