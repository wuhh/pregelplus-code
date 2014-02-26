#include "vworker_app_pagerank.h"

int main(int argc, char* argv[])
{
    init_workers();
    vworker_pagerank("/pagerank_vw", "/pagerank_out", true);
    worker_finalize();
    return 0;
}
