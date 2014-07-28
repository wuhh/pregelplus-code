#include "ghost_app_pagerank.h"

int main(int argc, char* argv[])
{
    init_workers();
    set_ghost_threshold(1000);
    ghost_pagerank("/pullgel/webuk", "/exp/pagerank_ghost", false);

    worker_finalize();
    return 0;
}
