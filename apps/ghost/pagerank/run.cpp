#include "ghost_app_pagerank.h"

int main(int argc, char* argv[])
{
    init_workers();
    set_ghost_threshold(1);
    ghost_pagerank("/pullgel/iusa", "/exp/pagerank_ghost", true);

    worker_finalize();
    return 0;
}
