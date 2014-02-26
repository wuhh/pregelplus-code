#include "vwghost_app_pagerank.h"

int main(int argc, char* argv[])
{
    init_workers();
    set_ghost_threshold(100);
    worker_finalize();
    return 0;
}
