#include "pregel_app_pagerank.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_pagerank(argv[1], argv[2], true);

    worker_finalize();
    return 0;
}
