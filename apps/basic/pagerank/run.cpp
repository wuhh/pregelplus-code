#include "pregel_app_pagerank.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_pagerank("/pullgel/webuk", "/exp/pagerank", true);
    worker_finalize();
    return 0;
}
