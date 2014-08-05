#include "pregel_app_randomwalk.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_randomwalk("/pullgel/livej", "/exp/pagerank");
    worker_finalize();
    return 0;
}
