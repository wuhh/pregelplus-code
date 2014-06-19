#include "pregel_app_pagerank.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_pagerank("/pullgel/LJ", "/exp/taobao", true);
    worker_finalize();
    return 0;
}
