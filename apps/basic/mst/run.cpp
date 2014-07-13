#include "pregel_app_mst.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_mst("/zjs/graph1.txt", "/exp");
    worker_finalize();
    return 0;
}
