#include "pregel_app_mst.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_mst("/pullgel/usa", "/exp/msf_out");
    worker_finalize();
    return 0;
}
