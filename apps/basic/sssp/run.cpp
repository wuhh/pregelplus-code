#include "pregel_app_sssp.h"
int main(int argc, char* argv[])
{
    init_workers();
    pregel_sssp(0, "/dis/friend", "/exp/usa/sssp_out", true);
    pregel_sssp(0, "/dis/friend", "/exp/usa/sssp_out", false);
    worker_finalize();
    return 0;
}
