#include "vworker_app_sssp.h"
int main(int argc, char* argv[])
{
    init_workers();
    vworker_sssp("/req/euro", "/exp/usa/sssp_out", true);
    vworker_sssp("/req/usa", "/exp/usa/sssp_out", true);
    worker_finalize();
    return 0;
}
