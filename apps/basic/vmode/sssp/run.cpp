#include "vworker_app_sssp.h"

int main(int argc, char* argv[])
{
    init_workers();
    vworker_sssp(1, "/sssp_vw", "/sssp_out", true);
    worker_finalize();
    return 0;
}
