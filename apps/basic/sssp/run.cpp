#include "pregel_app_sssp.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_sssp(1, "/sssp", "/sssp_out", true);
    worker_finalize();
    return 0;
}
