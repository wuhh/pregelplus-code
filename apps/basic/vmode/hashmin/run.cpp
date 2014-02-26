#include "vworker_app_hashmin.h"

int main(int argc, char* argv[])
{
    init_workers();
    vworker_hashmin("/pagerank_vw", "/hashmin_out", true);
    worker_finalize();
    return 0;
}
