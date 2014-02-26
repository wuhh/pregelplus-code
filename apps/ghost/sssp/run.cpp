#include "ghost_app_sssp.h"

int main(int argc, char* argv[])
{
    init_workers();
    set_ghost_threshold(100);
    ghost_sssp(1, "/sssp", "/sssp_ghost", true);
    worker_finalize();
    return 0;
}
