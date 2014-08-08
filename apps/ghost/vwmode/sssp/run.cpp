#include "vwghost_app_sssp.h"

int main(int argc, char* argv[])
{
    init_workers();
    set_ghost_threshold(100);
    vwghost_sssp(0, "/sssp_vw", "/sssp_ghost", true);
    worker_finalize();
    return 0;
}
