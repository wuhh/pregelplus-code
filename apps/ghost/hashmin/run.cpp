#include "ghost_app_hashmin.h"

int main(int argc, char* argv[])
{
    init_workers();
    set_ghost_threshold(1);
    ghost_hashmin("/pullgel/iusa", "/hashmin_ghost", false);
    worker_finalize();
    return 0;
}
