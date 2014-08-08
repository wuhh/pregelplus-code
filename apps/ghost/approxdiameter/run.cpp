#include "ghost_app_approxdiameter.h"

int main(int argc, char* argv[])
{
    init_workers();
    set_ghost_threshold(126);
    ghost_approxdiameter("/pullgel/btc", "/exp/approxdiameter_ghost", true);
    worker_finalize();
    return 0;
}
