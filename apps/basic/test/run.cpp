#include "pregel_app_hashmin.h"
int main(int argc, char* argv[])
{
    init_workers();
    tcpcomm_init();
    worker_finalize();
    return 0;
}
