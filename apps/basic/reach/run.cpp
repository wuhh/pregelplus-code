//#include "pregel_app_reach.h"
#include "vworker_app_reach.h"

int main(int argc, char* argv[])
{
    init_workers();
    vworker_reach("/ldgreach/livej", "/exp/reach", true);

    worker_finalize();
    return 0;
}
