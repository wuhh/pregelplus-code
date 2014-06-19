#include "pregel_app_DGTOLDG.h"
int main(int argc, char* argv[])
{
    init_workers();
    vworker_DGTOLDG("/req/livej", "/ldgreach/livej");
    worker_finalize();
    return 0;
}
