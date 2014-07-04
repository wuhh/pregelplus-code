#include "pregel_app_UGTODG.h"
int main(int argc, char* argv[])
{
    init_workers();
    pregel_UGTODG("/pullgel/livej", "/tmp/livej");
    worker_finalize();
    return 0;
}
