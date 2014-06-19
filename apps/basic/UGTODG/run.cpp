#include "pregel_app_UGTODG.h"
int main(int argc, char* argv[])
{
    init_workers();
    pregel_UGTODG("/pullgel/livej", "/reach/livej");
    worker_finalize();
    return 0;
}
