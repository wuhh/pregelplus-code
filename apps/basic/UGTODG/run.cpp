#include "pregel_app_UGTODG.h"
int main(int argc, char* argv[])
{
    init_workers();
    pregel_UGTODG(argv[1], argv[2]);
    worker_finalize();
    return 0;
}
