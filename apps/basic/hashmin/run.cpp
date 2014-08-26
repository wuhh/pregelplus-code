#include "pregel_app_hashmin.h"
int main(int argc, char* argv[])
{
    init_workers();
    pregel_hashmin(argv[1], argv[2], false);
    worker_finalize();
    return 0;
}
