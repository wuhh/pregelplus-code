#include "pregel_app_kcore.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_kcore(argv[1], argv[2]);
    worker_finalize();
    return 0;
}
