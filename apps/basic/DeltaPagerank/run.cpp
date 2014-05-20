#include "pregel_app_deltapagerank.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_deltapagerank(argv[1], argv[2], true);
    worker_finalize();
    return 0;
}
