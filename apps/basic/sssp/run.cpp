#include "pregel_app_sssp.h"
int main(int argc, char* argv[])
{
    init_workers();
    pregel_sssp(atoi(argv[2]),argv[1], "/exp/sssp", true);
    worker_finalize();
    return 0;
}
