#include "pregel_app_hashmin.h"
int main(int argc, char* argv[])
{
    init_workers();
    pregel_hashmin("/req/iusa", "/exp/hashmin", true);
    worker_finalize();
    return 0;
}
