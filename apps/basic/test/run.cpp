#include "pregel_app_hashmin.h"
int main(int argc, char* argv[])
{
    init_workers();
    worker_finalize();
    return 0;
}
