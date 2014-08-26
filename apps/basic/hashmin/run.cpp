#include "pregel_app_hashmin.h"
int main(int argc, char* argv[])
{
    init_workers();
    pregel_hashmin("/pullgel/iusa", "/exp/test", true);
    worker_finalize();
    return 0;
}
