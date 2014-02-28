#include "pregel_app_reach.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_reach("/pullgel/webuk", "/exp/reach", true);
    worker_finalize();
    return 0;
}
