#include "pregel_app_hashminSP.h"
int main(int argc, char* argv[])
{
    init_workers();
    pregel_hashminSP("/pullgel/btc", "/exp/cc", true);
    worker_finalize();
    return 0;
}
