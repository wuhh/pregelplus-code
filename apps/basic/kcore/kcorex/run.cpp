#include "pregel_app_kcorex.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_kcorex("/pullgel/btc", "/exp/kcorex");
    worker_finalize();
    return 0;
}
