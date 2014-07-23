#include "pregel_app_fieldbcast.h"

int main(int argc, char* argv[])
{
    init_workers();
    bool directed = true;
    pregel_fieldbcast("/pullgel/twitter", "/exp/webbase", directed);
    worker_finalize();
    return 0;
}
