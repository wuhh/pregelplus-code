#include "req_app_fieldbcast.h"

int main(int argc, char* argv[])
{
    init_workers();
    req_fieldbcast("/pullgel/livej", "/exp/livej");
    worker_finalize();
    return 0;
}
