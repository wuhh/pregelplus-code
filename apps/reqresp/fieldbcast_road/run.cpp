#include "req_app_fieldbcast.h"

int main(int argc, char* argv[])
{
    init_workers();
    req_fieldbcast("/ldg/euro", "/req/euro");
    req_fieldbcast("/ldg/usa", "/req/usa");
    worker_finalize();
    return 0;
}
