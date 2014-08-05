#include "req_app_mst.h"

int main(int argc, char* argv[])
{
    init_workers();
    req_mst("/pullgel/btc", "/exp/msf_out2");
    worker_finalize();
    return 0;
}
