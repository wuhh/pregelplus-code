#include "req_app_sv.h"

int main(int argc, char* argv[])
{
    init_workers();
    req_sv("/pullgel/btc", "/exp/sv_out");
    worker_finalize();
    return 0;
}
