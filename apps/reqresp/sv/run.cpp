#include "req_app_sv.h"

int main(int argc, char* argv[])
{
    init_workers();
    req_sv("/pullgel/livej", "/exp/sv_out");
    req_sv("/pullgel/friend", "/exp/sv_out");
    worker_finalize();
    return 0;
}
