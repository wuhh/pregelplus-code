#include "req_app_sv.h"

int main(int argc, char* argv[])
{
    init_workers();
    req_sv_report("/pullgel/iusa", "/exp/sv_out","/report/req");
    worker_finalize();
    return 0;
}
