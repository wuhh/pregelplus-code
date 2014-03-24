#include "pregel_app_sv.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_sv_report("/pullgel/iusa", "/exp/sv","/report/sv");
    worker_finalize();
    return 0;
}
