#include "pregel_app_sssp.h"
#include "utils/log.h"
int main(int argc, char* argv[])
{
    init_workers();
    char buf[100];
    sprintf(buf, "_num_workers %d", _num_workers);
    logger(buf);
    pregel_sssp(0, "/pullgel/euro", "/exp/sssp_out", true);
    worker_finalize();
    return 0;
}
