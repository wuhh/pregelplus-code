#include "pregel_app_kcore.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_kcore("/huan/dblp", "/huan/dblp_out");
    worker_finalize();
    return 0;
}
