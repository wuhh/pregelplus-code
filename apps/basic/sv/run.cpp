#include "pregel_app_sv.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_sv(argv[1], "/exp/sv");
    worker_finalize();
    return 0;
}
