#include "pregel_app_sv.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_sv("/test", "/exp/sv");
    worker_finalize();
    return 0;
}
