#include "pregel_app_bipartitematching.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_bipartitematching("/pullgel/blivej", "/exp/bmm");
    worker_finalize();
    return 0;
}
