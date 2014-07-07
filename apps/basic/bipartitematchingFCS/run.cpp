#include "pregel_app_bipartitematchingFCS.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_bipartitematchingFCS("/pullgel/blivej", "/exp/bmm");
    worker_finalize();
    return 0;
}
