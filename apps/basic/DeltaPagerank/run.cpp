#include "pregel_app_deltapagerank.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_deltapagerank("/pullgel/webbase", "/exp/pr", true);
    worker_finalize();
    return 0;
}
