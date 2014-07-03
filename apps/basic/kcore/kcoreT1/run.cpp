#include "pregel_app_kcoreT1.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_kcoreT1("/temp/dblp", "/exp/kcorex");
    worker_finalize();
    return 0;
}
