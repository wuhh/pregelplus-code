#include "pregel_app_kcoreT2.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_kcoreT2("/temp/dblp", "/exp/kcorex");
    worker_finalize();
    return 0;
}
