#include "pregel_app_kcore.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_kcore("/temp/dblp", "/exp/kcorex");
    worker_finalize();
    return 0;
}
