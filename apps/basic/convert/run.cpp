#include "pregel_app_convert.h"
int main(int argc, char* argv[])
{
    init_workers();
    pregel_convert(argv[1], argv[2]);
    worker_finalize();
    return 0;
}
