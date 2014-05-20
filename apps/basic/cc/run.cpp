#include "pregel_app_hashmin.h"
extern int T;
int main(int argc, char* argv[])
{
    T = atoi(argv[3]);
    init_workers();
    pregel_hashmin(argv[1], argv[2], true);
    worker_finalize();
    return 0;
}
