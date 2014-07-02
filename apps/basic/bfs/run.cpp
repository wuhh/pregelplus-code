#include "pregel_app_bfs.h"
int main(int argc, char* argv[])
{
    init_workers();
    pregel_bfs("/pullgel/webuk", "/exp/hashmin", true, atoi(argv[1]));
    worker_finalize();
    return 0;
}
