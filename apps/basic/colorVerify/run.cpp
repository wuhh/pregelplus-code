#include "pregel_app_colorverify.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_colorverify("/pullgel/livej", "/exp/color1");
    worker_finalize();
    return 0;
}
