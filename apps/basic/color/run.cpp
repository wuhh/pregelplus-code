#include "pregel_app_color.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_color(argv[1], "/exp/color_out");
    worker_finalize();
    return 0;
}
