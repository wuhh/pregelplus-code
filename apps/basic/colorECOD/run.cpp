#include "pregel_app_colorECOD.h"

int main(int argc, char* argv[])
{
    init_workers();
    pregel_colorECOD("/pullgel/iusa", "/exp/color_out");
    worker_finalize();
    return 0;
}
