#include "pregel_app_hashmin.h"
#include "utils/log.h"
int main(int argc, char* argv[])
{
    init_workers();
    if (_num_workers == 8 * 15 + 1) {
        logger("btc hashmin");
        pregel_hashmin("/pullgel/btc", "/exp/hashmin_out", true);
        logger("friend hashmin");
        pregel_hashmin("/pullgel/friend", "/exp/hashmin_out", true);
    }
    if (_num_workers == 4 * 15 + 1) {
        logger("iusa hashmin");
        pregel_hashmin("/pullgel/iusa", "/exp/hashmin_out", true);
    }
    worker_finalize();
    return 0;
}
