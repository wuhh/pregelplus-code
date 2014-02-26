#include "pregel_app_inoverflow.h"
#include "tera_intpair.h"
#include "req_bal_fieldbcast.h"
#include "pregel_app_outoverflow.h"
#include "req_bal_fieldbcast2.h"
#include "to_undirected.h"

void print(const char* str)
{
    if (_my_rank == 0)
        cout << str << endl;
}

int main(int argc, char* argv[])
{
    init_workers();
    print(argv[1]);
    print("pregel_inoverflow");
    pregel_inoverflow(argv[1], "/balance/indeg", 100, false);
    print("sort_intpair");
    sort_intpair("/balance/indeg", "/balance/indeg_prefixed", 0.001, true); //in real graph, 0.5->0.001
    print("bal_fieldbcast");
    bal_fieldbcast("/balance/indeg_prefixed", "/balance/indeg_done");
    print("pregel_outoverflow");
    pregel_outoverflow("/balance/indeg_done", "/balance/outdeg", 100);
    print("sort_intpair");
    sort_intpair("/balance/outdeg", "/balance/outdeg_prefixed", 0.001, true); //in real graph, 0.5->0.001
    print("bal_fieldbcast2");
    bal_fieldbcast2("/balance/outdeg_prefixed", "/balance/outdeg_done");
    //    print("toUG");
    //    toUG("/balance/outdeg_done", "/balance/UG");
    worker_finalize();
    return 0;
}
