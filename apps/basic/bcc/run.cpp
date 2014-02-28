#include "pregel_ppa_hashmin.h"
#include "pregel_app_sv.h"
#include "pregel_ppa_spantree.h"
#include "pregel_ppa_etour.h"
#include "pregel_ppa_hashminAndSpantree.h"
#include "pregel_ppa_listrank1.h"
#include "pregel_ppa_listrank2.h"
#include "pregel_ppa_merge.h"
#include "pregel_ppa_minmax.h"
#include "pregel_ppa_auxgraph.h"
#include "pregel_ppa_triphashmin.h"
#include "pregel_ppa_tripsv.h"
#include "pregel_ppa_case1mark.h"
#include "pregel_ppa_edgeback.h"

void print(const char* str)
{
    if (_my_rank == 0)
        cout << str << endl;
}

int main(int argc, char* argv[])
{
    init_workers();
    print(argv[1]);
    print("ppa_hashmin");
    ppa_hashmin(argv[1], "/ppa/hashmin_ppa", true);
    print("ppa_spantree");
    ppa_spantree("/ppa/hashmin_ppa", "/ppa/spantree_ppa", true);

    print("ppa_sv");
    pregel_sv(argv[1], "/ppa/sv"); //to test time only

    //ppa_hashmin_spantree("/btc", "/ppa/spantree_ppa", true);//////
    print("ppa_etour");
    ppa_etour("/ppa/spantree_ppa", "/ppa/etour_ppa");

    print("ppa_listrank1");
    ppa_listrank1("/ppa/etour_ppa", "/ppa/listrank1_ppa");

    print("ppa_listrank2");
    ppa_listrank2("/ppa/listrank1_ppa", "/ppa/listrank2_ppa");

    MultiInputParams param;
    param.add_input_path(argv[1]);
    param.add_input_path("/ppa/spantree_ppa");
    param.add_input_path("/ppa/listrank2_ppa");
    param.output_path = "/ppa/merge_ppa";
    param.force_write = true;
    param.native_dispatcher = false;
    print("ppa_merge");
    ppa_merge(param);

    print("ppa_minmax");
    ppa_minmax("/ppa/merge_ppa", "/ppa/minmax_ppa");
    print("ppa_auxgraph");
    ppa_auxgraph("/ppa/minmax_ppa", "/ppa/auxg_ppa");
    print("ppa_triphashmin");
    ppa_triphashmin("/ppa/auxg_ppa", "/ppa/triphashmin_ppa", true);

    print("ppa_tripsv");
    ppa_tripsv("/ppa/auxg_ppa", "/ppa/tripsv_ppa");
    MultiInputParams param1;
    param1.add_input_path("/ppa/minmax_ppa");
    param1.add_input_path("/ppa/triphashmin_ppa");
    param1.output_path = "/ppa/case1mark_ppa";
    param1.force_write = true;
    param1.native_dispatcher = false;
    print("ppa_case1");
    ppa_case1(param1); // non tree edge
    MultiInputParams param2;
    param2.add_input_path("/ppa/spantree_ppa");
    param2.add_input_path("/ppa/listrank2_ppa");
    param2.add_input_path("/ppa/case1mark_ppa");
    param2.output_path = "/ppa/edgeback_ppa";
    param2.force_write = true;
    param2.native_dispatcher = false;
    print("ppa_eback");
    ppa_eback(param2); // visualization
    worker_finalize();
    return 0;
}
