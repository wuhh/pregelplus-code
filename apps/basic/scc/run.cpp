#include "pregel_scc_owcty.h"
#include "pregel_scc_minlabel.h"
#include "pregel_scc_mingdecom.h"
#include "pregel_scc_multilabel.h"
#include "pregel_scc_multigdecom.h"
//vid \t color=-2 sccTag=0 in_num in1 in2 ... out_num out1 out2 ...

int main(int argc, char* argv[])
{
    init_workers();

    pregel_owcty("/input", "/scc/owcty");
    scc_multilabel("/scc/owcty", "/scc/minlabel");
    scc_multiGDecom("/scc/minlabel", "/scc/output");
    /*
    	pregel_owcty("/input", "/scc/owcty");
    	scc_minlabel("/scc/owcty", "/scc/minlabel");
    	scc_minGDecom("/scc/minlabel", "/scc/output");

    	pregel_owcty("/scc/output", "/scc/owcty");
    	scc_minlabel("/scc/owcty", "/scc/minlabel");
    	scc_minGDecom("/scc/minlabel", "/scc/output");
    */
    worker_finalize();
    return 0;
}
