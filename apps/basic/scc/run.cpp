#include "pregel_scc_owcty.h"
#include "pregel_scc_minlabel.h"
#include "pregel_scc_mingdecom.h"
#include "pregel_scc_multilabel.h"
#include "pregel_scc_multigdecom.h"
#include "pregel_scc_tosccgraphformat.h"
//vid \t color=-2 sccTag=0 in_num in1 in2 ... out_num out1 out2 ...

void multi_twitter()
{
    if (_my_rank == 0)
        cout << "First Round" << endl;

    pregel_owcty("/scc/twitter", "/sccmulexp/twitter/owcty");
    scc_multilabel("/sccmulexp/twitter/owcty", "/sccmulexp/twitter/minlabel");
    scc_multiGDecom("/sccmulexp/twitter/minlabel", "/sccmulexp/twitter/output1");

    if (_my_rank == 0)
        cout << "Second Round" << endl;

    pregel_owcty("/sccmulexp/twitter/output1", "/sccmulexp/twitter/owcty");
    scc_multilabel("/sccmulexp/twitter/owcty", "/sccmulexp/twitter/minlabel");
    scc_multiGDecom("/sccmulexp/twitter/minlabel", "/sccmulexp/twitter/output2");

    if (_my_rank == 0)
        cout << "Third Round" << endl;

    pregel_owcty("/sccmulexp/twitter/output2", "/sccmulexp/twitter/owcty");
    scc_multilabel("/sccmulexp/twitter/owcty", "/sccmulexp/twitter/minlabel");
    scc_multiGDecom("/sccmulexp/twitter/minlabel", "/sccmulexp/twitter/output3");

    if (_my_rank == 0)
        cout << "Forth Round" << endl;

    pregel_owcty("/sccmulexp/twitter/output3", "/sccmulexp/twitter/owcty");
    scc_multilabel("/sccmulexp/twitter/owcty", "/sccmulexp/twitter/minlabel");
    scc_multiGDecom("/sccmulexp/twitter/minlabel", "/sccmulexp/twitter/output4");
    if (_my_rank == 0)
        cout << "Fifth Round" << endl;

    pregel_owcty("/sccmulexp/twitter/output4", "/sccmulexp/twitter/owcty");
    scc_multilabel("/sccmulexp/twitter/owcty", "/sccmulexp/twitter/minlabel");
    scc_multiGDecom("/sccmulexp/twitter/minlabel", "/sccmulexp/twitter/output5");
}

void multi_twitter_balance()
{
    if (_my_rank == 0)
        cout << "First Round" << endl;

    pregel_owcty("/scc/twitter_balance", "/sccmulexp/twitter_balance/owcty");
    scc_multilabel("/sccmulexp/twitter_balance/owcty", "/sccmulexp/twitter_balance/minlabel");
    scc_multiGDecom("/sccmulexp/twitter_balance/minlabel", "/sccmulexp/twitter_balance/output1");

    if (_my_rank == 0)
        cout << "Second Round" << endl;

    pregel_owcty("/sccmulexp/twitter_balance/output1", "/sccmulexp/twitter_balance/owcty");
    scc_multilabel("/sccmulexp/twitter_balance/owcty", "/sccmulexp/twitter_balance/minlabel");
    scc_multiGDecom("/sccmulexp/twitter_balance/minlabel", "/sccmulexp/twitter_balance/output2");

    if (_my_rank == 0)
        cout << "Third Round" << endl;

    pregel_owcty("/sccmulexp/twitter_balance/output2", "/sccmulexp/twitter_balance/owcty");
    scc_multilabel("/sccmulexp/twitter_balance/owcty", "/sccmulexp/twitter_balance/minlabel");
    scc_multiGDecom("/sccmulexp/twitter_balance/minlabel", "/sccmulexp/twitter_balance/output3");
}

void multi_LJ()
{

    if (_my_rank == 0)
        cout << "First Round" << endl;

    pregel_owcty("/scc/LJ", "/sccmulexp/LJ/owcty");
    scc_multilabel("/sccmulexp/LJ/owcty", "/sccmulexp/LJ/minlabel");
    scc_multiGDecom("/sccmulexp/LJ/minlabel", "/sccmulexp/LJ/output1");

    if (_my_rank == 0)
        cout << "Second Round" << endl;

    pregel_owcty("/sccmulexp/LJ/output1", "/sccmulexp/LJ/owcty");
    scc_multilabel("/sccmulexp/LJ/owcty", "/sccmulexp/LJ/minlabel");
    scc_multiGDecom("/sccmulexp/LJ/minlabel", "/sccmulexp/LJ/output2");

    /*
    if(_my_rank == 0)
        cout << "Third Round" << endl;

    pregel_owcty("/sccmulexp/LJ/output2", "/sccmulexp/LJ/owcty");
    scc_multilabel("/sccmulexp/LJ/owcty", "/sccmulexp/LJ/minlabel");
    scc_multiGDecom("/sccmulexp/LJ/minlabel", "/sccmulexp/LJ/output3");

    if(_my_rank == 0)
        cout << "Forth Round" << endl;

    pregel_owcty("/sccmulexp/LJ/output3", "/sccmulexp/LJ/owcty");
    scc_multilabel("/sccmulexp/LJ/owcty", "/sccmulexp/LJ/minlabel");
    scc_multiGDecom("/sccmulexp/LJ/minlabel", "/sccmulexp/LJ/output4");
    if(_my_rank == 0)
        cout << "Fifth Round" << endl;

    pregel_owcty("/sccmulexp/LJ/output4", "/sccmulexp/LJ/owcty");
    scc_multilabel("/sccmulexp/LJ/owcty", "/sccmulexp/LJ/minlabel");
    scc_multiGDecom("/sccmulexp/LJ/minlabel", "/sccmulexp/LJ/output5");
    if(_my_rank == 0)
        cout << "Sixth Round" << endl;

    pregel_owcty("/sccmulexp/LJ/output5", "/sccmulexp/LJ/owcty");
    scc_multilabel("/sccmulexp/LJ/owcty", "/sccmulexp/LJ/minlabel");
    scc_multiGDecom("/sccmulexp/LJ/minlabel", "/sccmulexp/LJ/output6");
*/
}

void multi_LJ_balance()
{
    if (_my_rank == 0)
        cout << "First Round" << endl;

    pregel_owcty("/scc/LJ_balance", "/sccmulexp/LJ_balance/owcty");
    scc_multilabel("/sccmulexp/LJ_balance/owcty", "/sccmulexp/LJ_balance/minlabel");
    scc_multiGDecom("/sccmulexp/LJ_balance/minlabel", "/sccmulexp/LJ_balance/output1");

    if (_my_rank == 0)
        cout << "Second Round" << endl;

    pregel_owcty("/sccmulexp/LJ_balance/output1", "/sccmulexp/LJ_balance/owcty");
    scc_multilabel("/sccmulexp/LJ_balance/owcty", "/sccmulexp/LJ_balance/minlabel");
    scc_multiGDecom("/sccmulexp/LJ_balance/minlabel", "/sccmulexp/LJ_balance/output2");

    if (_my_rank == 0)
        cout << "Third Round" << endl;

    pregel_owcty("/sccmulexp/LJ_balance/output2", "/sccmulexp/LJ_balance/owcty");
    scc_multilabel("/sccmulexp/LJ_balance/owcty", "/sccmulexp/LJ_balance/minlabel");
    scc_multiGDecom("/sccmulexp/LJ_balance/minlabel", "/sccmulexp/LJ_balance/output3");
}
void twitter()
{
    if (_my_rank == 0)
        cout << "First Round" << endl;

    pregel_owcty("/scc/twitter", "/sccexp/twitter/owcty");
    scc_minlabel("/sccexp/twitter/owcty", "/sccexp/twitter/minlabel");
    scc_minGDecom("/sccexp/twitter/minlabel", "/sccexp/twitter/output1");

    if (_my_rank == 0)
        cout << "Second Round" << endl;

    pregel_owcty("/sccexp/twitter/output1", "/sccexp/twitter/owcty");
    scc_minlabel("/sccexp/twitter/owcty", "/sccexp/twitter/minlabel");
    scc_minGDecom("/sccexp/twitter/minlabel", "/sccexp/twitter/output2");

    /*
    if(_my_rank == 0)
        cout << "Third Round" << endl;

    pregel_owcty("/sccexp/twitter/output2", "/sccexp/twitter/owcty");
    scc_minlabel("/sccexp/twitter/owcty", "/sccexp/twitter/minlabel");
    scc_minGDecom("/sccexp/twitter/minlabel", "/sccexp/twitter/output3");

    if(_my_rank == 0)
        cout << "Forth Round" << endl;

    pregel_owcty("/sccexp/twitter/output3", "/sccexp/twitter/owcty");
    scc_minlabel("/sccexp/twitter/owcty", "/sccexp/twitter/minlabel");
    scc_minGDecom("/sccexp/twitter/minlabel", "/sccexp/twitter/output4");
    */
}

void twitter_balance()
{
    if (_my_rank == 0)
        cout << "First Round" << endl;

    pregel_owcty("/scc/twitter_balance", "/sccexp/twitter_balance/owcty");
    scc_minlabel("/sccexp/twitter_balance/owcty", "/sccexp/twitter_balance/minlabel");
    scc_minGDecom("/sccexp/twitter_balance/minlabel", "/sccexp/twitter_balance/output1");

    if (_my_rank == 0)
        cout << "Second Round" << endl;

    pregel_owcty("/sccexp/twitter_balance/output1", "/sccexp/twitter_balance/owcty");
    scc_minlabel("/sccexp/twitter_balance/owcty", "/sccexp/twitter_balance/minlabel");
    scc_minGDecom("/sccexp/twitter_balance/minlabel", "/sccexp/twitter_balance/output2");
    /*
    if(_my_rank == 0)
        cout << "Third Round" << endl;

    pregel_owcty("/sccexp/twitter_balance/output2", "/sccexp/twitter_balance/owcty");
    scc_minlabel("/sccexp/twitter_balance/owcty", "/sccexp/twitter_balance/minlabel");
    scc_minGDecom("/sccexp/twitter_balance/minlabel", "/sccexp/twitter_balance/output3");

    if(_my_rank == 0)
        cout << "Forth Round" << endl;

    pregel_owcty("/sccexp/twitter_balance/output3", "/sccexp/twitter_balance/owcty");
    scc_minlabel("/sccexp/twitter_balance/owcty", "/sccexp/twitter_balance/minlabel");
    scc_minGDecom("/sccexp/twitter_balance/minlabel", "/sccexp/twitter_balance/output4");
    */
}

void LJ()
{
    if (_my_rank == 0)
        cout << "First Round" << endl;

    pregel_owcty("/scc/LJ", "/sccexp/LJ/owcty");
    scc_minlabel("/sccexp/LJ/owcty", "/sccexp/LJ/minlabel");
    scc_minGDecom("/sccexp/LJ/minlabel", "/sccexp/LJ/output1");

    if (_my_rank == 0)
        cout << "Second Round" << endl;

    pregel_owcty("/sccexp/LJ/output1", "/sccexp/LJ/owcty");
    scc_minlabel("/sccexp/LJ/owcty", "/sccexp/LJ/minlabel");
    scc_minGDecom("/sccexp/LJ/minlabel", "/sccexp/LJ/output2");
    /*
       if(_my_rank == 0)
       cout << "Third Round" << endl;

       pregel_owcty("/sccexp/LJ/output2", "/sccexp/LJ/owcty");
       scc_minlabel("/sccexp/LJ/owcty", "/sccexp/LJ/minlabel");
       scc_minGDecom("/sccexp/LJ/minlabel", "/sccexp/LJ/output3");

       if(_my_rank == 0)
       cout << "Forth Round" << endl;

       pregel_owcty("/sccexp/LJ/output3", "/sccexp/LJ/owcty");
       scc_minlabel("/sccexp/LJ/owcty", "/sccexp/LJ/minlabel");
       scc_minGDecom("/sccexp/LJ/minlabel", "/sccexp/LJ/output4");

       if(_my_rank == 0)
       cout << "Fifth Round" << endl;

       pregel_owcty("/sccexp/LJ/output4", "/sccexp/LJ/owcty");
       scc_minlabel("/sccexp/LJ/owcty", "/sccexp/LJ/minlabel");
       scc_minGDecom("/sccexp/LJ/minlabel", "/sccexp/LJ/output5");

       if(_my_rank == 0)
       cout << "Sixth Round" << endl;

       pregel_owcty("/sccexp/LJ/output5", "/sccexp/LJ/owcty");
       scc_minlabel("/sccexp/LJ/owcty", "/sccexp/LJ/minlabel");
       scc_minGDecom("/sccexp/LJ/minlabel", "/sccexp/LJ/output6");

       if(_my_rank == 0)
       cout << "Seventh Round" << endl;

       pregel_owcty("/sccexp/LJ/output6", "/sccexp/LJ/owcty");
       scc_minlabel("/sccexp/LJ/owcty", "/sccexp/LJ/minlabel");
       scc_minGDecom("/sccexp/LJ/minlabel", "/sccexp/LJ/output7");
       */
}

void LJ_balance()
{
    if (_my_rank == 0)
        cout << "First Round" << endl;

    pregel_owcty("/scc/LJ_balance", "/sccexp/LJ_balance/owcty");
    scc_minlabel("/sccexp/LJ_balance/owcty", "/sccexp/LJ_balance/minlabel");
    scc_minGDecom("/sccexp/LJ_balance/minlabel", "/sccexp/LJ_balance/output1");

    if (_my_rank == 0)
        cout << "Second Round" << endl;

    pregel_owcty("/sccexp/LJ_balance/output1", "/sccexp/LJ_balance/owcty");
    scc_minlabel("/sccexp/LJ_balance/owcty", "/sccexp/LJ_balance/minlabel");
    scc_minGDecom("/sccexp/LJ_balance/minlabel", "/sccexp/LJ_balance/output2");
    /*
       if(_my_rank == 0)
       cout << "Third Round" << endl;

       pregel_owcty("/sccexp/LJ_balance/output2", "/sccexp/LJ_balance/owcty");
       scc_minlabel("/sccexp/LJ_balance/owcty", "/sccexp/LJ_balance/minlabel");
       scc_minGDecom("/sccexp/LJ_balance/minlabel", "/sccexp/LJ_balance/output3");

       if(_my_rank == 0)
       cout << "Forth Round" << endl;

       pregel_owcty("/sccexp/LJ_balance/output3", "/sccexp/LJ_balance/owcty");
       scc_minlabel("/sccexp/LJ_balance/owcty", "/sccexp/LJ_balance/minlabel");
       scc_minGDecom("/sccexp/LJ_balance/minlabel", "/sccexp/LJ_balance/output4");

       if(_my_rank == 0)
       cout << "Fifth Round" << endl;

       pregel_owcty("/sccexp/LJ_balance/output4", "/sccexp/LJ_balance/owcty");
       scc_minlabel("/sccexp/LJ_balance/owcty", "/sccexp/LJ_balance/minlabel");
       scc_minGDecom("/sccexp/LJ_balance/minlabel", "/sccexp/LJ_balance/output5");

       if(_my_rank == 0)
       cout << "Sixth Round" << endl;

       pregel_owcty("/sccexp/LJ_balance/output5", "/sccexp/LJ_balance/owcty");
       scc_minlabel("/sccexp/LJ_balance/owcty", "/sccexp/LJ_balance/minlabel");
       scc_minGDecom("/sccexp/LJ_balance/minlabel", "/sccexp/LJ_balance/output6");

       if(_my_rank == 0)
       cout << "Seventh Round" << endl;

       pregel_owcty("/sccexp/LJ_balance/output6", "/sccexp/LJ_balance/owcty");
       scc_minlabel("/sccexp/LJ_balance/owcty", "/sccexp/LJ_balance/minlabel");
       scc_minGDecom("/sccexp/LJ_balance/minlabel", "/sccexp/LJ_balance/output7");
       */
}

int main(int argc, char* argv[])
{
    init_workers();

    if(_my_rank == 0)
        cout << "Twitter SCC" << endl;
    twitter_balance();




    /*
       if(_my_rank == 0)
       cout << "Twitter SCC" << endl;
       multi_twitter();

       if(_my_rank == 0)
       cout << "Twitter Balance SCC" << endl;
       multi_twitter_balance();
       */
    /*
       if (_my_rank == 0)
       cout << "LJ SCC" << endl;
       multi_LJ();
       */
    /*
       if(_my_rank == 0)
       cout << "LJ Balance SCC" << endl;
       multi_LJ_balance();
       */

    //pregel_tosccgraphformat("/pullgel/twitter","/scc/twitter");
    //pregel_tosccgraphformat("/pullgel/twitter_balance","/scc/twitter_balance");
    //pregel_tosccgraphformat("/pullgel/LJ","/scc/LJ");
    //pregel_tosccgraphformat("/pullgel/LJ_balance","/scc/LJ_balance");

    worker_finalize();
    return 0;
}
