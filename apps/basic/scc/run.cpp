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

void patent()
{
    if (_my_rank == 0)
        cout << "First Round" << endl;

    pregel_owcty("/scc/patent", "/sccexp/patent/owcty");
    scc_minlabel("/sccexp/patent/owcty", "/sccexp/patent/minlabel");
    scc_minGDecom("/sccexp/patent/minlabel", "/sccexp/patent/output1");
}
void multi_patent()
{

    if (_my_rank == 0)
        cout << "First Round" << endl;

    pregel_owcty("/scc/patent", "/sccmulexp/patent/owcty");
    scc_multilabel("/sccmulexp/patent/owcty", "/sccmulexp/patent/minlabel");
    scc_multiGDecom("/sccmulexp/patent/minlabel", "/sccmulexp/patent/output1");

    if (_my_rank == 0)
        cout << "Second Round" << endl;

    pregel_owcty("/sccmulexp/patent/output1", "/sccmulexp/patent/owcty");
    scc_multilabel("/sccmulexp/patent/owcty", "/sccmulexp/patent/minlabel");
    scc_multiGDecom("/sccmulexp/patent/minlabel", "/sccmulexp/patent/output2");

    if (_my_rank == 0)
        cout << "Third Round" << endl;

    pregel_owcty("/sccmulexp/patent/output2", "/sccmulexp/patent/owcty");
    scc_multilabel("/sccmulexp/patent/owcty", "/sccmulexp/patent/minlabel");
    scc_multiGDecom("/sccmulexp/patent/minlabel", "/sccmulexp/patent/output3");
}
void pokec()
{
    if (_my_rank == 0)
        cout << "First Round" << endl;

    pregel_owcty("/scc/pokec", "/sccexp/pokec/owcty");
    scc_minlabel("/sccexp/pokec/owcty", "/sccexp/pokec/minlabel");
    scc_minGDecom("/sccexp/pokec/minlabel", "/sccexp/pokec/output1");
}
void multi_pokec()
{

    if (_my_rank == 0)
        cout << "First Round" << endl;

    pregel_owcty("/scc/pokec", "/sccmulexp/pokec/owcty");
    scc_multilabel("/sccmulexp/pokec/owcty", "/sccmulexp/pokec/minlabel");
    scc_multiGDecom("/sccmulexp/pokec/minlabel", "/sccmulexp/pokec/output1");

    if (_my_rank == 0)
        cout << "Second Round" << endl;

    pregel_owcty("/sccmulexp/pokec/output1", "/sccmulexp/pokec/owcty");
    scc_multilabel("/sccmulexp/pokec/owcty", "/sccmulexp/pokec/minlabel");
    scc_multiGDecom("/sccmulexp/pokec/minlabel", "/sccmulexp/pokec/output2");

    if (_my_rank == 0)
        cout << "Third Round" << endl;

    pregel_owcty("/sccmulexp/pokec/output2", "/sccmulexp/pokec/owcty");
    scc_multilabel("/sccmulexp/pokec/owcty", "/sccmulexp/pokec/minlabel");
    scc_multiGDecom("/sccmulexp/pokec/minlabel", "/sccmulexp/pokec/output3");
}

void flickr()
{
    if (_my_rank == 0)
        cout << "First Round" << endl;

    pregel_owcty("/scc/flickr", "/sccexp/flickr/owcty");
    scc_minlabel("/sccexp/flickr/owcty", "/sccexp/flickr/minlabel");
    scc_minGDecom("/sccexp/flickr/minlabel", "/sccexp/flickr/output1");

    if (_my_rank == 0)
        cout << "Second Round" << endl;

    pregel_owcty("/sccexp/flickr/output1", "/sccexp/flickr/owcty");
    scc_minlabel("/sccexp/flickr/owcty", "/sccexp/flickr/minlabel");
    scc_minGDecom("/sccexp/flickr/minlabel", "/sccexp/flickr/output2");
}

void multi_flickr()
{

    if (_my_rank == 0)
        cout << "First Round" << endl;

    pregel_owcty("/scc/flickr", "/sccmulexp/flickr/owcty");
    scc_multilabel("/sccmulexp/flickr/owcty", "/sccmulexp/flickr/minlabel");
    scc_multiGDecom("/sccmulexp/flickr/minlabel", "/sccmulexp/flickr/output1");

    if (_my_rank == 0)
        cout << "Second Round" << endl;

    pregel_owcty("/sccmulexp/flickr/output1", "/sccmulexp/flickr/owcty");
    scc_multilabel("/sccmulexp/flickr/owcty", "/sccmulexp/flickr/minlabel");
    scc_multiGDecom("/sccmulexp/flickr/minlabel", "/sccmulexp/flickr/output2");

    if (_my_rank == 0)
        cout << "Third Round" << endl;

    pregel_owcty("/sccmulexp/flickr/output2", "/sccmulexp/flickr/owcty");
    scc_multilabel("/sccmulexp/flickr/owcty", "/sccmulexp/flickr/minlabel");
    scc_multiGDecom("/sccmulexp/flickr/minlabel", "/sccmulexp/flickr/output3");
}

int main(int argc, char* argv[])
{
    init_workers();

    //pregel_tosccgraphformat("/pullgel/twitter","/scc/twitter");
    //pregel_tosccgraphformat("/pullgel/LJ","/scc/LJ");

    //pregel_tosccgraphformat("/pullgel/patent","/scc/patent");
    //pregel_tosccgraphformat("/pullgel/pokec","/scc/pokec");
    //pregel_tosccgraphformat("/pullgel/flickr","/scc/flickr");

    /*
    if (_my_rank == 0)
        cout << "Twitter SCC" << endl;
    twitter();

    if (_my_rank == 0)
        cout << "LJ SCC" << endl;
    LJ();
    */

    if (_my_rank == 0)
        cout << "patent SCC" << endl;
    multi_patent();

    if (_my_rank == 0)
        cout << "pokec SCC" << endl;
    multi_pokec();

    if (_my_rank == 0)
        cout << "flickr SCC" << endl;
    multi_flickr();

    worker_finalize();
    return 0;
}
