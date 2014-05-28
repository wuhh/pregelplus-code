#include "pregel_app_UGTODG.h"
int main(int argc, char* argv[])
{
    init_workers();
    pregel_UGTODG("/pullgel/webuk", "/reach/webuk");
    pregel_UGTODG("/pullgel/livej", "/reach/livej");
    pregel_UGTODG("/pullgel/iusa", "/reach/iusa");
    worker_finalize();
    return 0;
}
