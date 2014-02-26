#include "req_app_fieldbcast.h"

int main(int argc, char* argv[])
{
    init_workers();
    req_fieldbcast("/field1", "/field2");
    worker_finalize();
    return 0;
}
