#include "req_ldg_sv.h"

int main(int argc, char* argv[]){
	init_workers();
	req_ldg_sv("/ldg", "/ldg_out");
	worker_finalize();
	return 0;
}
