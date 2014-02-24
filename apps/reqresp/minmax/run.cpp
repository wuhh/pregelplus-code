#include "req_ppa_minmax.h"

int main(int argc, char* argv[]){
	init_workers();
	req_minmax("/merge_ppa", "/minmax_ppa");
	worker_finalize();
	return 0;
}
