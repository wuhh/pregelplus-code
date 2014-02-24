#include "vwghost_app_hashmin.h"

int main(int argc, char* argv[]){
	init_workers();
	set_ghost_threshold(100);
	vwghost_hashmin("/pagerank_vw", "/hashmin_ghost", true);
	worker_finalize();
	return 0;
}
