#include "ghost_app_hashmin.h"

int main(int argc, char* argv[]){
	init_workers();
	set_ghost_threshold(100);
	ghost_hashmin("/pagerank", "/hashmin_ghost", true);
	worker_finalize();
	return 0;
}
