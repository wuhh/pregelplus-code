#include "pregel_app_hashmin.h"

int main(int argc, char* argv[]){
	init_workers();
	pregel_hashmin("/pagerank", "/hashmin_out", true);
	worker_finalize();
	return 0;
}
