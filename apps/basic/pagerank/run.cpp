#include "pregel_app_pagerank.h"

int main(int argc, char* argv[]){
	init_workers();
	pregel_pagerank("/pagerank", "/pagerank_out", true);
	worker_finalize();
	return 0;
}
