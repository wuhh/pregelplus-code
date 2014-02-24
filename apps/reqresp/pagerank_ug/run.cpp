#define RESP_APP_PAGERANK

#include "resp_app_pagerank.h"

int main(int argc, char* argv[]){
	init_workers();
	resp_pagerank("/toy", "/ydout");
	worker_finalize();
	return 0;
}
