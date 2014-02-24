#ifndef TERA_INTPAIR_H_
#define TERA_INTPAIR_H_

#include "utils/TeraSort.h"
#include "utils/type.h"

class IntpairSort:public TeraWorker<intpair>
{
	public:
		IntpairSort(double sampling_rate, bool prefixsum):
			TeraWorker<intpair>(sampling_rate, prefixsum){}

		virtual TeraItem<intpair>* toVertex(char* line)
		{
			TeraItem<intpair>* v=new TeraItem<intpair>;
			v->content=line;//strtok will change line later
			char * pch;
			pch=strtok(line, " ");
			v->key.v1=atoi(pch);
			pch=strtok(NULL, "\t");
			v->key.v2=atoi(pch);
			return v;
		}

		virtual void toline(TeraItem<intpair>* v)
		{
			write(v->content.c_str());
			write("\n");
		}
};

void sort_intpair(string in_path, string out_path, double sampRate, bool orderNum)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=true;
	IntpairSort worker(sampRate, orderNum);
	worker.run(param);
}

#endif
