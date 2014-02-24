#include "reqresp/req-dev.h"
#include "utils/type.h"
using namespace std;

//<V>=<D[v], star[v]>
//initially, D[v]=v, star[v]=false
struct SVValue_ldg
{
	vwpair D;
	bool star;
	vector<vwpair> edges;
};

ibinstream & operator<<(ibinstream & m, const SVValue_ldg & v)
{
	m << v.D;
	m << v.star;
	m << v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, SVValue_ldg & v)
{
	m >> v.D;
	m >> v.star;
	m >> v.edges;
	return m;
}

//====================================
vwpair nullV = vwpair(-1,-1);

class SVVertex_ldg: public RVertex<vwpair, SVValue_ldg, vwpair, vwpair,VWPairHash>
{

	void treeInit_D()
	{
		//set D[u]=min{v} to allow fastest convergence, though any v is ok (assuming (u, v) is accessed last)
		vector<vwpair> & edges = value().edges;
		for (int i = 0; i < edges.size(); i++)
		{
			vwpair nb = edges[i];
			if (nb < value().D)
				value().D = nb;
		}
	}

	void req_Du()
	{ // request to w
		vwpair Du = value().D;
		request(Du);
	} //respond logic: (1)int Dw=value().D; (2)return Dw

	void bcast_Dv() //in fact, one can use the ghost technique
	{ // send negated D[v]
		vwpair Dv = value().D;
		vector<vwpair> & edges = value().edges;
		for (int i = 0; i < edges.size(); i++)
		{
			vwpair nb = edges[i];
			send_message(nb, Dv);
		}
	} //in fact, a combiner with MIN operator can be used here

	// ========================================

	void rtHook_check(MessageContainer & msgs)
	{ //set D[w]=min_v{D[v]} to allow fastest convergence, though any D[v] is ok (assuming (u, v) is accessed last)
		vwpair Du = value().D;
		vwpair Dw = get_respond(Du);
		vwpair Dv = nullV; //pick the min
		for (int i = 0; i < msgs.size(); i++)
		{
			vwpair cur = msgs[i];
			if (Dv == nullV || cur < Dv)
				Dv = cur;
		}
		if (Dw == Du && Dv != nullV && Dv < Du) //condition checking
		{
			send_message(Du, Dv);
		}
	}

	void rtHook_update(MessageContainer & msgs) // = starhook's write D[D[u]]
	{ //set D[w]=min_v{D[v]} to allow fastest convergence, though any D[v] is ok (assuming (u, v) is accessed last)
		vwpair Dv = nullV;
		for (int i = 0; i < msgs.size(); i++)
		{
			vwpair cur = msgs[i];
			if (Dv == nullV || cur < Dv)
				Dv = cur;
		}
		if (Dv != nullV)
			value().D = Dv;
	}

	// ========================================

	void starHook_update(MessageContainer & msgs) // set star[u] first
	{ //set D[w]=min_v{D[v]} to allow fastest convergence
		if (value().star)
		{
			vwpair Du = value().D;
			vwpair Dv = nullV;
			for (int i = 0; i < msgs.size(); i++)
			{
				vwpair cur = msgs[i];
				if (Dv == nullV || cur < Dv)
					Dv = cur;
			}
			if (Dv != nullV && Dv < Du) //condition checking
			{
				send_message(Du, Dv);
			}
		}
	}

	// ========================================

	void shortcut_update()
	{ //D[u]=D[D[u]]
		value().D = get_respond(value().D);
	}

	// ========================================

	void setStar_notify(MessageContainer & msgs)
	{
		vwpair Du = value().D;
		vwpair Dw = get_respond(Du);
		if (Du != Dw)
		{
			value().star = false;
			//notify Du
			send_message(Du, nullV); //-1 means star_notify
			//notify Dw
			send_message(Dw, nullV);
		}
	}

	void setStar_update(MessageContainer & msgs)
	{
		if (msgs.size() > 0)
			value().star = false;
		request(value().D);
	} //respond logic: (1)bool star=value().star; (2)return star

	void setStar_final()
	{
		value().star = get_respond(value().D).vid;
	}

	//==================================================

public:

	virtual vwpair respond()
	{
		int step = step_num();
		if (step % 5 == 0)
			return vwpair(value().star, -1);
		else
			return value().D;
	}

	virtual void compute(MessageContainer & messages)
	{
		int cycle = 10;
		if (step_num() == 1)
		{
			treeInit_D();
			req_Du();
			//resp D
			bcast_Dv();
		}
		else if (step_num() % cycle == 2)
		{
			//============== end condition ==============
			bool* agg = (bool*) getAgg();
			if (*agg)
			{
				vote_to_halt();
				return;
			}
			//===========================================
			rtHook_check(messages);
		}
		else if (step_num() % cycle == 3)
		{
			rtHook_update(messages);
			value().star = true;
			req_Du();
			//resp D
		}
		else if (step_num() % cycle == 4)
		{
			setStar_notify(messages);
		}
		else if (step_num() % cycle == 5)
		{
			setStar_update(messages);
			//resp star
			bcast_Dv();
		}
		else if (step_num() % cycle == 6)
		{
			setStar_final();
			starHook_update(messages); //set star[v] first
		}
		else if (step_num() % cycle == 7)
		{
			rtHook_update(messages);
			req_Du();
			//resp D
		}
		else if (step_num() % cycle == 8)
		{
			shortcut_update();
			value().star = true;
			req_Du();
			//resp D
		}
		else if (step_num() % cycle == 9)
		{
			setStar_notify(messages);
		}
		else if (step_num() % cycle == 0)
		{
			setStar_update(messages);
			//resp star
		}
		else if (step_num() % cycle == 1)
		{
			setStar_final();
			req_Du();
			//resp D
			bcast_Dv();
		}
	}
};

//====================================

class SVAgg_ldg: public Aggregator<SVVertex_ldg, bool, bool>
{
private:
	bool AND;
public:
	virtual void init()
	{
		AND = true;
	}

	virtual void stepPartial(SVVertex_ldg* v)
	{
		if (v->value().star == false)
			AND = false;
	}

	virtual void stepFinal(bool* part)
	{
		if (part == false)
			AND = false;
	}

	virtual bool* finishPartial()
	{
		return &AND;
	}
	virtual bool* finishFinal()
	{
		return &AND;
	}
};

//====================================

class SVWorker_ldg: public RWorker<SVVertex_ldg, SVAgg_ldg>
{
	char buf[100];

public:

	virtual SVVertex_ldg* toVertex(char* line)
	{
		char * pch;
		pch = strtok(line, " ");
		SVVertex_ldg* v = new SVVertex_ldg;
		v->id.vid = atoi(pch);
		pch = strtok(NULL, "\t");
		v->id.wid = atoi(pch);
		while (pch = strtok(NULL, " "))
		{
			int vid = atoi(pch);
			pch = strtok(NULL, " ");
			int wid = atoi(pch);
			v->value().edges.push_back(vwpair(vid, wid));
		}

		v->value().D = v->id;
		v->value().star = false; //strictly speaking, this should be true
		//after treeInit_D(), should do star-checking
		//however, this is time-consuming, and it's very unlikely that treeInit_D() gives stars
		//therefore, set false here to save the first star-checking
		return v;
	}

	virtual void toline(SVVertex_ldg* v, BufferedWriter & writer)
	{
		sprintf(buf, "%d\t%d\n", v->id.vid, v->value().D.vid);
		writer.write(buf);
	}
};

void req_ldg_sv(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path = in_path;
	param.output_path = out_path;
	param.force_write = true;
	param.native_dispatcher = false;
	SVWorker_ldg worker;
	SVAgg_ldg agg;
	worker.setAggregator(&agg);
	worker.run(param);
}
