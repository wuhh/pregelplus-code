package twoBFS;

import java.util.*;

public class Vertex implements Comparable<Vertex>{
	
	public long id;
	public List<Long> inEdges=new ArrayList<Long>();
	public List<Long> outEdges=new ArrayList<Long>();
	public byte color;//white=0, gray=1, black=2
	public long pa;
	public long finish;
	
	public Vertex(long id, String edgeStr)
	{
		this.id=id;
		StringTokenizer tk=new StringTokenizer(edgeStr);
		long in_num = Long.parseLong(tk.nextToken());
		for(int i = 0 ; i< in_num ; i ++)
		{
		    long dst=Long.parseLong(tk.nextToken());
		    inEdges.add(dst);
		}
		long out_num = Long.parseLong(tk.nextToken());
		for(int i = 0 ; i< out_num ; i ++)
		{
		    long dst=Long.parseLong(tk.nextToken());
		    outEdges.add(dst);
		}
		color=0;
		pa=-1;
		finish=0;
	}
	
	public Vertex(long id)
	{
		this.id=id;
		color=0;
		pa=-1;
		finish=0;
	}
	
	public int compareTo(Vertex v)
	{
		//descreasing "finish"
		if(finish>v.finish) return -1;
		else if(finish<v.finish) return 1;
		else return 0;
	}

}
