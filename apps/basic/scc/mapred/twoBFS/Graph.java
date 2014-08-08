package twoBFS;
import java.util.*;

public class Graph{
	
	public TreeMap<Long, Vertex> getV=new TreeMap<Long, Vertex>();
	public long time=0;
	
	//=======================================
	
	public void dfs_init()
	{
		for(Vertex v:getV.values())
		{
			v.color=0;
		}
	}
	
	public void dfs_visit(long id)
	{
		Vertex u=getV.get(id);
		u.color=1;
		time++;
		for(Long vid:u.outEdges)
		{
			Vertex v=getV.get(vid);
			if(v.color==0)
			{
				v.pa=id;
				dfs_visit(vid);
			}
		}
		u.color=2;
		time++;
		u.finish=time;
	}
	
	public void dfs()
	{
		time=0;
		for(long id:getV.keySet())
		{
			Vertex u=getV.get(id);
			if(u.color==0) dfs_visit(id);
		}
	}
	
	public void rdfs_visit(long id, Vector<Long> collector)
	{
		Vertex u=getV.get(id);
		u.color=1;
		for(Long vid:u.inEdges)
		{
			Vertex v=getV.get(vid);
			if(v.color==0)
			{
				v.pa=id;
				rdfs_visit(vid, collector);
			}
		}
		u.color=2;
		collector.add(id);
	}
	
	public void rdfs(Vector<Long> collector)
	{
		for(long id:getV.keySet())
		{
			Vertex u=getV.get(id);
			if(u.color==0) rdfs_visit(id, collector);
		}
	}
	
	//=======================================
	
	int pos=0;
	
	public Vector<Vertex> sccInit()
	{
		dfs();
		dfs_init();
		Vector<Vertex> re=new Vector<Vertex>();
		for(Vertex v:getV.values()) re.add(v);
		Collections.sort(re);
		pos=0;
		return re;
	}
	
	public Vector<Long> nextScc(Vector<Vertex> vec)
	{
		while(pos<vec.size())
		{
			Vector<Long> re=new Vector<Long>();
			Vertex cur=vec.get(pos);
			pos++;
			if(cur.color==0){
				rdfs_visit(cur.id, re);
				return re;
			}
		}
		return null;
	}
	
	public static void main(String[] args)
	{
		Graph g=new Graph();
		Vertex v1=new Vertex(1, "2 1 3 0");
		g.getV.put(v1.id, v1);
		Vertex v2=new Vertex(2, "3 1 1 0");
		g.getV.put(v2.id, v2);
		Vertex v3=new Vertex(3, "1 1 2 0 7 1");
		g.getV.put(v3.id, v3);
		Vertex v4=new Vertex(4, "5 1 6 0");
		g.getV.put(v4.id, v4);
		Vertex v5=new Vertex(5, "6 1 4 0");
		g.getV.put(v5.id, v5);
		Vertex v6=new Vertex(6, "4 1 5 0 7 1");
		g.getV.put(v6.id, v6);
		Vertex v7=new Vertex(7, "8 1 3 0 6 0 9 0");
		g.getV.put(v7.id, v7);
		Vertex v8=new Vertex(8, "9 1 7 0");
		g.getV.put(v8.id, v8);
		Vertex v9=new Vertex(9, "8 0 7 1");
		g.getV.put(v9.id, v9);
		
		Vector<Vertex> vec=g.sccInit();
		Vector<Long> l=null;
		while((l=g.nextScc(vec))!=null)
		{
			System.out.println(l);
		}
	}
	
}
