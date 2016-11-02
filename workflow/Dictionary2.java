package workflow;


import java.util.*;
public class Dictionary2 {
	
	public HashMap<Integer,Float> protein = new HashMap<Integer,Float>();
	//public ArrayList<Float> score = new ArrayList<Float>();
	
	public void  Dictionary2(){
		protein = new HashMap<Integer,Float>();
		//score = new ArrayList<Float>();
	}
	
	public Dictionary2(Integer aa, float cc){
		protein = new HashMap<Integer,Float>();
		//score = new ArrayList<Float>();
		
		protein.put(aa,cc);
		//score.add(cc);
	}
	public void set(Integer aa, float cc){
		//int s = this.protein.size();
		//int ss = this.score.size();
		
		this.protein.put(aa,cc);
		//this.score.add(ss,cc);
	}
}