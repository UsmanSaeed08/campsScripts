package workflow;
import java.util.*;
public class Dictionary3 {
	
	//public ArrayList<Integer> protein = new ArrayList<Integer>();
	//public ArrayList<Float> score = new ArrayList<Float>();
	public HashMap<Integer,Float> prot2score = new HashMap<Integer,Float>();
	
	public void  Dictionary3(){
		//protein = new ArrayList<Integer>();
		//score = new ArrayList<Float>();
	}
	
	public Dictionary3(Integer aa, float cc){
		/*protein = new ArrayList<Integer>();
		score = new ArrayList<Float>();
		
		protein.add(aa);
		score.add(cc);*/
		prot2score.put(aa, cc);
	}
	public void set(Integer aa, float cc){
		/*int s = this.protein.size();
		int ss = this.score.size();
		
		this.protein.add(s,aa);
		this.score.add(ss,cc);*/
		prot2score.put(aa, cc);
	}

}
