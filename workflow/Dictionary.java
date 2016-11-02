package workflow;
import java.util.*;
public class Dictionary {
	
	public ArrayList<Integer> protein = new ArrayList<Integer>();
	public ArrayList<Float> score = new ArrayList<Float>();
	
	public void  Dictionary(){
		protein = new ArrayList<Integer>();
		score = new ArrayList<Float>();
	}
	
	public Dictionary(Integer aa, float cc){
		protein = new ArrayList<Integer>();
		score = new ArrayList<Float>();
		
		protein.add(aa);
		score.add(cc);
	}
	public void set(Integer aa, float cc){
		int s = this.protein.size();
		int ss = this.score.size();
		
		this.protein.add(s,aa);
		this.score.add(ss,cc);
	}

}
