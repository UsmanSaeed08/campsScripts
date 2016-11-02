package workflow;

import java.util.ArrayList;
import java.util.HashMap;

public class ClusterDs {
	// a data structure class of clusters obtained from clusters_mcl table to improve efficiency
	
	public float clusterThreshold = 0;
	public HashMap <Integer, ArrayList<Integer>> list = new HashMap<Integer, ArrayList<Integer>>();// clusterid key and array is sequences

}
