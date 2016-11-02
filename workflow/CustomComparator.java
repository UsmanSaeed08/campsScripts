package workflow;

import java.util.Comparator;

/**
 * 
 * @author Usman Saeed
 *Used by the clusters_mcl_structures -- ClustersAndStructures.java
 *Sorts ascending
 */
public class CustomComparator implements Comparator<OtherDatabase_Strucutre> {
	
    @Override
    public int compare(OtherDatabase_Strucutre o1, OtherDatabase_Strucutre o2) {
    	Float a = o1.ident;
    	Float b = o2.ident;
    	int ret = Float.compare(a, b);
    	if(ret>0){
    		return -1; // a > b
    	}
    	else if(ret<0){
    		return 1; // a<b
    	}
    	else{
    		return 0;
    	}
    	/*if (a<b){
    		return 1;
    	}
    	else if (ret==0){
    		return 0;
    	}
    	else{
    		return -1;
    	}*/
    }
    
}