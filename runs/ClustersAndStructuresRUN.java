package runs;

import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import utils.DBAdaptor;
import workflow.ClustersAndStructures;
import workflow.ClustersAndStructures_ForMcl0;


public class ClustersAndStructuresRUN {
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			/*
			 * Now re-running this all to make new clusters_mcl_strcutures2 table after having new clusters i.e. after camps rerunning..
			 * The calculation is based on 30% identity
			 */
			
			CreateDatabaseTables.create_table_clusters_mcl_structures();
			//ClustersAndStructures.run(); -- was used for making mcl_clusters_structures
											// but now using below to resolve the problem of identity
			// The problem was that for a pdb, the max identity was not considered, rather it was added simply if 
			// the identity was over 30%
			ClustersAndStructures_ForMcl0.run();
			// after above is complete -- add pdb titles using: workflow.AddPDBTitleNames
			
			DBAdaptor.createIndex("camps4","clusters_mcl_structures2",new String[]{"pdbid"},"pdbid");
			DBAdaptor.createIndex("camps4","clusters_mcl_structures2",new String[]{"cluster_id","cluster_threshold"},"cindex1");
						
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}	

	}

}
