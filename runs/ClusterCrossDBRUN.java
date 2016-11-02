package runs;

import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import utils.DBAdaptor;
import workflow.ClusterCrossDB;


public class ClusterCrossDBRUN {
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			// 1 3 4 6 8
			//CreateDatabaseTables.create_table_clusters_cross_db();
			
			//ClusterCrossDB.runCath(); //script 1
			
			//ClusterCrossDB.runGo();	script 2
			
			//ClusterCrossDB.runGPCR();	//script 3
			//ClusterCrossDB.runOPM();	//script 4
			//ClusterCrossDB.runPfam();	script 5
			//ClusterCrossDB.runSCOP();	//script 6
			//ClusterCrossDB.runSuperfamily();	script 7
			//ClusterCrossDB.runTCDB();	//script 8
			
			//DBAdaptor.createIndex("camps4","clusters_cross_db",new String[]{"cluster_id","cluster_threshold"},"cindex1");
			//DBAdaptor.createIndex("camps4","clusters_cross_db",new String[]{"cluster_id","cluster_threshold","db"},"cindex2");
						
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}	

	}

}
