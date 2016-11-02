package runs;

import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import utils.DBAdaptor;
import workflow.CpClusters;


public class CpClustersRUN {
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			
			//CreateDatabaseTables.create_table_cp_clusters();
			
			//CpClusters.run("/home/proj/check/RunMetaModel_gef/HMMs/CAMPS4_1/");
			CpClusters.run("F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/HMMs/CAMPS4_1/");
			///F:\SC_Clust_postHmm\Results_hmm_new16Jan\RunMetaModel_gef\HMMs\CAMPS4_1
			
			//DBAdaptor.createIndex("camps4","cp_clusters",new String[]{"cluster_id","cluster_threshold","type"},"cindex1");
			//DBAdaptor.createIndex("camps4","cp_clusters",new String[]{"super_cluster_id","super_cluster_threshold"},"cindex2");
			//DBAdaptor.createIndex("camps4","cp_clusters",new String[]{"cluster_id","cluster_threshold"},"cindex3");
						
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}	

	}

}

