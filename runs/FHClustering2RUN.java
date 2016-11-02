package runs;


import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import utils.DBAdaptor;
import workflow.FHClustering2;



public class FHClustering2RUN {
	
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			
			CreateDatabaseTables.create_table_fh_clusters();
			FHClustering2.run();
			DBAdaptor.createIndex("camps4","fh_clusters",new String[]{"code"},"code");
			
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}	

	}

}
