package runs;

import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import utils.DBAdaptor;
import workflow.Taxonomy2CpCluster2;


public class Taxonomy2CpCluster2RUN {
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			//System.exit(0);
			CreateDatabaseTables.create_table_taxonomy2cpCluster2();
			Taxonomy2CpCluster2 tax = new Taxonomy2CpCluster2();
			tax.run();
			
			DBAdaptor.createIndex("camps4","taxonomy2cpCluster2",new String[]{"taxonomyid"},"taxonomyid");
			DBAdaptor.createIndex("camps4","taxonomy2cpCluster2",new String[]{"taxonomyid","type"},"cindex1");
			DBAdaptor.createIndex("camps4","taxonomy2cpCluster2",new String[]{"cluster_code","type"},"cindex2");
									
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}	

	}
	

}
