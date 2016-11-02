package runs;

import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import utils.DBAdaptor;
import workflow.Taxonomy2CpClusterCount2;


public class Taxonomy2CpClusterCount2RUN {
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			
			CreateDatabaseTables.create_table_taxonomy2cpClusterCount2();
			
			Taxonomy2CpClusterCount2.run();
			
			DBAdaptor.createIndex("camps4","taxonomy2cpClusterCount2",new String[]{"taxonomyid"},"taxonomyid");
												
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}	

	}

}
