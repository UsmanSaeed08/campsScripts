package runs;

import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import extract_proteins.ExtractDAClusterAssignments;


import utils.DBAdaptor;

public class ExtractDAClusterAssignmentsRUN {
	
	private static final SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			
			Date startDate = new Date();		
			
			System.out.println("START ["+df.format(startDate)+"]\n\n");
						
			Date start2Date = new Date();
			System.out.println("\n\n\t...["+df.format(start2Date)+"]  Extract Domain architecture cluster assignments from SIMAPclusters");
			//CreateDatabaseTables.create_table_da_cluster_assignments();			
			ExtractDAClusterAssignments.run();
			//DBAdaptor.createIndex("camps4","da_cluster_assignments",new String[]{"sequenceid"},"sequenceid");
			DBAdaptor.createIndex("camps4","da_cluster_assignments",new String[]{"sequenceid"},"sequenceid");
					
						
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
			
		} catch(Exception e) {
			e.printStackTrace();
		}

	}

}
