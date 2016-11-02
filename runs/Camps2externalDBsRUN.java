package runs;


import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import utils.DBAdaptor;
import workflow.Camps2externalDB;


public class Camps2externalDBsRUN {
	
	private static final SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );

	/**
	 * @param args
	 */
	
	public static void main(String[] args) {
		System.out.print("Running\n");
		makerun();
	}
	public static void makerun() {
		
		Date startDate = new Date();
		System.out.println("\n\n\t...["+df.format(startDate)+"]  Extract external links from SIMAP");
		
		//CreateDatabaseTables.create_table_camps2uniprot();
		//CreateDatabaseTables.create_table_camps2pdb();
		//CreateDatabaseTables.create_table_camps2genbank();
		
		Camps2externalDB.run();
		
		Date endDate = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
		
		
	}

}
