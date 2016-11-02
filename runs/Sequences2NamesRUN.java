package runs;

import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import utils.DBAdaptor;
import workflow.Sequences2Names;


public class Sequences2NamesRUN {
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			
			CreateDatabaseTables.create_table_sequences2names();
			
			Sequences2Names.run();
			
			DBAdaptor.createIndex("camps4","sequences2names",new String[]{"sequenceid"},"sequenceid");
			DBAdaptor.createIndex("camps4","sequences2names",new String[]{"md5"},"md5");
			DBAdaptor.createIndex("camps4","sequences2names",new String[]{"name"},"name");
						
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}	

	}

}
