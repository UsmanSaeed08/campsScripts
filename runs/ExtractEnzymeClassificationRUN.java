

package runs;


import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import extract_proteins.ExtractEnzymeClassification;



public class ExtractEnzymeClassificationRUN {
	
	private static final SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
		

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Date startDate = new Date();
		System.out.println("\n\n\t...["+df.format(startDate)+"]  Extract expasy enzme classification data");
		
		CreateDatabaseTables.create_table_ec_classification();
		
		//String fname1 = "/home/proj/check/otherdatabase/expasy/enzyme.dat";
		//String fname2 = "/home/proj/check/otherdatabase/expasy/enzclass.txt";
		
		String fname1 = "F:/Scratch/expassy/enzyme.dat";
		String fname2 = "F:/Scratch/expassy/enzclass.txt";
		
		ExtractEnzymeClassification.run(fname1, fname2);
						
		Date endDate = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate)+"]");

	}

}
