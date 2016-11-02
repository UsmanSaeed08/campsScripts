package runs;

import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import workflow.Pdbtm2scop_cath;;


public class Pdbtm2scop_cathRUN {
	
	private static final SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Date startDate = new Date();
		System.out.println("\n\n\t...["+df.format(startDate)+"]  Extract structural classifications for PDBtm entries");
		
		CreateDatabaseTables.create_table_pdbtm2scop_cath();
		CreateDatabaseTables.create_table_other_database_hierarchies();
		
		Pdbtm2scop_cath.getStructuralClassifications();
				
		Date endDate = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
		
		
		
		Date startDate2 = new Date();
		System.out.println("\n\n\t...["+df.format(startDate2)+"]  Extract CATH hierarchy");
		Pdbtm2scop_cath.addCathHierarchy("/home/proj/check/otherdatabase/CathNames.v4.0.0");	
				
		Date endDate2 = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate2)+"]");
//		
//		
//		
		Date startDate3 = new Date();
		System.out.println("\n\n\t...["+df.format(startDate3)+"]  Extract SCOP hierarchy");
		Pdbtm2scop_cath.addScopHierarchy("/home/proj/check/otherdatabase/dir.des.scope.2.05-stable.txt");	
//				
		Date endDate3 = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate3)+"]");
		
		
		Pdbtm2scop_cath.disconnect();

	}

}
