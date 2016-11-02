package runs;

import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import utils.DBAdaptor;
import workflow.Pdbtm2opm;


public class Pdbtm2opmRUN {
	
	private static final SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Date startDate = new Date();
		System.out.println("\n\n\t...["+df.format(startDate)+"]  Extract classifications for PDBtm entries");
		CreateDatabaseTables.create_table_pdbtm2opm();
		Pdbtm2opm.getClassifications("F:/SC_Clust_postHmm/otherdatabase/opm/opm_tm.txt");
		
		DBAdaptor.createIndex("camps4","pdbtm2opm",new String[]{"pdb_id"},"pdb_id");
				
		Date endDate = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
		
		Pdbtm2opm.disconnect();

	}

}
