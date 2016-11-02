package reRUN_CAMPS;

import java.io.File;

import workflow.MCL_PopulateDb;
import workflow.Writetodb_mcl;

public class PutMCLclustersInDB {

	/**
	 * @param args
	 * As I noticed during the running of mcl clustering that the process was slow due to making connection with
	 * database and then putting all the data in it. Therefore, I only generated the .out files and processed them later to put them in database.
	 * This class is used to put all those out files to database.   
	 */
	private static final int[] THRESHOLDS_EXPONENT = new int[] {
		5,
		6,
		7,
		8,
		9,
		10,	
		11,
		12,
		13,
		14,
		15,		
		16,
		17,
		18,
		19,
		20,		
		22,
		24,
		25,
		26,
		28,
		30,
		35,
		40,
		45,
		50,
		55,
		60,
		70,
		80,
		90,
		100};
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Running...");
		for(int i =0;i<=THRESHOLDS_EXPONENT.length-1;i++){
			int curentThresh = THRESHOLDS_EXPONENT[i]; //Integer.parseInt(currentThreshold);
			MCL_PopulateDb obj;
			if(curentThresh > 55 ){ // 9 because 5 6 7 8 have already been in the table
				System.out.println("Threshold in progress: " + curentThresh);
				obj = new MCL_PopulateDb(curentThresh, false);
				File folder = new File("/localscratch/CAMPS/mcl_run/done/"); // get currentThresh.out files in this folder
				obj.listFilesForFolder(folder);
				obj.print();
				obj.run();
			}
		}
		Writetodb_mcl.Close_connection();
	}
}
