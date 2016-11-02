package reRUN_CAMPS;

import java.io.IOException;
import java.sql.SQLException;

import workflow.MCL_Workflow2;

public class MCL_Main {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws SQLException, IOException {
		// TODO Auto-generated method stub
		
		String currentThreshold = args[0];
		String nextThreshold = "";
		try{
		nextThreshold = args[1];
		}
		catch(Exception e){
			nextThreshold = "0";
		}
		String inFolder = "/localscratch/CAMPS/mcl_run/in/";
		String outFolder = "/localscratch/CAMPS/mcl_run/out/";
		String dictPath = "/localscratch/CAMPS/mcl_run/dict/";
		
		//String currentThreshold = "5";
		//String nextThreshold = "6";
		//String inFolder = "F:/mcl/testJava2/in/";
		//String outFolder = "F:/mcl/testJava2/out/";
		//String dictPath = "F:/mcl/testJava2/dict/";
		
		int curentThresh = Integer.parseInt(currentThreshold);
		int nextThresh = Integer.parseInt(nextThreshold);
		System.out.println("Running threshold: "+ currentThreshold +" -- " + nextThreshold);

		MCL_Workflow2 obj1 = new MCL_Workflow2(curentThresh,nextThresh, inFolder, outFolder, dictPath); //workflow2 uses threads

	}

}
