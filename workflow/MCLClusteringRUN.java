package workflow;

/*
 * MCLClusteringRUN
 * 
 * Version 1.0
 * 
 * 2009-10-23
 * 
 */

//package runs;

import java.text.SimpleDateFormat;
import java.util.Date;

import workflow.MCLClustering;


/**
 * @author usman
 * 
 * 
 * Performs MCL clusterings for a given threshold.
 *
 */
public class MCLClusteringRUN {

	
		
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			
			//
			String mclOutdir = args[0];
			int currentThreshold = Integer.parseInt(args[1]);
			String outdir = args[2];
			//
			
			MCLClustering mcl = new MCLClustering(mclOutdir, currentThreshold, outdir);
			mcl.run();
			
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}

	}

}
