package workflow;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;



import utils.DBAdaptor;

public class MCL_Workflow1 {

	// would be called by bash script
	// the script would run mcl and generate output in output folder
	// this java code is to 1. read that output and put in database
	// and 2. to make new input files in the input folder for mcl running
	// the script would then run all new input files and generate new output

	//the script would find all the files with specific extension for each run 
	//and submit them for clustering
	//the files for previous runs should all be moved to another folder

	// For every threshold all the files should be read and then new file in sparate folder should be
	// made for the new threshold
	
//	-i /scratch/usman/mcl/in/thresh.005.abc -o /scratch/usman/mcl/out/thresh.005.out
	
	//String NewFilesfolder = "/scratch/usman/mcl/in/";	
	//String MCLresultsfolder = "/scratch/usman/mcl/out/"; 
	
	String NewFilesfolder = "F:/mcl/testJava/in/";
	String MCLresultsfolder = "F:/mcl/testJava/out/";
	
	int clusterid; // resets to zero after for every new threshold
	//float thresh;
	int currentThresh;
	int nextThresh;

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	private static final DecimalFormat SIMILARITY_FORMAT = new DecimalFormat("0.000");

	private ArrayList<String> files_results;	// stores all the files for every threshold run
	
	public MCL_Workflow1(int ct, int nt) throws SQLException, IOException{
		
		this.currentThresh = ct;	//for the current files in output folder
		this.nextThresh = nt;		//for the next files to be made in input folder in abc format
		
		File folder = new File(this.MCLresultsfolder);
		listFilesForFolder(folder);
		run();
	}

	public void listFilesForFolder(final File folder) {		// to get all the files for this threshold run
		files_results = new ArrayList<String>();
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				listFilesForFolder(fileEntry);
			} else {
				
				if (fileEntry.getName().toString().endsWith(this.currentThresh+".out")){
					// to pick up only current threshold files
					//files_results.add(fileEntry.getName().toString());
					files_results.add(fileEntry.getPath());
				}
				
			}
		}
	}


	void run() throws SQLException, IOException{

		//read all files in output folder and for each file --> call write to Db
		// all files are actually output of one threshold
		this.clusterid = 0;

		for (int i =0; i<= files_results.size()-1; i++){
			writetoDbnCallMakeNew(files_results.get(i));	

		}

	}

	void makenewInputFiles(String [] cluster,int clusid) throws IOException{
		// for each cluster makes new input file

		//---> Get threshNew
		BufferedWriter wr = new BufferedWriter(new FileWriter(this.NewFilesfolder+"thresh"+clusid+"."+this.nextThresh+".abc"));	//PATH TO INPUTFILE + NAME
		for (int i =0 ; i<= cluster.length-1; i++){	//check if the condition is correct or wrong
			int mem = Integer.parseInt(cluster[i]);	//find all the occurances of this member in mcl file of
			// e -5. So it is fast 

			//BufferedReader br = new BufferedReader(new FileReader("/scratch/usman/mcl/in/thresh.005.abc"));	** //PATH TO E-5 FILE
			BufferedReader br = new BufferedReader(new FileReader("F:/mcl/testJava/in/Testfile.thresh.005.abc"));
			//BufferedReader br = new BufferedReader(new FileReader("/scratch/usman/mcl/in/Testfile.thresh.005.abc"));
			String sCurrentLine = ""; 
			
			
			int a = 0;
			int b = 0;
			float c = 0f;

			while ((sCurrentLine = br.readLine()) != null) {
				String[] abc = sCurrentLine.split(" ");
				//System.out.print(abc[0]+" "+abc[1]+" "+abc[2]+"\n");
				
					a = Integer.parseInt(abc[0]);
					b = Integer.parseInt(abc[1]);
					try{
					c = Float.parseFloat(abc[2]);
				}
				catch(NumberFormatException e){
					c = 101;
				}


				if (c > this.nextThresh){ 	// CHECK CONDITION
					if (a == mem || b == mem){
						wr.write(a + " " + b + " "+c);
						wr.newLine();
					}
				}
			}
			br.close();
			

		}
		wr.close();

	}



	void writetoDbnCallMakeNew(String file) throws SQLException, IOException{
		PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
				"INSERT INTO clusters_mclTest " +
						"(cluster_id, cluster_threshold, sequenceid) VALUES " +
				"(?,?,?)");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String sCurrentLine = ""; 
		float ct_temp = this.currentThresh;
		while ((sCurrentLine = br.readLine()) != null) {	// every line is a cluster
			//1. split the line for tab to get members of cluster
			//2. insert

			String[] clusteredSequences = sCurrentLine.split("\t");

			for (int i =0 ; i<= clusteredSequences.length-1; i++){	//check if the condition is correct or wrong
				int mem = Integer.parseInt(clusteredSequences[i]);
				pstm1.setInt(1,this.clusterid);	//cluster id	
				//pstm1.setFloat(2, this.currentThresh);		//threhold of current files for which clustering is done
				pstm1.setFloat(2, ct_temp);
				pstm1.setInt(3,mem);				// cluster member

				pstm1.addBatch();	// if confusion then un comment below
				//pstm1.execute();
			}
			pstm1.execute();
			// CALL MAKE NEW FILE FOR THIS CLUSTER
			
			if (this.currentThresh < 100){
				makenewInputFiles(clusteredSequences,this.clusterid);
			}
			
			this.clusterid ++;	//once in a loop...not for every clusteredsequence member
		}
		br.close();
	}

	
}
