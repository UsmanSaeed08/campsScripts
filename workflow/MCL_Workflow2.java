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

public class MCL_Workflow2 {

	// different from MCL_Workflow1 by the use of threading
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

	//gefjun-10g
	String NewFilesfolder ;//= "/home/users/saeed/mcl_run/in/";	
	String MCLresultsfolder ;//= "/home/users/saeed/mcl_run/out/";
	String dictP;

	//String NewFilesfolder = "F:/mcl/testJava/in/";
	//String MCLresultsfolder = "F:/mcl/testJava/out/";

	int clusterid; // resets to zero after for every new threshold
	//float thresh;
	int currentThresh;
	int nextThresh;

	//private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	//private static final DecimalFormat SIMILARITY_FORMAT = new DecimalFormat("0.000");

	private ArrayList<String> files_results;	// stores all the files for every threshold run

	public MCL_Workflow2(int ct, int nt, String in, String out, String d) throws SQLException, IOException{

		this.currentThresh = ct;	//for the current files in output folder
		this.nextThresh = nt;		//for the next files to be made in input folder in abc format
		this.NewFilesfolder = in;
		this.MCLresultsfolder = out;
		this.dictP = d;

		File folder = new File(this.MCLresultsfolder);
		
		// remove make dictionary and make it in the new files maker
		//if(this.nextThresh != 0){
			//makedictionary();	//to reduce the search space for next runs
		//}

		listFilesForFolder(folder);
		System.out.println("Out Files Fetched: "+ files_results.size());
		System.out.flush();
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
		
		Makenewfile_mcl.setCurrentThresh(this.currentThresh); //set all
		Makenewfile_mcl.setNextThresh(this.nextThresh);
		Makenewfile_mcl.setDictPath(this.dictP);
		
		System.out.println("Making Dictionary...");
		System.out.flush();
		if(this.nextThresh != 0){
			Makenewfile_mcl.makedict();	//to reduce the search space for next runs
		}
		System.out.println("Make Dictionary Complete ");
		System.out.flush();
		//BufferedWriter wr = new BufferedWriter(new FileWriter("F:/mcl/testJava2/Log."+this.currentThresh+".txt"));
		BufferedWriter wr = new BufferedWriter(new FileWriter("/localscratch/CAMPS/mcl_run/logs/Log."+this.currentThresh+".txt"));
		wr.write("Make Dictionary Complete ");
		wr.newLine();
		wr.flush();
		
		//System.out.println("Populate db and make new input files. New Files made by using similarity matrix. ");
		System.out.println("Make new input files. ");
		System.out.flush();

		for (int i =0; i<= files_results.size()-1; i++){
			
			//write to db
			// --- comment out
			/*
			Writetodb_mcl objdb1 = new Writetodb_mcl();
			objdb1 = new Writetodb_mcl(this.clusterid,this.currentThresh,files_results.get(i));
			objdb1.run();
			//objdb1.Close_connection();
			wr.write("Writing to Db Complete for file "+ files_results.get(i));
			wr.newLine();
			wr.flush();
			*/
			//make new file
			//have to put dictionary in static variable and use it there
			// have to take care for the cluster id...as before threadworkbywrokflow2 class was managing it.
			// cusid ++ for every cluster and becomes zero for every new threshold...i.e only the next time when sfotware runs
			
			MclThreadworkbyWorkflow2 obj = new MclThreadworkbyWorkflow2(this.clusterid,this.NewFilesfolder, this.MCLresultsfolder, wr);
			obj.run(files_results.get(i));
			//this.clusterid = obj.lines;
			this.clusterid = obj.clusterid;
			wr.write("Making new files from current thresh file Complete: "+ files_results.get(i));
			wr.newLine();
			wr.newLine();
			wr.newLine();
			wr.flush();

		}
		wr.close();
		System.out.println("File making complete... ");
		System.out.flush();

	}




}
