package extract_proteins;

import general.CreateDatabaseTables;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import runs.Camps2externalDBsRUN;
import runs.Camps2externalDBs_v2RUN;
import runs.ExtractGOAnnotation_v2RUN;
import runs.TmsCoresExtractionRUN;
import utils.DBAdaptor;
import workflow.Camps2externalDBs_v2;
import workflow.Dictionary;
import workflow.MCL_PopulateDb;
import workflow.MCL_Workflow1;
import workflow.MCL_Workflow2;
import workflow.MergeProteinsTables;
import workflow.Scrap;
import workflow.TestMCL;
import workflow.TmsCoresExtraction;
import workflow.initialClustering.ClusterPropertiesMCL_NR;
import workflow.initialClustering.ClustersMCLTrack;
import workflow.initialClustering.HomologyCleanup;

import java.util.Collection;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;










public class Main {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 */

	//private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("test_camps3");

	//private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");
	//private static final Connection CAMPS_CONNECTION_old = DBAdaptor.getConnection("CAMPS3");

	public static void main(String[] args) throws Exception {
		
		// ********************* CORE & BLOCK EXTRACTION Start *************************************
		//CreateDatabaseTables.create_tms_cores();
		//System.out.print("\n tms_cores made \n");
		//CreateDatabaseTables.create_tms_blocks();
		//System.out.print("\n tms_blocks made \n");
		//TmsCoresExtractionRUN.main(args);
		//TmsCoresExtraction.main(args);
		// ********************* CORE & BLOCK EXTRACTION END   *************************************
		

		// ********************* Calc ClusterProperties Start *************************************
		
		//ClusterPropertiesMCL_NR.main(args);
		// ********************* Calc ClusterProperties Start *************************************
		
		
		
		
		
		// ********************* EXTRACT GO ANNOTATION *************************************
/*
		SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
		Date startDate = new Date();
		System.out.println("\n\n\t...["+df.format(startDate)+"]  Extract GO annotations...");
		CreateDatabaseTables.create_table_go_annotations();
		
		String annotationFile = "/tmp/GOAnn/gene_association.goa_uniprot";
		String ontologyFile = "/tmp/GOAnn/GOAnnotation/gene_ontology_ext.obo";
		
		ExtractGOAnnotation.run(annotationFile,ontologyFile);
					
		Date endDate = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
*/
		// ********************* EXTRACT GO ANNOTATION ENDS ********************************
		
		// ********************* GET TOPOLOGY OF COMPLETE CAMPS DATASET START***********************
/*	
		SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
		Date startDate = new Date();
		System.out.println("\n\n\t...["+df.format(startDate)+"]  Determine membrane protein topology");
		CreateDatabaseTables.create_table_topology();
		DetermineTopology.run();		
		Date endDate = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
*/	
		// ********************* GET TOPOLOGY OF COMPLETE CAMPS DATASET END  ***********************
		
		// ********************* Extract PFAM Data Start *************************************************
/*		SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
		Date startDate = new Date();
		System.out.println("\n\n\t...["+df.format(startDate)+"]  Extract PFAM-A hits from SIMAPFeatures");
		CreateDatabaseTables.create_table_domains_pfam();
		ExtractPFAMData.run();
		DBAdaptor.createIndex("camps4","domains_pfam",new String[]{"sequenceid"},"sequenceid");
			
		Date endDate = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
*/
		// ********************* Extract PFAM Data End ***************************************************
		
		// ********************* Extract SUPERFAMILY Data Start *************************************************
/*		SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
		Date startDate = new Date();
		System.out.println("\n\n\t...["+df.format(startDate)+"]  Extract SUPERFAMILY hits from SIMAPFeatures");
		CreateDatabaseTables.create_table_domains_superfamily();
		ExtractSUPERFAMILYDATA.run();
		DBAdaptor.createIndex("camps4","domains_superfamily",new String[]{"sequenceid"},"sequenceid");
			
		Date endDate = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
*/
		// ********************* Extract SUPERFAMILY Data End ***************************************************


		// ************************** Workflow of Initial Clustering *********************
		
		// have made the file with clusters and thresholds. 
		//~/workspace/data/mc_clusters_camps4.txt using Create MCL clusterlist
		// NOW insert in clustersMCLTrack
		//
		//ClustersMCLTrack.run();

		//HomologyCleanup.main(args);
		
		// ***************************** END OF MCL CLUSTER LIST & Homology CLeanup & Clus Props ********
		//RUNNING MCL NOW
		/*
		 **********************************************************************************
		String currentThreshold = args[0];
		String nextThreshold = "";
		try{
		nextThreshold = args[1];
		}
		catch(Exception e){
			nextThreshold = "0";
		}
		//String inFolder = args[2];
		//String outFolder = args[3];
		//String dictPath = args[4];

		 ************************************************************************************/

		//String inFolder = "/home/users/saeed/mcl_run/in/";
		//String outFolder = "/home/users/saeed/mcl_run/out/";
		//String dictPath = "/home/users/saeed/mcl_run/dict/";

		//String inFolder = "/scratch/usman/mcl_run/in/";
		//String outFolder = "/scratch/usman/mcl_run/out/";
		//String dictPath = "/scratch/usman/mcl_run/dict/";

		//String inFolder = "F:/mcl/testJava/in/";
		//String outFolder = "F:/mcl/testJava/out/";
		//String dictPath = "F:/mcl/testJava/dict/";
		
		//String inFolder = "/localscratch/CAMPS/mcl_run/in/";
		//String outFolder = "/localscratch/CAMPS/mcl_run/out/";
		//String dictPath = "/localscratch/CAMPS/mcl_run/dict/";


		//MCL_Workflow1 obj1 = new MCL_Workflow1(curentThresh,nextThresh);

		//below trying out new mcl workflow with threads

		//String currentThreshold = "5";
		//String nextThreshold = "6";

		/* ******************************************************************************************
		int curentThresh = Integer.parseInt(currentThreshold);
		int nextThresh = Integer.parseInt(nextThreshold);

		MCL_Workflow2 obj1 = new MCL_Workflow2(curentThresh,nextThresh, inFolder, outFolder, dictPath); //workflow2 uses threads
		 *********************************************************************************************/
		/*
		for(int i =0; i<=20;i++){
			File f = new File("F:/mcl/testJava/test/thresh"+i+".17.out");
			 BufferedWriter output = new BufferedWriter(new FileWriter(f));
	         output.write("Hey");
	         output.close();
		}
		 */
		
		
/*
		String currentThreshold = args[0];
		//String currentThreshold = "55"; //6 7 10 20 50 *
		int curentThresh = Integer.parseInt(currentThreshold);
		MCL_PopulateDb obj;
		
		if (curentThresh == 5){
			obj = new MCL_PopulateDb(curentThresh, true);
		}
		else
		{
			obj = new MCL_PopulateDb(curentThresh, false);
		}
		File folder = new File("/scratch/usman/mcl_run/done/out/");
		
		//File folder = new File("F:/mcl/testJava/test/"); *
		
		obj.listFilesForFolder(folder);
		obj.print();
		obj.run();
*/	


		//TestMCL obj = new TestMCL();
		//obj.writefile();					// to generate e -5 file for mcl input

		//MCL RUNS END



		// ********** TO read file and populate alignment table just as it is in file. ***********
		//CreateDatabaseTables.create_table_alignments_initial();
		//InsertInAlign obj = new InsertInAlign();
		//obj.run();
		//System.out.print("H");
		//CreateDatabaseTables.create_table_alignments2();
		//InsertInAlign2 obj = new InsertInAlign2();
		//obj.run();
		//************** threaded Insert********
		//CreateDatabaseTables.create_table_alignments_temp();
		String f  = args[0];
		int no = Integer.parseInt(args[1]);
		InsertAlign4.run(f,no);
		
		//f = null;
		//obj.run();

		/*
		try {

			String sCurrentLine;
			BufferedReader br = null;

			br = new BufferedReader(new FileReader("G:/CAMPS_Similarity_Scores/camps_seq_file.matrix"));
		//	br = new BufferedReader(new FileReader("G:/CAMPS_Similarity_Scores/check.txt"));
			//br = new BufferedReader(new FileReader("/scratch/usman/download/camps_seq_file.matrix"));
			int i = 0;


			while ((sCurrentLine = br.readLine()) != null) {
				//System.out.println(sCurrentLine);
				//String q2[] = sCurrentLine.split("\t");

				//for (int j=0;j<q2.length;j++){
					//0 is queryid, 1 is hitid and so on
					//System.out.print(q2[j]);
					//System.out.print("\n");
				//}
				i++;
			}
			System.out.println(i);
		}
		catch (IOException e) {
			e.printStackTrace();
		} 


		 */




		//make_seqFile();	//to make file from the sequences table for input of simap

		//check_pep();
		//check bulk and thread... an example to 
		//CreateDatabaseTables.create_table_bulk();
		//Check_threadControl obj = new Check_threadControl();
		//obj.run();


		// TODO Auto-generated method stub
		//ExtractMembraneProteins obj_ExtractMembraneProteins = new ExtractMembraneProteins();	//generating table protein,sequences,databases_,taxonomies --> TM>1 !3
		//System.out.print("object made extracting proteins\n");
		//CreateDatabaseTables.create_table_proteins();
		//CreateDatabaseTables.create_table_sequences();
		//CreateDatabaseTables.create_table_taxonomies();
		//CreateDatabaseTables.create_table_databases();
		//ExtractMembraneProteins.run(); // to make proteins,sequences,taxonomies and databases_

		//for table tms & elements

		//ExtractTMSDataNew obj = new ExtractTMSDataNew(); 
		//ExtractPFAMData obj = new ExtractPFAMData();
		//ExtractTMSDataNew.make_mapping();

		//ExtractTMSDataNew.run();

		// ****************************CAMPS TO UNIPROT ***********************************
		
		//Camps2externalDBs_v2RUN obj = new Camps2externalDBs_v2RUN();
		
		//String c = args[0];
		//String c = "F:/tail.fasta";
		//String c = "F:/tempTrembl.fasta";
		//int n = Integer.parseInt(args[1]);
		//Camps2externalDBs_v2RUN.run_uniprot(c, n); //-- have used this because the other has problems after mapping
		
		// Maps calculated, put the data from file to camps2uniprot
		
		//String f = "/scratch/usman/mappingCamps/MAPPEDCAMPS_GEPARD";
		//String f = "F:/MappedTrembl_tProb";
		//Camps2externalDBs_v2.mapFileToCamps(f);
		
		// run for the .dat file to get taxonomy information
		//String swiss = "/tmp/uniprot_sprot.dat.gz";
		//String trm = "/tmp/uniprot_trembl.dat.gz";
		//Camps2externalDBs_v2.runAddTaxonomuid(swiss, trm);
		
		//String in = "F:/TmpSeqNotinCamps";
		//String in = "F:/sp_id3";
		//Scrap obj = new Scrap();
		//obj.run6(in);
		//obj.run4(in);
		//obj.run3();
		// 3800836
// ******************************************* Protein Merged & Taxonomies Merged *****************************
		
		//MergeProteinsTables.run();
		
		// ****************IMPORTANT CHECK **********************
		/*
		 * WARNING: Could not found taxonomyid 587201 in SIMAP!
        WARNING: Could not found taxonomyid 587202 in SIMAP!
        WARNING: Could not found taxonomyid 587203 in SIMAP!
.       WARNING: Could not found taxonomyid 1283342 in SIMAP!
....    WARNING: Could not found taxonomyid 1289385 in SIMAP!
....... WARNING: Could not found taxonomyid 1283347 in SIMAP!

		 */
		

		//ExtractGOAnnotation_v2RUN obj = new ExtractGOAnnotation_v2RUN();
		//obj.run_ExtractGOAnnotation_v2();

		//to extract similarity score

		//CreateDatabaseTables.create_table_alignments();
		//CreateDatabaseTables.create_table_alignments_nonTMS(); //no tms hits--those not mapped

		//ExtractSimilarityScores obj = new ExtractSimilarityScores(); //would use mapped_tms and no mapping
		//obj.run("D:/alignments"); 




		//to do mapping
		//CreateDatabaseTables.create_table_mapped_tms();
		//MakeIdMapping.run();


		//BufferedWriter out;

		//out = new BufferedWriter(new FileWriter("file.txt"));

		//out.write("[INFO]: Table 'proteins' is being filled."); //TIME LOG
		//Date a = Calendar.getInstance().getTime();
		//out.write("\n The start time for table protein to be filled is: " + a);
		//out.close();

		//obj.run();


	}
	
	
// This function was used to check the new proteins present in CAMPS4 as compared to CAMPS3
	/*
	public static void checkDb() throws SQLException{

		int statusCounter1 =0;
		Statement stmCamps4 = CAMPS_CONNECTION.createStatement();
		//Statement stmCamps3 = CAMPS_CONNECTION_old.createStatement();
		//stm1.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs1 = stmCamps4.executeQuery("SELECT md5 FROM sequences2");
		PreparedStatement pstmCamps3 = CAMPS_CONNECTION_old.prepareStatement("SELECT sequenceid FROM sequences WHERE md5=?");

		PreparedStatement pstmCamps4 = CAMPS_CONNECTION.prepareStatement("SELECT tms_id FROM tms WHERE md5=?");

		while(rs1.next()) {
			String md5 = rs1.getString("md5");
			pstmCamps3.setString(1, md5);
			ResultSet temp = pstmCamps3.executeQuery();
			if(!temp.isBeforeFirst()){
				//No Data So check if >3 TM segments
				pstmCamps4.setString(1, md5);
				ResultSet rs = pstmCamps4.executeQuery();
				int i =0;
				while (rs.next()){
					i++;
					if(i>3){
						System.out.print("\n Found    " +md5+"\n");
						rs.close();
						break;
					}
				}
			}
			
			if (statusCounter1 % 1000 == 0) {
				System.out.write('.');
				System.out.flush();
			}
			if (statusCounter1 % 100000 == 0) {
				System.out.write('\n');
				System.out.flush();
			}						
			statusCounter1++;	

			
		}
		rs1.close();
		pstmCamps4.close();
		pstmCamps3.close();
		CAMPS_CONNECTION_old.close();
		CAMPS_CONNECTION.close();

	}
*/

	/*
	private static void make_seqFile() throws SQLException, IOException {
		// TODO Auto-generated method stub
		int statusCounter1 = 0;
		Statement stm1 = CAMPS_CONNECTION.createStatement();
		//stm1.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs1 = stm1.executeQuery("SELECT sequenceid,sequence FROM sequences_old");

		String name = "camps_seq_file2";
		File file = new File("D:/camps_Seq/"+ name +".fasta");
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);

		while(rs1.next()) {

			int sequenceid = rs1.getInt("sequenceid");
			String sequence = rs1.getString("sequence");

			//System.out.print(">"+sequenceid+"\n");
			//System.out.print(sequence+"\n");
			if (sequence.length() == 0){
				System.out.print(">"+sequenceid+"\n");
			}
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			if(!sequence.isEmpty()){
				bw.write(">"+sequenceid);
				bw.newLine();

				if(sequence.length() > 80 || sequence.length() == 80){
					int x = sequence.length()/80;
					if (sequence.length()%80 > 0){
						x++;
					}

					while(x > 0){
						if (sequence.length()>80){
							bw.write(sequence.substring(0, 80));
							sequence = sequence.substring(80,sequence.length());
						}
						else
						{
							bw.write(sequence.substring(0, sequence.length()));
						}
						bw.newLine();
						x--;
					}
				}
				else if (sequence.length() < 80){
					bw.write(sequence);
				}

				bw.newLine();
			}

			if (statusCounter1 % 100 == 0) {
				System.out.write('.');
				System.out.flush();
			}
			if (statusCounter1 % 10000 == 0) {
				System.out.write('\n');
				System.out.flush();
			}						
			statusCounter1++;	
		}

		bw.close();

	}

	 */

}
