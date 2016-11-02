/*
 * ExtractDAClusterAssignments
 * 
 * Version 1.0
 * 
 * 2009-08-28
 * 
 */

package extract_proteins;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;

import utils.DBAdaptor;
import utils.IDMapping;
import workflow.Protein;

public class ExtractDAClusterAssignments {

	//private static final Connection SIMAPC_CONNECTION = DBAdaptor.getConnection("simapclusters");

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");

	//number of records for insertion of multiple rows at once
	private static final int MULTIROW_INSERT_SIZE = 1000;	
	
	private static ArrayList<String> files_results = new ArrayList<String>();
	
	private static ArrayList<String> files_input = new ArrayList<String>();
	
//	private static ArrayList<Integer> sequences = new ArrayList<Integer>();
	private static HashMap <Integer, Integer> sequences_table = new HashMap<Integer, Integer>();// to make it all fast

	// function which return all the files in the given folder
	public static ArrayList<String> listFilesForFolder(final File folder) {
		int count = 0; 
		ArrayList<String> files = new ArrayList<String>();
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				listFilesForFolder(fileEntry);
			} else {
				count++;
				files.add(fileEntry.getName().toString());
			}
		}
		System.out.println("\n total file " + count + " \n");
		return files;
		// all file names received
	}
	// the function makes file for the sequences which were not processed by interpro
	public static void make_file(){
		try{
			double CountX = 0;
			int batchNumber =1;
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("/home/users/saeed/FHClustering/Camps_"+batchNumber+".fasta")));
			PreparedStatement pstm_seq;
			
			for (int key: sequences_table.keySet()){
				pstm_seq = CAMPS_CONNECTION.prepareStatement("Select sequence from sequences2 where sequenceid=?");
				pstm_seq.setInt(1, key);
				ResultSet rsSeq = pstm_seq.executeQuery();
				while(rsSeq.next()){
					String s = rsSeq.getString(1);
					bw.write(">"+key+"\n");
					bw.write(s+"\n");
					CountX++;
					if (CountX%350==0){
						bw.close();
						batchNumber ++;
						bw = new BufferedWriter(new FileWriter(new File("/home/users/saeed/FHClustering/Camps_"+batchNumber+".fasta")));
						System.out.print("Done - "+batchNumber+"\n");
						System.out.flush();
					}
				}
				rsSeq.close();
				pstm_seq.close();
			}
			bw.close();
			System.out.print("\nFile written to process remaining entries:\n"+"F:/SC_Clust_postHmm/FHClustIssue/Camps_SkippedSeqs.fasta\n");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void run() {
		String filenPath = "";
		int lineCount = 0;
		String seqid = "";
		try {
			// get all input files to see which ones are not processed.
			// also make a list of all sequnces to see if any is not processed
			System.out.println("\t\t[INFO]: Starting to populate da_cluster_asignments \n");

			// Get the input file
			//String pathIn = "F:/SC_Clust_postHmm/FHClustIssue/in/inAll/";
			String pathIn = "/home/users/saeed/FHClustering/inAll/";
			files_input = listFilesForFolder(new File(pathIn));

			// Get the sequences
			PreparedStatement pstm_seq = CAMPS_CONNECTION.prepareStatement("Select sequenceid from sequences2");
			ResultSet rsSeq = pstm_seq.executeQuery();
			while(rsSeq.next()){
				int id = rsSeq.getInt(1);
				//sequences.add(id);
				sequences_table.put(id, null);
			}
			rsSeq.close();
			pstm_seq.close();
			
			//for(int i =0;i<=sequences.size()-1;i++){
				//System.out.print(sequences.get(i)+"\n");
			//}
			System.out.print("Number of Sequences "+sequences_table.size()+"\n");
			System.out.flush();


			PreparedStatement pr = CAMPS_CONNECTION.prepareStatement("INSERT INTO da_cluster_assignments" +
					" (sequenceid, begin, end, length, name, description,method) " +
					" VALUES "+
					"(?,?,?,?,?,?,?)"); 
			// list all files in folder
			//String pathOut = "F:/SC_Clust_postHmm/MDClustIssue/blah/";
			String pathOut = "/home/users/saeed/FHClustering/reRun/out/";
			//String pathOut = "/home/users/saeed/FHClustering/out/";

			files_results = listFilesForFolder(new File(pathOut));
			// Now have all the file names;
			// process all the result files and add them to the given table;
			
			for (int i =0;i<=files_results.size()-1;i++){
				String fname = files_results.get(i);
				filenPath = pathOut+fname;
				BufferedReader br = new BufferedReader(new FileReader(new File(filenPath)));
				String line = "";
				lineCount = 0;
				while((line =br.readLine())!=null){
					String parts[] = line.split("\t");
					/**
					 * 
0						Protein Accession (e.g. P51587)
1						Sequence MD5 digest (e.g. 14086411a2cdf1c4cba63020e1622579)
2						Sequence Length (e.g. 3418)
3						Analysis (e.g. Pfam / PRINTS / Gene3D)
4						Signature Accession (e.g. PF09103 / G3DSA:2.40.50.140)
5						Signature Description (e.g. BRCA2 repeat profile)
6						Start location
7						Stop location
8						Score - is the e-value of the match reported by member database method (e.g. 3.1E-52)
9						Status - is the status of the match (T: true)
10						Date - is the date of the run
						(InterPro annotations - accession (e.g. IPR002093) - optional column; only displayed if -iprscan option is switched on)
						(InterPro annotations - description (e.g. BRCA2 repeat) - optional column; only displayed if -iprscan option is switched on)
						(GO annotations (e.g. GO:0005515) - optional column; only displayed if --goterms option is switched on)
						(Pathways annotations (e.g. REACT_71) - optional column; only displayed if --pathways option is switched on)
					 */
					//1 acc,	3 length,	7 start, 8 end, 5 name, 6 Description
					seqid = parts[0];
					String len = parts[2];
					String beg = parts[6];
					String en = parts[7];
					String name = parts[4];	//String
					String description = parts[5];	//String
					String method = parts[3];	//String

					int sqid = Integer.parseInt(seqid.trim());
					int length = Integer.parseInt(len.trim());
					int begin = Integer.parseInt(beg.trim());
					int end = Integer.parseInt(en.trim());
					//(sequenceid, begin, end, length, name, description,method)
					pr.setInt(1, sqid);
					pr.setInt(2, begin);
					pr.setInt(3, end);
					pr.setInt(4, length);
					pr.setString(5, name);
					pr.setString(6, description);
					pr.setString(7, method);

					pr.addBatch();
					lineCount++;
					// remove the Sequence
					if (sequences_table.containsKey(sqid)){
						sequences_table.remove(sqid);
					}
					if (lineCount% MULTIROW_INSERT_SIZE == 0){
						pr.executeBatch();
						pr.clearBatch();
					}
				}
				//remove this file as it is processed and present
				System.out.print("Files Processed: "+i+"\n");
				files_input.remove(fname);
				br.close();
				pr.executeBatch();
				pr.clearBatch();
			}
			pr.executeBatch();
			pr.clearBatch();
			System.out.print("Sequences Not processed: \n");
			for (int key: sequences_table.keySet()){
				System.out.print(key+"\n");
			}
			
			System.out.print("Files Not processed: \n");
			for (int i = 0;i<=files_input.size()-1;i++){
				System.out.print(files_input.get(i)+"\n");
			}
			if (sequences_table.size()>0){
				//make_file();
			}
			pr.close();
		} catch(Exception e) {
			e.printStackTrace();
			System.out.print("\n"+seqid+"\t"+filenPath+"\n");
		} finally {
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}			
		}
	}

}
