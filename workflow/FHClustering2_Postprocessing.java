/*
 * FHClustering2_Postprocessing
 * 
 * Version 1.0
 * 
 * 2011-01-20
 * 
 */

package workflow;

import general.CreateDatabaseTables;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import properties.ConfigFile;

import utils.DBAdaptor;
import utils.FastaReader;
import utils.FastaReader.FastaEntry;



/**
 * 
 * @author sneumann
 * 
 * Processes FH clusters (version 2) after their generation:
 * 
 * - homology cleanup: removal of redundant sequences
 * - cluster properties: computation of different cluster properties
 * - extraction of TMS cores and blocks
 * - association to different external DBs (clusters cross db)
 * - association to PDB structures
 *
 */
public class FHClustering2_Postprocessing {


	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");

	//private static final Connection SIMAP_CONNECTION = DBAdaptor.getConnection("simap");

	private static final int BLOB_SIZE = 10000;



	//sequence identity threshold for homology cleanup
	private static final double SIMILARITY_THRESHOLD = 0.8;

	//word length for cd-hit program (see cd-hit manual for further information)
	//
	//Recommended:
	//word size=5 for thresholds 0.7 - 1.0
	//word size=4 for thresholds 0.6 - 0.7
	//word size=3 for thresholds 0.5 - 0.6
	//word size=2 for thresholds 0.4 - 0.5
	private static final int WORD_LENGTH = 5;



	////////////////////////////////////////////////////////////////////////////
	//																		  //
	//				filter parameters for TMS core extraction 				  //
	//      																  //
	////////////////////////////////////////////////////////////////////////////

	//minimal number of cluster members for core extraction
	//private static final int MINIMUM_CLUSTER_SIZE = 15;
	private static final int MINIMUM_CLUSTER_SIZE = 2;

	//minimal percentage coverage of aligned sequences to form TMS core
	private static final double MIN_COVERAGE_ALIGNMENT = 35;

	//number of residues the core is extended at both sides to form TMS block
	private static final int CORE_EXTENSION = 7;

	private static final int MINIMUM_NUMBER_CORES = 1;

	//sequences longer than 2000 are ignored
	private static final int MAX_SEQUENCE_LENGTH = 2000;



	////////////////////////////////////////////////////////////////////////////
	//																		  //
	//				filter parameters for cross db           				  //
	//      																  //
	////////////////////////////////////////////////////////////////////////////

	private static final double CROSS_COVERAGE_THRESHOLD = 70;



	////////////////////////////////////////////////////////////////////////////
	//																		  //
	//				filter parameters for structures           				  //
	//      																  //
	////////////////////////////////////////////////////////////////////////////

	//private static final float QUERY_COVERAGE_THRESHOLD = 90;
	//private static final float HIT_COVERAGE_THRESHOLD = 90;
	//private static final float IDENTITY_THRESHOLD = 90;
	private static final float QUERY_COVERAGE_THRESHOLD = 30;
	private static final float HIT_COVERAGE_THRESHOLD = 30;
	private static final float IDENTITY_THRESHOLD = 30;

	//private static final String BASE_URL = "http://www.pdb.org/pdb/files/";
	//http://www.rcsb.org/pdb/files/

	//private static final String BASE_URL = "http://www.rcsb.org/pdb/files/";
	private static final String BASE_Address = "/home/users/saeed/pdbtest/pdb/";

	private static final Pattern RESOLUTION_PATTERN = Pattern.compile("REMARK\\s+\\d+\\s+RESOLUTION\\.\\s+(\\d+\\.\\d+)\\s+ANGSTROMS\\.");



	/**
	 * Performs homology cleanup on the fh clusters and stores information in db. 
	 */
	public void doHomologyCleanup() {		
		try {	


			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM fh_clusters WHERE code=?");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT sequence FROM sequences2 WHERE sequenceid=? limit 1");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("UPDATE fh_clusters SET redundant=\"No\" WHERE code=? AND sequenceid=?");

			Statement stm = CAMPS_CONNECTION.createStatement();


			int counter = 0;
			//ResultSet rs = stm.executeQuery("SELECT code FROM cp_clusters WHERE type=\"fh_cluster\"");
			//select * from cp_clusters where type="fh_cluster" and id >=7425
			// because the last run failed here
			//processing issue.. program freezes for CMSC0183_FH008
			// CMSC0183_FH008
			ResultSet rs = stm.executeQuery("SELECT code FROM cp_clusters WHERE type=\"fh_cluster\" AND id>7427");
			while(rs.next()) {

				counter++;

				String code = rs.getString("code");


				//get distinct sequences for each cluster
				System.out.println("\n\n\t[INFO] Get cluster members for: "+code+"  ("+counter+")");
				BitSet sequenceIDs = new BitSet();
				pstm1.setString(1, code);		
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {					
					int sequenceid = rs1.getInt("sequenceid");
					sequenceIDs.set(sequenceid);					
				}
				rs1.close();
				rs1 = null;


				//write tmp FASTA file for cd-hit
				File cdhitInfileTmp = File.createTempFile("cluster_"+code, ".fasta");
				PrintWriter pw = new PrintWriter(new FileWriter(cdhitInfileTmp));

				for(int sequenceid = sequenceIDs.nextSetBit(0); sequenceid>=0; sequenceid = sequenceIDs.nextSetBit(sequenceid+1)) {

					String sequence = "";
					pstm2.setInt(1, sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						sequence = rs2.getString("sequence").toUpperCase();

						pw.println(">"+sequenceid+"\n"+sequence);
					}
					rs2.close();
					rs2 = null;

				}

				pw.close();

				int numSeqs = sequenceIDs.cardinality();


				//run cd-hit
				System.out.println("\t[INFO] Run cd-hit");
				File cdhitOutfileTmp = File.createTempFile("cluster_"+code+"_NR", ".fasta");
				ConfigFile cf = new ConfigFile("/home/users/saeed/workspace/CAMPS3/config/config.xml");
				String cdhitDir = cf.getProperty("apps:cdhit:install-dir");
				// similarity threshold of 0.8 default was 0.9, i.e no of identical aa. word length of 5-> default is 5, alignment coverage 0.8 i.e 80% of the sequence is covered by the alignment
				///home/users/saeed/apps/cd-hit/cd-hit-v4.6.1-2012-08-27
				String cdhitCmd = cdhitDir + " -i " +cdhitInfileTmp.getAbsolutePath() + " -o " +cdhitOutfileTmp.getAbsolutePath() + " -c " + SIMILARITY_THRESHOLD + " -n " + WORD_LENGTH + " -aL 0.7 -aS 0.8" + " -T 0 ";
				System.out.println(cdhitCmd);
				Process p = Runtime.getRuntime().exec(cdhitCmd);
				p.waitFor();
				p.destroy();

				//read out nonredundant sequences
				BitSet nrSequenceIDs = new BitSet();
				FastaReader fr = new FastaReader(cdhitOutfileTmp);
				ArrayList<FastaEntry> entries = fr.getEntries();
				for(FastaEntry entry: entries) {

					int sequenceid = Integer.parseInt(entry.getHeader());					
					nrSequenceIDs.set(sequenceid);
				}
				entries = null;

				int numSeqsNR = nrSequenceIDs.cardinality();
				System.out.println("\t[INFO] Number of sequences: " +numSeqs+", Number of nonredundant sequences: " +numSeqsNR);


				//update database table
				System.out.println("\t[INFO] Update MySQL database");
				for(int sequenceid = nrSequenceIDs.nextSetBit(0); sequenceid>=0; sequenceid = nrSequenceIDs.nextSetBit(sequenceid+1)) {

					pstm3.setString(1, code);
					pstm3.setInt(2, sequenceid);

					pstm3.executeUpdate();

				}

				//remove tmp files
				//new File(cdhitOutfileTmp.getAbsoluteFile()+".bak.clstr").delete();
				new File(cdhitOutfileTmp.getAbsoluteFile()+".clstr").delete();
				cdhitInfileTmp.delete();
				cdhitOutfileTmp.delete();



			}
			rs.close();

			stm.close();

			pstm1.close();
			pstm1 = null;

			pstm2.close();
			pstm2 = null;

			pstm3.close();
			pstm3 = null;


		} catch(Exception e) {
			System.err.println("Exception in FHClustering2_Postprocessing.doHomologyCleanup(): " +e.getMessage());
			e.printStackTrace();

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


	/**
	 * Computes cluster properties for fh clusters in the same as for
	 * the initial MCL clusters.
	 */
	public void computeClusterProperties() {

		try {

			int batchSize = 50;
			int batchCounter = 0;

			//
			//get sequence lengths for all sequences
			//
			System.out.println("\n\t[INFO] Get sequence lengths");
			int maxSequenceid = 0;
			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT max(sequenceid) FROM sequences2");
			while(rs.next()) {
				maxSequenceid = rs.getInt("max(sequenceid)");
			}
			rs.close();
			rs = null;
			stm.close();
			stm = null;

			int[] sequenceids2length = new int[maxSequenceid+1];
			stm = CAMPS_CONNECTION.createStatement();
			stm.setFetchSize(Integer.MIN_VALUE);
			rs = stm.executeQuery("SELECT sequenceid, length FROM sequences2");
			while(rs.next()) {
				int sequenceid = rs.getInt("sequenceid");
				int length = rs.getInt("length");
				sequenceids2length[sequenceid] = length;
			}
			rs.close();
			rs = null;
			stm.close();
			stm = null;
			System.out.println("\t...DONE");

			//
			//get number of TMS for all sequences
			//
			System.out.println("\n\t[INFO] Get number of TMS");
			int[] sequenceids2numTms = new int[maxSequenceid+1];
			stm = CAMPS_CONNECTION.createStatement();
			stm.setFetchSize(Integer.MIN_VALUE);
			rs = stm.executeQuery("SELECT sequenceid FROM tms");
			while(rs.next()) {
				int sequenceid = rs.getInt("sequenceid");
				sequenceids2numTms[sequenceid]++;
			}
			rs.close();
			rs = null;
			stm.close();
			stm = null;
			System.out.println("\t...DONE");

			//
			//get maximal number of TMS for all clusters
			//
			int globalMaxNumTms=100;			
			stm = CAMPS_CONNECTION.createStatement();
			rs = stm.executeQuery("SELECT max(tms_id) FROM tms");
			while(rs.next()) {
				globalMaxNumTms = rs.getInt("max(tms_id)");
			}
			rs.close();
			rs = null;
			stm.close();
			stm = null;



			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid" +
							" FROM fh_clusters" +
					" WHERE code=?");	

			//			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
			//					"INSERT INTO fh_clusters_info" +
			//					" (code," +
			//					" sequences, average_length," +
			//					" tms_range, structural_homogeneity," +
			//					" sequences_with_GOA, functional_homogeneity," +
			//					" proteins, proteins_archaea, proteins_bacteria, proteins_eukaryota, proteins_viruses, proteins_misc) " +
			//					"VALUES " +
			//					"(?,?,?,?,?,?,?,?,?,?,?,?,?)");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_info" +
							" (code," +
							" sequences, average_length," +
							" tms_range, structural_homogeneity," +					
							" proteins, proteins_archaea, proteins_bacteria, proteins_eukaryota, proteins_viruses, proteins_misc) " +
							"VALUES " +
					"(?,?,?,?,?,?,?,?,?,?,?)");


			int counter = 0;
			stm = CAMPS_CONNECTION.createStatement();
			rs = stm.executeQuery("SELECT code FROM cp_clusters WHERE type=\"fh_cluster\"");
			while(rs.next()) {

				counter++;

				String code = rs.getString("code");


				if (counter % 100 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (counter % 10000 == 0) {					
					System.out.write('\n');
					System.out.flush();
				}

				//get distinct sequences for each cluster
				BitSet clusterMembers = new BitSet();
				pstm1.setString(1, code);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					int sequenceid = rs1.getInt("sequenceid");
					clusterMembers.set(sequenceid);
				}
				rs1.close();
				rs1 = null;

				//
				//get TMS and sequence length information
				//
				int cumulativeLength = 0;

				//numTms2count: index: number TMS, value: number of occurrences
				int[] numTms2count = new int[globalMaxNumTms+1];		

				for(int sequenceid = clusterMembers.nextSetBit(0); sequenceid>=0; sequenceid = clusterMembers.nextSetBit(sequenceid+1)) {

					//get sequence length
					int sequenceLength = sequenceids2length[sequenceid];
					cumulativeLength += sequenceLength;

					//get number of tms
					int numTms = sequenceids2numTms[sequenceid];
					numTms2count[numTms]++;

				}

				int numberSequences = clusterMembers.cardinality();
				int averageLength = (int) ((cumulativeLength/numberSequences) + 0.5);

				//
				//get TMS range and structural homogeneity 
				//
				Hashtable<String,String> result1 = getStructuralHomogeneity(numTms2count, numberSequences);
				//				
				String tmsRange = result1.get("tms_range");
				float structuralHomogeneity = Float.parseFloat(result1.get("structural_homogeneity"));


				//
				//get number of GO annotated sequences and functional homogeneity 
				//
				//					Hashtable<String,String> result2 = getFunctionalHomogeneity(clusterMembers);
				//					//
				//					int numSeqsWithGOA = Integer.parseInt(result2.get("annotated_sequences"));
				//					float functionalHomogeneity = Float.parseFloat(result2.get("functional_homogeneity"));


				//
				//get kingdom distribution
				//
				Hashtable<String,Integer> result3 = getSuperkingdomDistribution(clusterMembers);
				//
				int numberProteins = result3.get("All");
				int numberProteinsArchaea = result3.get("Archaea");
				int numberProteinsBacteria = result3.get("Bacteria");
				int numberProteinsEukaryota = result3.get("Eukaryota");
				int numberProteinsViruses = result3.get("Viruses");
				int numberProteinsMisc = result3.get("Misc");


				//skip blobs
				if(numberSequences > BLOB_SIZE) {
					tmsRange = "NA";	//NA: not available
					structuralHomogeneity = 0;					
				}


				//					//insert all information into database
				//					batchCounter++;
				//					pstm2.setString(1, code);
				//					pstm2.setInt(2, numberSequences);
				//					pstm2.setInt(3, averageLength);
				//					pstm2.setString(4, tmsRange);
				//					pstm2.setFloat(5, structuralHomogeneity);
				//					pstm2.setInt(6, numSeqsWithGOA);
				//					pstm2.setFloat(7, functionalHomogeneity);
				//					pstm2.setInt(8, numberProteins);
				//					pstm2.setInt(9, numberProteinsArchaea);
				//					pstm2.setInt(10, numberProteinsBacteria);
				//					pstm2.setInt(11, numberProteinsEukaryota);
				//					pstm2.setInt(12, numberProteinsViruses);
				//				    pstm2.setInt(13, numberProteinsMisc);

				//insert all information into database
				batchCounter++;
				pstm2.setString(1, code);
				pstm2.setInt(2, numberSequences);
				pstm2.setInt(3, averageLength);
				pstm2.setString(4, tmsRange);
				pstm2.setFloat(5, structuralHomogeneity);					
				pstm2.setInt(6, numberProteins);
				pstm2.setInt(7, numberProteinsArchaea);
				pstm2.setInt(8, numberProteinsBacteria);
				pstm2.setInt(9, numberProteinsEukaryota);
				pstm2.setInt(10, numberProteinsViruses);
				pstm2.setInt(11, numberProteinsMisc);

				pstm2.addBatch();

				if(batchCounter % batchSize == 0) {								
					pstm2.executeBatch();
					pstm2.clearBatch();
				}
			}
			rs.close();

			stm.close();
			stm = null;

			pstm1.close();
			pstm1 = null;	
			pstm2.executeBatch();		//add remaining entries
			pstm2.clearBatch();
			pstm2.close();
			pstm2 = null;	


		}
		catch(Exception e) {
			System.err.println("Exception in FHClustering2_Postprocessing.computeClusterProperties(): " +e.getMessage());
			e.printStackTrace();

		} finally {
			/*if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}	
			 /*
			if (SIMAP_CONNECTION != null) {
				try {
					SIMAP_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}*/
		}		
	}


	private static Hashtable<String,String> getStructuralHomogeneity(int[] numTms2count, int numSequences) {

		Hashtable<String,String> result = null;

		try {

			result = new Hashtable<String,String>();

			//compute TMS range and structural homogeneity
			int maxNumTmsCount = 0;
			int favoriteNumTms = 0;				//number of TMH with highest occurrence

			for(int i=0; i<numTms2count.length; i++) {
				int numTms = i;
				int count = numTms2count[i];					

				//System.out.println("\t"+numTms+"\t#"+count);

				if(count > maxNumTmsCount) {
					maxNumTmsCount = count;
					favoriteNumTms = numTms;
				}
			}

			int lessTmsAdmitted = favoriteNumTms/5;
			if(lessTmsAdmitted == 0) {
				lessTmsAdmitted = 1;
			}

			int minNumTms = favoriteNumTms - lessTmsAdmitted;
			int maxNumTms = favoriteNumTms + 1;

			int tmsWithinRange = 0;
			for(int i = minNumTms; i <= maxNumTms; i++) {
				if(i < numTms2count.length) {
					tmsWithinRange += numTms2count[i];
				}					
			}

			String tmsRange = minNumTms+"-"+maxNumTms;
			float structuralHomogeneity = tmsWithinRange/((float) numSequences);

			//round structural homogeneity value
			BigDecimal bd1 = new BigDecimal(structuralHomogeneity);				
			bd1 = bd1.setScale(2, BigDecimal.ROUND_HALF_UP);
			structuralHomogeneity = bd1.floatValue();

			result.put("tms_range", tmsRange);
			result.put("structural_homogeneity", String.valueOf(structuralHomogeneity));

		}catch(Exception e) {
			e.printStackTrace();
		}

		return result;
	}


	/*
	 * Calculates superkingdom distribution for specified cluster.
	 */
	private static Hashtable<String,Integer> getSuperkingdomDistribution(BitSet clusterMembers) {

		Hashtable<String,Integer> superkingdom2count = null;

		try {		

			superkingdom2count = new Hashtable<String,Integer>();

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT distinct(taxonomyid) FROM proteins_merged WHERE sequenceid=?");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT superkingdom FROM taxonomies_merged WHERE taxonomyid=?");

			int countAllProteins = 0;
			int countArchaea = 0;
			int countBacteria = 0;
			int countEukaryota = 0;
			int countViruses = 0;
			int countMisc = 0;

			for(int sequenceID = clusterMembers.nextSetBit(0); sequenceID>=0; sequenceID = clusterMembers.nextSetBit(sequenceID+1)) {	

				//
				//note: one sequenceid can give several taxonomy ids!!!
				//
				pstm1.setInt(1, sequenceID);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					int taxonomyID = rs1.getInt("taxonomyid");

					countAllProteins++;

					String superkingdom = "";
					pstm2.setInt(1, taxonomyID);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {

						superkingdom = rs2.getString("superkingdom");
						if(superkingdom == null) {
							countMisc++;
							continue;
						}

						if(superkingdom.equals("Archaea")) {
							countArchaea++;
						}
						else if(superkingdom.equals("Bacteria")) {
							countBacteria++;
						}
						else if(superkingdom.equals("Eukaryota")) {
							countEukaryota++;
						}
						else if(superkingdom.equals("Viruses")) {
							countViruses++;
						}
						else {
							countMisc++;
						}
					}
					rs2.close();				
				}
				rs1.close();				

			}

			pstm1.close();
			pstm2.close();


			superkingdom2count.put("All", Integer.valueOf(countAllProteins));	
			superkingdom2count.put("Archaea", Integer.valueOf(countArchaea));
			superkingdom2count.put("Bacteria", Integer.valueOf(countBacteria));
			superkingdom2count.put("Eukaryota", Integer.valueOf(countEukaryota));
			superkingdom2count.put("Viruses", Integer.valueOf(countViruses));
			superkingdom2count.put("Misc", Integer.valueOf(countMisc));

		} catch(Exception e) {
			e.printStackTrace();
		}

		return superkingdom2count;
	}


	private static Hashtable<String,String> getFunctionalHomogeneity(BitSet clusterMembers) {

		Hashtable<String,String> result = null;

		try {

			result = new Hashtable<String,String>();

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT md5 FROM sequences2 WHERE sequenceid=?");
			//PreparedStatement pstm2 = SIMAP_CONNECTION.prepareStatement("SELECT sequenceid FROM sequence2 WHERE md5=?");
			PreparedStatement pstm2 =null;

			StringBuffer sb = new StringBuffer();

			for(int campsSequenceID = clusterMembers.nextSetBit(0); campsSequenceID>=0; campsSequenceID = clusterMembers.nextSetBit(campsSequenceID+1)) {			

				String md5 = "";
				pstm1.setInt(1, campsSequenceID);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					md5 = rs1.getString("md5");
				}
				rs1.close();

				int simapSequenceID = -1;
				pstm2.setString(1, md5);	
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()) {
					simapSequenceID = rs2.getInt("sequenceid");
				}
				rs2.close();

				if(simapSequenceID != -1) {
					sb.append("\t"+simapSequenceID);
				}
				else {
					System.out.println("\n\t[WARNING] Could not get SIMAP id for " + campsSequenceID);
				}

			}

			pstm1.close();
			pstm2.close();


			//write temp file
			File inputTmp = File.createTempFile("calcFunctHom_input", ".txt");

			PrintWriter pw1 = new PrintWriter(new FileWriter(inputTmp));
			pw1.println(sb.toString().trim());
			pw1.close();

			File outputTmp = File.createTempFile("calcFunctHom_output", ".txt");

			//run script to calculate functional homogeneity
			ConfigFile cf = new ConfigFile("/home/users/sneumann/workspace/CAMPS3/config/config.xml");
			String javaStr = cf.getProperty("apps:java:install-dir");
			File tmp = new File(cf.getProperty("apps:functionalHomogeneity:src-dir"));
			String functHomDir = tmp.getParent() + "/";
			String functHomFile = tmp.getName();

			File shTmp = File.createTempFile("calcFunctHom",".sh");
			PrintWriter pw2 = new PrintWriter(new FileWriter(shTmp));
			pw2.println("cd " +functHomDir);
			pw2.println(javaStr+" -jar "+functHomFile + " " +inputTmp.getAbsolutePath() + " " + outputTmp.getAbsolutePath());
			pw2.close();

			String cmd = "sh " +shTmp.getAbsolutePath();

			Process p = Runtime.getRuntime().exec(cmd);
			BufferedReader br1 = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line1;
			while((line1=br1.readLine()) != null) {
				//System.err.println("\t\t[INFO]"+line);
			}
			br1.close();

			p.waitFor();
			p.destroy();

			//parse result file
			BufferedReader br2 = new BufferedReader(new FileReader(outputTmp));
			String line2;
			while((line2=br2.readLine()) != null) {
				if(line2.startsWith("#") || line2.trim().equals("")) {
					continue;
				}

				String[] content = line2.split("\t");

				String numAnnotations = content[1];
				String functionHomogeneity = content[2];

				result.put("annotated_sequences", numAnnotations);
				result.put("functional_homogeneity", functionHomogeneity);
			}
			br2.close();

			//remove temp files
			inputTmp.delete();
			outputTmp.delete();
			shTmp.delete();

		}catch(Exception e) {
			e.printStackTrace();
		}

		return result;
	}


	/*
	 * Add superkingdom composition to each fh cluster.
	 */
	public static void addSuperkingdomComposition1() {
		try {			

			Statement stm = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery(
					"SELECT id, " +
							"proteins_archaea, proteins_bacteria, proteins_eukaryota, proteins_viruses, proteins_misc, superkingdom_composition " +
					"FROM fh_clusters_info FOR UPDATE");
			int statusCounter = 0;
			while(rs.next()) {

				statusCounter++;
				if (statusCounter % 1000 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter % 100000 == 0) {
					System.out.write('\n');
					System.out.flush();
				}

				int proteinsArchaea = rs.getInt("proteins_archaea");
				int proteinsBacteria = rs.getInt("proteins_bacteria");
				int proteinsEukaryota = rs.getInt("proteins_eukaryota");
				int proteinsViruses = rs.getInt("proteins_viruses");
				//int proteinsMisc = rs.getInt("proteins_misc");

				String superkingdomComposition = "";
				if(proteinsArchaea > 0) {
					superkingdomComposition += ", Archaea";
				}
				if(proteinsBacteria > 0) {
					superkingdomComposition += ", Bacteria";
				}
				if(proteinsEukaryota > 0) {
					superkingdomComposition += ", Eukaryota";
				}
				if(proteinsViruses > 0) {
					superkingdomComposition += ", Viruses";
				}
				superkingdomComposition = superkingdomComposition.substring(2);

				rs.updateString("superkingdom_composition", superkingdomComposition);
				rs.updateRow();

			}
			rs.close();


		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			/*
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			 */
		}
	}


	/**
	 * Computes cluster properties for nonredundant fh clusters in the same as for
	 * the initial MCL clusters.
	 */
	public void computeClusterProperties_NR() {

		try {

			int batchSize = 50;
			int batchCounter = 0;

			//
			//get sequence lengths for all sequences
			//
			System.out.println("\n\t[INFO] Get sequence lengths");
			int maxSequenceid = 0;

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT max(sequenceid) FROM sequences2");
			while(rs.next()) {
				maxSequenceid = rs.getInt("max(sequenceid)");
			}
			rs.close();
			rs = null;
			stm.close();
			stm = null;

			int[] sequenceids2length = new int[maxSequenceid+1];
			stm = CAMPS_CONNECTION.createStatement();
			stm.setFetchSize(Integer.MIN_VALUE);
			rs = stm.executeQuery("SELECT sequenceid, length FROM sequences2");
			while(rs.next()) {
				int sequenceid = rs.getInt("sequenceid");
				int length = rs.getInt("length");
				sequenceids2length[sequenceid] = length;
			}
			rs.close();
			rs = null;
			stm.close();
			stm = null;
			System.out.println("\t...DONE");

			//
			//get number of TMS for all sequences
			//
			System.out.println("\n\t[INFO] Get number of TMS");
			int[] sequenceids2numTms = new int[maxSequenceid+1];
			stm = CAMPS_CONNECTION.createStatement();
			stm.setFetchSize(Integer.MIN_VALUE);
			rs = stm.executeQuery("SELECT sequenceid FROM tms");
			while(rs.next()) {
				int sequenceid = rs.getInt("sequenceid");
				sequenceids2numTms[sequenceid]++;
			}
			rs.close();
			rs = null;
			stm.close();
			stm = null;
			System.out.println("\t...DONE");

			//
			//get maximal number of TMS for all clusters
			//
			int globalMaxNumTms=100;			
			stm = CAMPS_CONNECTION.createStatement();
			rs = stm.executeQuery("SELECT max(tms_id) FROM tms");
			while(rs.next()) {
				globalMaxNumTms = rs.getInt("max(tms_id)");
			}
			rs.close();
			rs = null;
			stm.close();
			stm = null;



			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid" +
							" FROM fh_clusters" +
					" WHERE code=? AND redundant=\"No\"");	

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_nr_info" +
							" (code," +
							" sequences, average_length," +
							" tms_range, structural_homogeneity," +
							" sequences_with_GOA, functional_homogeneity," +
							" proteins, proteins_archaea, proteins_bacteria, proteins_eukaryota, proteins_viruses, proteins_misc) " +
							"VALUES " +
					"(?,?,?,?,?,?,?,?,?,?,?,?,?)");


			int counter = 0;
			stm = CAMPS_CONNECTION.createStatement();
			rs = stm.executeQuery("SELECT code FROM cp_clusters WHERE type=\"fh_cluster\"");
			while(rs.next()) {

				counter++;

				String code = rs.getString("code");


				if (counter % 100 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (counter % 10000 == 0) {					
					System.out.write('\n');
					System.out.flush();
				}

				//get distinct sequences for each cluster
				BitSet clusterMembers = new BitSet();
				pstm1.setString(1, code);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					int sequenceid = rs1.getInt("sequenceid");
					clusterMembers.set(sequenceid);
				}
				rs1.close();
				rs1 = null;

				//
				//get TMS and sequence length information
				//
				int cumulativeLength = 0;

				//numTms2count: index: number TMS, value: number of occurrences
				int[] numTms2count = new int[globalMaxNumTms+1];		

				for(int sequenceid = clusterMembers.nextSetBit(0); sequenceid>=0; sequenceid = clusterMembers.nextSetBit(sequenceid+1)) {

					//get sequence length
					int sequenceLength = sequenceids2length[sequenceid];
					cumulativeLength += sequenceLength;

					//get number of tms
					int numTms = sequenceids2numTms[sequenceid];
					numTms2count[numTms]++;

				}

				int numberSequences = clusterMembers.cardinality();
				int averageLength = (int) ((cumulativeLength/numberSequences) + 0.5);

				//
				//get TMS range and structural homogeneity 
				//
				Hashtable<String,String> result1 = getStructuralHomogeneity(numTms2count, numberSequences);
				//				
				String tmsRange = result1.get("tms_range");
				float structuralHomogeneity = Float.parseFloat(result1.get("structural_homogeneity"));


				//
				//get number of GO annotated sequences and functional homogeneity 
				//
				//Hashtable<String,String> result2 = getFunctionalHomogeneity(clusterMembers);
				//
				//int numSeqsWithGOA = Integer.parseInt(result2.get("annotated_sequences"));
				//float functionalHomogeneity = Float.parseFloat(result2.get("functional_homogeneity"));


				//
				//get kingdom distribution
				//
				Hashtable<String,Integer> result3 = getSuperkingdomDistribution(clusterMembers);
				//
				int numberProteins = result3.get("All");
				int numberProteinsArchaea = result3.get("Archaea");
				int numberProteinsBacteria = result3.get("Bacteria");
				int numberProteinsEukaryota = result3.get("Eukaryota");
				int numberProteinsViruses = result3.get("Viruses");
				int numberProteinsMisc = result3.get("Misc");


				//skip blobs
				if(numberSequences > BLOB_SIZE) {
					tmsRange = "NA";	//NA: not available
					structuralHomogeneity = 0;					
				}


				//insert all information into database
				batchCounter++;
				pstm2.setString(1, code);
				pstm2.setInt(2, numberSequences);
				pstm2.setInt(3, averageLength);
				pstm2.setString(4, tmsRange);
				pstm2.setFloat(5, structuralHomogeneity);
				//pstm2.setInt(6, numSeqsWithGOA);
				//pstm2.setFloat(7, functionalHomogeneity);
				pstm2.setInt(6, -1);
				pstm2.setFloat(7, -1);
				pstm2.setInt(8, numberProteins);
				pstm2.setInt(9, numberProteinsArchaea);
				pstm2.setInt(10, numberProteinsBacteria);
				pstm2.setInt(11, numberProteinsEukaryota);
				pstm2.setInt(12, numberProteinsViruses);
				pstm2.setInt(13, numberProteinsMisc);

				pstm2.addBatch();

				if(batchCounter % batchSize == 0) {								
					pstm2.executeBatch();
					pstm2.clearBatch();
				}
			}
			rs.close();

			stm.close();
			stm = null;

			pstm1.close();
			pstm1 = null;	
			pstm2.executeBatch();		//add remaining entries
			pstm2.clearBatch();
			pstm2.close();
			pstm2 = null;	


		}
		catch(Exception e) {
			System.err.println("Exception in FHClustering2_Postprocessing.computeClusterProperties_NR(): " +e.getMessage());
			e.printStackTrace();

		} finally {
			/*
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			 /*
			if (SIMAP_CONNECTION != null) {
				try {
					SIMAP_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}*/
		}		
	}


	/*
	 * Add superkingdom composition to each fh cluster.
	 */
	public static void addSuperkingdomComposition2() {
		try {			

			Statement stm = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery(
					"SELECT id, " +
							"proteins_archaea, proteins_bacteria, proteins_eukaryota, proteins_viruses, proteins_misc, superkingdom_composition " +
					"FROM fh_clusters_nr_info FOR UPDATE");
			int statusCounter = 0;
			while(rs.next()) {

				statusCounter++;
				if (statusCounter % 1000 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter % 100000 == 0) {
					System.out.write('\n');
					System.out.flush();
				}

				int proteinsArchaea = rs.getInt("proteins_archaea");
				int proteinsBacteria = rs.getInt("proteins_bacteria");
				int proteinsEukaryota = rs.getInt("proteins_eukaryota");
				int proteinsViruses = rs.getInt("proteins_viruses");
				//int proteinsMisc = rs.getInt("proteins_misc");

				String superkingdomComposition = "";
				if(proteinsArchaea > 0) {
					superkingdomComposition += ", Archaea";
				}
				if(proteinsBacteria > 0) {
					superkingdomComposition += ", Bacteria";
				}
				if(proteinsEukaryota > 0) {
					superkingdomComposition += ", Eukaryota";
				}
				if(proteinsViruses > 0) {
					superkingdomComposition += ", Viruses";
				}
				superkingdomComposition = superkingdomComposition.substring(2);

				rs.updateString("superkingdom_composition", superkingdomComposition);
				rs.updateRow();

			}
			rs.close();


		} catch(Exception e) {
			e.printStackTrace();
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


	//	public static void findRepresentativeDomainArchitecture() {
	//		try {
	//			
	//			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
	//					"SELECT sequences_sequenceid FROM fh_clusters WHERE code=?");
	//			
	//			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
	//					"SELECT accession,subtype FROM " +
	//					"domains_pfam " +
	//					"WHERE sequences_sequenceid=? ORDER BY begin");
	//			
	//			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
	//					"INSERT INTO fh_clusters_nr_da " +
	//					"(code,domain_architecture,simap_id,frequency_all_members,frequency_members_with_pfam_domain) " +
	//					"VALUES " +
	//					"(?,?,?,?,?)");
	//						
	//			
	//			Statement stm = CAMPS_CONNECTION.createStatement();
	//			ResultSet rs = stm.executeQuery("SELECT code,comment FROM cp_clusters WHERE type=\"fh_cluster_v2\"");
	//			while(rs.next()) {
	//				
	//				String code = rs.getString("code");
	//				String comment = rs.getString("comment");
	//								
	//				BitSet members = new BitSet();				
	//				pstm1.setString(1, code);
	//				ResultSet rs1 = pstm1.executeQuery();
	//				while(rs1.next()) {
	//					
	//					int sequenceid = rs1.getInt("sequences_sequenceid");
	//					members.set(sequenceid);
	//				}
	//				rs1.close();
	//				
	////				if(members.cardinality() < 8) {
	////					continue;
	////				}
	//				
	//				//
	//				//get PFAM domains
	//				//
	//				int countMembersWithSignature = 0;
	//				Hashtable<String,Integer> signature2count = new Hashtable<String,Integer>();
	//				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {
	//					
	//					String signature = "";
	//					
	//					pstm2.setInt(1, sequenceid);
	//					ResultSet rs2 = pstm2.executeQuery();
	//					while(rs2.next()) {
	//						
	//						String accession = rs2.getString("accession");
	//						signature += " - "+accession;					
	//					}
	//					rs2.close();
	//					
	//					if(!signature.isEmpty()) {
	//						countMembersWithSignature++;
	//						
	//						signature = signature.substring(3);
	//						
	//						int count = 0;
	//						if(signature2count.containsKey(signature)) {
	//							count = signature2count.get(signature).intValue();
	//						}
	//						
	//						count = count+1;
	//						signature2count.put(signature, Integer.valueOf(count));
	//					}				
	//				}
	//				
	//				
	//				double coverageAllMembersRounded;
	//				double coverageAllMembersWithSignatureRounded;
	//				String da = "NA";
	//				
	//				if(countMembersWithSignature == 0) {
	//					coverageAllMembersRounded = 0;
	//					coverageAllMembersWithSignatureRounded = 0;
	//				}
	//				else {
	//					//get signature with most occurrences
	//					int max = Integer.MIN_VALUE;
	//					String signatureMax = null;
	//					
	//					Enumeration<String> en = signature2count.keys();
	//					while(en.hasMoreElements()) {
	//						
	//						String signature = en.nextElement();
	//						int count = signature2count.get(signature).intValue();
	//										
	//						if(count > max) {
	//							max = count;
	//							signatureMax = signature;
	//						}
	//					}
	//					
	//					//
	//					//format signature: if domain occur several times
	//					//in a row, then the number of repetitions is begin added
	//					//in parentheses
	//					//
	//					//ex.: PF0001 - PF0001 - PF0001 - PF0001 - PF0002
	//					// => PF0001 (4) - PF0002
	//					//
	//					String formattedSignature = "";
	//					
	//					String[] sarr = signatureMax.split("-");
	//					String previousDomain = sarr[0].trim();
	//					int currentCount = 1;
	//					for(int i=1; i<sarr.length; i++) {
	//						
	//						String domain = sarr[i].trim();
	//						if(domain.equals(previousDomain)) {
	//							currentCount++;
	//						}
	//						else {
	//							
	//							if(currentCount == 1) {
	//								formattedSignature += " - " +previousDomain;
	//							}
	//							else {
	//								formattedSignature += " - " +previousDomain +" ("+currentCount+")";
	//							}
	//							
	//							currentCount = 1;
	//						}
	//						previousDomain = domain;
	//					}
	//					//add last domain
	//					if(currentCount == 1) {
	//						formattedSignature += " - " +previousDomain;
	//					}
	//					else {
	//						formattedSignature += " - " +previousDomain +" ("+currentCount+")";
	//					}
	//					
	//					formattedSignature = formattedSignature.substring(3);
	//										
	//					da = formattedSignature;
	//					
	//					double coverageAllMembers = (100*max)/((double) members.cardinality());
	//					BigDecimal bd1 = new BigDecimal(coverageAllMembers);
	//					bd1 = bd1.setScale(2, BigDecimal.ROUND_HALF_UP);
	//					coverageAllMembersRounded = bd1.doubleValue();
	//					
	//					double coverageAllMembersWithSignature = (100*max)/((double) countMembersWithSignature);
	//					BigDecimal bd2 = new BigDecimal(coverageAllMembersWithSignature);
	//					bd2 = bd2.setScale(2, BigDecimal.ROUND_HALF_UP);
	//					coverageAllMembersWithSignatureRounded = bd2.doubleValue();
	//				}
	//									
	//				
	//				pstm3.setString(1, code);
	//				pstm3.setString(2, da);
	//				pstm3.setString(3, comment);
	//				pstm3.setDouble(4, coverageAllMembersRounded);
	//				pstm3.setDouble(5, coverageAllMembersWithSignatureRounded);
	//				
	//				pstm3.executeUpdate();
	//			}
	//			stm.close();
	//			rs.close();
	//			
	//			pstm1.close();
	//			pstm2.close();
	//			pstm3.close();		
	//			
	//		}catch(Exception e) {
	//			e.printStackTrace();
	//		}finally {
	//			if (CAMPS_CONNECTION != null) {
	//				try {
	//					CAMPS_CONNECTION.close();					
	//				} catch (SQLException e) {					
	//					e.printStackTrace();
	//				}
	//			}
	//		}
	//	}





	/**
	 * Extract tms cores for FH-clusters.
	 */
	public void extractTmsCores() {		
		try {

			int batchSize = 50;
			int batchCounter = 0;

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM fh_clusters WHERE code=? AND redundant=\"No\"");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT begin,end FROM tms WHERE sequenceid=? order by begin");
			PreparedStatement pstmx = CAMPS_CONNECTION.prepareStatement("SELECT length FROM sequences2 WHERE sequenceid=?");
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT sequence FROM sequences2 WHERE sequenceid=?");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_tms_cores " +
							"(code,tms_core_id,sequenceid," +
							"sequence_aligned,begin_aligned,end_aligned,length_aligned," +
							"sequence_unaligned,begin_unaligned,end_unaligned,length_unaligned) " +
							"VALUES " +
					"(?,?,?,?,?,?,?,?,?,?,?)");
			PreparedStatement pstm5 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_tms_blocks " +
							"(code,tms_block_id,sequenceid," +
							"sequence_aligned,begin_aligned,end_aligned,length_aligned," +
							"sequence_unaligned,begin_unaligned,end_unaligned,length_unaligned) " +
							"VALUES " +
					"(?,?,?,?,?,?,?,?,?,?,?)");


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT code, tms_range FROM fh_clusters_nr_info");	
			int counter = 0;
			while(rs.next()) {
				counter++;
				String code = rs.getString("code");


				//get distinct sequences for each cluster
				System.out.println("\n\n\t[INFO] Get cluster members for: "+code+"  ("+counter+")");
				BitSet sequenceIDs = new BitSet();
				pstm1.setString(1, code);						
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					int sequenceid = rs1.getInt("sequenceid");					
					sequenceIDs.set(sequenceid);					
				}
				rs1.close();
				rs1 = null;

				//avoid Blobs
				if(sequenceIDs.cardinality() > BLOB_SIZE) {
					System.out.println("\t[INFO] Cluster size: "+sequenceIDs.cardinality()+" --> Cluster too large --> Ignore!");					
					continue;
				}

				String tmsRange = rs.getString("tms_range");

				int minNumTms = Integer.parseInt(tmsRange.split("-")[0]);
				int maxNumTms = Integer.parseInt(tmsRange.split("-")[1]);


				System.out.println("\t[INFO] Choose candidates");
				//sequenceid2TmsPredictions: key: sequenceid, value: list of TMS predictions (format: tms_start-tms_end)				
				Hashtable<Integer,ArrayList<String>> sequenceid2TmsPredictions = new Hashtable<Integer,ArrayList<String>>();
				//candidates: subset of sequences with sufficient struct. homog.
				ArrayList<Integer> candidates = new ArrayList<Integer>();		
				for(int sequenceid = sequenceIDs.nextSetBit(0); sequenceid>=0; sequenceid = sequenceIDs.nextSetBit(sequenceid+1)) {

					//get predicted tms
					ArrayList<String> tmsPredictions = new ArrayList<String>();
					int numTms = 0;
					pstm2.setInt(1, sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						numTms++;						
						int tmsBegin = rs2.getInt("begin");
						int tmsEnd = rs2.getInt("end");						
						tmsPredictions.add(tmsBegin + "-" + tmsEnd);
					}
					rs2.close();
					rs2 = null;

					//get length
					int length = -1;
					pstmx.setInt(1, sequenceid);
					ResultSet rsx = pstmx.executeQuery();
					while(rsx.next()) {
						length = rsx.getInt("length");
					}
					rsx.close();

					//consider only cluster members with sufficient structural homogeneity
					//and sequence length below threshold
					if(numTms >= minNumTms && numTms <= maxNumTms && length <= MAX_SEQUENCE_LENGTH) {
						sequenceid2TmsPredictions.put(Integer.valueOf(sequenceid), tmsPredictions);
						candidates.add(Integer.valueOf(sequenceid));
					}
				}



				int numCandidates = candidates.size();

				//take most divergent sequences for seed alignment if cluster is too large
				if(numCandidates > 400) {	

					System.out.println("\t[INFO] Get identities");
					float[][] matrix = fillMatrix(candidates);

					System.out.println("\t[INFO] Choose most divergent sequences");
					ArrayList<Integer> newCandidates = getMostDivergent(matrix, candidates, 400);

					Hashtable<Integer,ArrayList<String>> sequenceid2TmsPredictionsCopy = (Hashtable<Integer, ArrayList<String>>) sequenceid2TmsPredictions.clone();
					ArrayList<Integer> candidatesCopy = (ArrayList<Integer>) candidates.clone();
					sequenceid2TmsPredictions = new Hashtable<Integer,ArrayList<String>>();	//reset
					candidates = new ArrayList<Integer>();	//reset

					for(Integer newCandidate: newCandidates) {
						sequenceid2TmsPredictions.put(newCandidate, sequenceid2TmsPredictionsCopy.get(newCandidate));
						candidates.add(newCandidate);
					}				

					sequenceid2TmsPredictionsCopy = null;
					candidatesCopy = null;	
					newCandidates = null;
				}

				//ignore small clusters
				if(numCandidates < MINIMUM_CLUSTER_SIZE) {
					System.out.println("\t[INFO] Cluster size: "+numCandidates+" --> Cluster too small --> Ignore!");
					continue;
				}

				System.out.println("\t[INFO] Run alignment program");
				//create FASTA file as input
				File alignmentInfileTMP = File.createTempFile("cluster_"+code, ".fasta");			
				PrintWriter pw = new PrintWriter(new FileWriter(alignmentInfileTMP));
				for(Integer candidate: candidates) {
					String sequence = null;
					pstm3.setInt(1, candidate.intValue());
					ResultSet rs3 = pstm3.executeQuery();
					while(rs3.next()) {
						sequence = rs3.getString("sequence").toUpperCase();
					}
					rs3.close();
					rs3 = null;

					pw.println(">"+candidate.intValue()+"\n"+sequence);
				}
				pw.close();

				//
				//run alignment program
				//
				File alignmentOutfileTMP = File.createTempFile("cluster_"+code, ".ali");			    
				ConfigFile cf = new ConfigFile("/home/users/saeed/workspace/CAMPS3/config/config.xml");
				String alignmentDir = cf.getProperty("apps:clustalw:install-dir");

				String cmd = alignmentDir+" -INFILE=" + alignmentInfileTMP.getAbsolutePath() + " -OUTFILE=" + alignmentOutfileTMP.getAbsolutePath() + " -OUTPUT=FASTA";
				//String cmd = alignmentDir+" -i " + alignmentInfileTMP.getAbsolutePath() + " -o " + alignmentOutfileTMP.getAbsolutePath() + " --outfmt FASTA --threads=8";

				Process p = Runtime.getRuntime().exec(cmd);
				BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
				String line;
				while((line=br.readLine()) != null) {
					//System.err.println("\t\t[INFO]"+line);
				}
				br.close();

				p.waitFor();
				p.destroy();


				//tmsImage: contains information to compute TMS cores/blocks; 
				//if x>1 (x:= array element): index specifies position in alignment 
				//that corresponds to TMS region in x sequences
				int[] tmsImage = null;

				Hashtable<Integer,String> sequenceid2alignedSeq = new Hashtable<Integer,String>();

				System.out.println("\t[INFO] Define TMS cores/blocks");
				//
				//read out alignment output
				//
				int imageLength = 0;
				FastaReader fr = new FastaReader(alignmentOutfileTMP);
				ArrayList<FastaEntry> alignedSeqs = fr.getEntries();

				if(alignedSeqs.isEmpty()) {
					System.out.println("\t[INFO] Empty alignment file -> TMH cores cannot be determined!");
					System.err.println("\n\n"+code+"\tEmpty alignment file -> TMH cores cannot be determined!");
					alignmentInfileTMP.delete();
					alignmentOutfileTMP.delete();
					continue;
				}

				for(FastaEntry alignedSeq: alignedSeqs) {

					Integer sequenceid = Integer.valueOf(alignedSeq.getHeader());
					String alignedSequence = alignedSeq.getSequence();

					sequenceid2alignedSeq.put(sequenceid, alignedSequence);

					if(imageLength == 0) {
						imageLength = alignedSequence.length();
						tmsImage = new int[imageLength+1];						
					}

					int[] positionMapping = getPosInAlignedSeq(alignedSequence);
					ArrayList<String> tmsPredictions = sequenceid2TmsPredictions.get(sequenceid);
					for(String tmsPrediction: tmsPredictions) {
						int tmsStart = Integer.parseInt(tmsPrediction.split("-")[0]);
						int tmsEnd = Integer.parseInt(tmsPrediction.split("-")[1]);
						int tmsStartAlign = positionMapping[tmsStart];
						int tmsEndAlign = positionMapping[tmsEnd];
						for(int i=tmsStartAlign; i<=tmsEndAlign; i++) {
							char residue = alignedSequence.charAt(i-1);
							if(residue != '-') {
								tmsImage[i]++;
							}
						}
					}
				}
				alignedSeqs = null;

				//				int index = 0;
				//				for(int i:tmsImage) {
				//					System.out.println(index+"\t"+i);					
				//					index++;
				//				}				

				alignmentInfileTMP.delete();
				alignmentOutfileTMP.delete();

				//
				//determine TMS cores
				//
				int numTmsCores = 0;
				int[] tmsCoresLocations = new int[tmsImage.length];
				Hashtable<Integer,int[]> tmsCoreID2position = new Hashtable<Integer,int[]>();

				for(int i=1; i<=imageLength; i++) {
					int count = tmsImage[i];
					if(count == 0) {
						if(i>1 && tmsCoresLocations[i-1] > 0) {
							int[] position = tmsCoreID2position.get(Integer.valueOf(numTmsCores));
							position[1] = i-1;		//set end position of TMS core
							if(position[1] - position[0] < 10) {
								numTmsCores--;
							}
						}
						continue;
					}					

					double tmsImagePercent = (count * 100) / ((double) candidates.size());

					if (tmsImagePercent >= MIN_COVERAGE_ALIGNMENT){
						tmsCoresLocations[i] = 1;
						if (tmsCoresLocations[i-1] == 0){
							numTmsCores++;
							int[] position = new int[2];
							position[0] = i;		//set begin position of TMS core
							tmsCoreID2position.put(Integer.valueOf(numTmsCores),position);
						}

					}
					else if (tmsCoresLocations[i-1] > 0){						
						int[] position =  tmsCoreID2position.get(Integer.valueOf(numTmsCores));
						position[1] = i-1;		//set end position of TMS core						
						if(position[1] - position[0] < 10) {
							numTmsCores--;
						}
					}
				}				

				if(tmsCoresLocations[imageLength] > 0) {
					int[] position = tmsCoreID2position.get(Integer.valueOf(numTmsCores));
					position[1] = imageLength;
				}	

				if(numTmsCores < MINIMUM_NUMBER_CORES) {
					System.out.println("\t[INFO] Number of detected TMH cores: "+numTmsCores+" --> Not enough cores --> Ignore!");
					continue;
				}


				boolean withinRange = false;
				if(numTmsCores >= minNumTms && numTmsCores <= maxNumTms) {
					withinRange = true;
				}

				if(!withinRange) {
					System.out.println("\t[INFO] Number of detected TMH cores: "+numTmsCores+" --> Not within TMS range ("+tmsRange+") --> Ignore!");
					continue;
				}

				//
				//determine TMS blocks
				//				
				int previousCoreEnd = 0;
				int previousBlockEnd = 0;
				for(int tmsCoreID=1; tmsCoreID<=numTmsCores; tmsCoreID++) {					
					int[] position = tmsCoreID2position.get(Integer.valueOf(tmsCoreID));
					int coreStart = position[0];
					int coreEnd = position[1];

					int blockStart = -1;
					int blockEnd = -1;

					if(tmsCoreID == 1) {		//first core
						blockStart = Math.max(1, coreStart-CORE_EXTENSION);
					}
					else if(tmsCoreID == numTmsCores) {		//last core
						blockEnd = Math.min(imageLength, coreEnd+CORE_EXTENSION);
					}

					if(blockStart == -1) {
						int interspaceSizeLeft = ((coreStart-1) - (previousCoreEnd+1)) + 1;

						if(interspaceSizeLeft >= 2*CORE_EXTENSION) {
							blockStart = coreStart - CORE_EXTENSION;
						}
						else {
							blockStart = previousBlockEnd + 1;
						}
					}

					if(blockEnd == -1) {
						int succCoreStart;
						if (tmsCoreID+1>numTmsCores){
							//succCoreStart = tmsCoreID2position.get(Integer.valueOf(tmsCoreID))[0];	//*** +1 removed
							// so to avoid the null point exception in case when there is the last core
							// then use the end of the core under consideration and it would automatically
							// set the interspaceSizeRight/2 + coreEnd as the Block End  
							succCoreStart = coreEnd;
						}
						else{
							succCoreStart = tmsCoreID2position.get(Integer.valueOf(tmsCoreID+1))[0];	//*** +1 removed
						}
						int interspaceSizeRight = ((succCoreStart-1) - (coreEnd+1)) + 1;

						if(interspaceSizeRight >= 2*CORE_EXTENSION) {
							blockEnd = coreEnd + CORE_EXTENSION;
						}
						else {
							blockEnd = coreEnd + (interspaceSizeRight/2);
						}
						// **********

					}

					previousCoreEnd = coreEnd;
					previousBlockEnd = blockEnd;

					for(Integer candidate: candidates) {
						String alignedSequence = sequenceid2alignedSeq.get(candidate);
						int[] positionMapping = getPosInUnalignedSeq(alignedSequence);

						String coreSequenceAligned = alignedSequence.substring(coreStart-1, coreEnd);
						String blockSequenceAligned = alignedSequence.substring(blockStart-1, blockEnd);

						String coreSequenceUnaligned = coreSequenceAligned.replaceAll("-", "");
						String blockSequenceUnaligned = blockSequenceAligned.replaceAll("-", "");

						int coreLengthAligned = 0;
						if(coreStart >0 && coreEnd >0) {
							coreLengthAligned = (coreEnd - coreStart) + 1;
						}

						int blockLengthAligned = 0;
						if(blockStart >0 && blockEnd >0) {
							blockLengthAligned = (blockEnd - blockStart) + 1;
						}

						String cs = alignedSequence.substring(coreStart-1, coreEnd);
						String bs = alignedSequence.substring(blockStart-1, blockEnd);

						int coreStart2 = coreStart;
						int coreEnd2 = coreEnd;
						int blockStart2 = blockStart;
						int blockEnd2 = blockEnd;

						//remove trailing gaps
						while(cs.startsWith("-")) {
							coreStart2++;
							cs = cs.substring(1);
						}

						while(cs.endsWith("-")) {
							coreEnd2--;
							cs = cs.substring(0, cs.length()-1);
						}

						while(bs.startsWith("-")) {
							blockStart2++;
							bs = bs.substring(1);
						}

						while(bs.endsWith("-")) {
							blockEnd2--;
							bs = bs.substring(0, bs.length()-1);
						}

						int coreStartInInitialSeq;
						int coreEndInInitialSeq;

						String coreSequenceCopy = new String(coreSequenceAligned);
						if(coreSequenceCopy.replaceAll("-", "").trim().equals("")) {
							coreStartInInitialSeq = 0;
							coreEndInInitialSeq = 0;
						}
						else {
							coreStartInInitialSeq = positionMapping[coreStart2];
							coreEndInInitialSeq = positionMapping[coreEnd2];
						}

						int coreLengthUnaligned = 0;
						if(coreStartInInitialSeq >0 && coreEndInInitialSeq >0) {
							coreLengthUnaligned = (coreEndInInitialSeq - coreStartInInitialSeq) + 1;
						}

						int blockStartInInitialSeq;
						int blockEndInInitialSeq;

						String blockSequenceCopy = new String(blockSequenceAligned);
						if(blockSequenceCopy.replaceAll("-", "").trim().equals("")) {
							blockStartInInitialSeq = 0;
							blockEndInInitialSeq = 0;
						}
						else {
							blockStartInInitialSeq = positionMapping[blockStart2];
							blockEndInInitialSeq = positionMapping[blockEnd2];
						}	

						int blockLengthUnaligned = 0;
						if(blockStartInInitialSeq >0 && blockEndInInitialSeq >0) {
							blockLengthUnaligned = (blockEndInInitialSeq - blockStartInInitialSeq) + 1;
						}


						batchCounter++;

						pstm4.setString(1, code);
						pstm4.setInt(2, tmsCoreID);
						pstm4.setInt(3, candidate.intValue());						
						pstm4.setString(4, coreSequenceAligned);
						pstm4.setInt(5, coreStart);
						pstm4.setInt(6, coreEnd);	
						pstm4.setInt(7, coreLengthAligned);							
						pstm4.setString(8, coreSequenceUnaligned);
						pstm4.setInt(9, coreStartInInitialSeq);
						pstm4.setInt(10, coreEndInInitialSeq);	
						pstm4.setInt(11, coreLengthUnaligned);							
						pstm4.addBatch();

						pstm5.setString(1, code);
						pstm5.setInt(2, tmsCoreID);
						pstm5.setInt(3, candidate.intValue());						
						pstm5.setString(4, blockSequenceAligned);
						pstm5.setInt(5, blockStart);
						pstm5.setInt(6, blockEnd);	
						pstm5.setInt(7, blockLengthAligned);							
						pstm5.setString(8, blockSequenceUnaligned);
						pstm5.setInt(9, blockStartInInitialSeq);
						pstm5.setInt(10, blockEndInInitialSeq);	
						pstm5.setInt(11, blockLengthUnaligned);						
						pstm5.addBatch();

						if(batchCounter % batchSize == 0) {								
							pstm4.executeBatch();
							pstm4.clearBatch();

							pstm5.executeBatch();
							pstm5.clearBatch();
						}
					}


				}

			}//for each cluster


			pstm1.close();
			pstm1 = null;
			pstm2.close();
			pstm2 = null;
			pstm3.close();
			pstm3 = null;
			pstm4.executeBatch();		//insert remaining entries
			pstm4.clearBatch();
			pstm4.close();
			pstm4 = null;
			pstm5.executeBatch();		//insert remaining entries
			pstm5.clearBatch();
			pstm5.close();
			pstm5 = null;
			pstmx.close();
			pstmx = null;

		} catch(Exception e) {
			System.err.println("Exception in FHClustering2_Postprocessing.extracTmsCores(): " +e.getMessage());
			e.printStackTrace();

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

	/*
	 * Computes the mapping of the positions in the aligned and unaligned sequence.
	 * The indices of the returned array specify the positions in the unaligned sequence, 
	 * and the values correspond to the positions in the aligned sequence.
	 */
	private static int[] getPosInAlignedSeq(String alignedSequence) {
		int[] positionMapping = null;
		try {

			int unalignedSeqLength = alignedSequence.replaceAll("-","").length();
			positionMapping = new int[unalignedSeqLength+1];

			int posAlignedSeq = 0;
			int posUnalignedSeq = 0;

			for(int i=0; i<alignedSequence.length(); i++) {
				char residue = alignedSequence.charAt(i);
				posAlignedSeq = i+1;

				if(residue != '-') {
					posUnalignedSeq++;

					positionMapping[posUnalignedSeq] = posAlignedSeq;
				}								
			}		

		} catch(Exception e) {
			e.printStackTrace();
		}
		return positionMapping;
	}	

	/*
	 * Computes the mapping of the positions in the aligned and unaligned sequence.
	 * The indices of the returned array specify the positions in the aligned sequence, 
	 * and the values correspond to the positions in the unaligned sequence.
	 */
	private static int[] getPosInUnalignedSeq(String alignedSequence) {
		int[] positionMapping = null;
		try {

			int alignedSeqLength = alignedSequence.length();
			positionMapping = new int[alignedSeqLength+1];

			int posAlignedSeq = 0;
			int posUnalignedSeq = 0;

			for(int i=0; i<alignedSequence.length(); i++) {
				char residue = alignedSequence.charAt(i);
				posAlignedSeq = i+1;

				if(residue != '-') {
					posUnalignedSeq++;

					positionMapping[posAlignedSeq] = posUnalignedSeq;
				}								
			}		

		} catch(Exception e) {
			e.printStackTrace();
		}
		return positionMapping;
	}	


	/*
	 * Returns a square matrix of the specified size that contains the specified 
	 * float value at each position.
	 * 
	 * size - number of columns/rows 
	 * f    - default value for each matrix cell
	 */
	private static float[][] getDefaultMatrix(int size, float f) {

		float[][] matrix = new float[size][size];
		for(int i=0; i<size; i++) {
			for(int j=0; j<size; j++) {
				matrix[i][j] = f;
			}
		}

		return matrix;
	}


	/*
	 * Returns a square matrix that contains the pairwise identity values for
	 * each pair of the specified set of candidate sequences as extracted from 
	 * table 'alignments'.
	 * 
	 * Self scores are not inserted, as well as redundant entries. At those
	 * positions the matrix contains the value 2 (since the identity values from 
	 * table 'alignments' range from 0 to 1). 
	 * 
	 * candidates - set of sequences (given by sequence id)
	 */
	private static boolean dictbool = false;
	static Hashtable<Integer, Dictionary> dict = new Hashtable<Integer, Dictionary>();
	private static float[][] fillMatrix(ArrayList<Integer> candidates) {
		float[][] matrix = null;
		PreparedStatement pstm = null;
		try{
			if(dictbool == false){

				int idx =1;
				for (int i=1; i<=17;i++){				
					pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial limit "+idx+","+10000000);
					//pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial");
					idx = i*10000000;
					System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
					ResultSet rs = pstm.executeQuery();
					while(rs.next()) {
						int idhit = rs.getInt("seqid_hit");
						int key = rs.getInt ("seqid_query");
						float identity = rs.getFloat("identity");

						if (dict.containsKey(key)){ // the key already doesnt exist...so that you dont delete the previous info of the key
							Dictionary temp = (Dictionary)dict.get(key);
							temp.set(idhit, identity);
							dict.put(key,temp);
						}
						else{
							Dictionary temp = new Dictionary(idhit, identity);
							dict.put(key,temp);
						}
					}

					// dict population complete
					rs.close();
				}
				dictbool = true;
				pstm.close();
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		System.out.print("\n Hashtable of Alignments Complete\n");
		System.out.print(" Making Matrix Now\n");
		System.out.print(" Size of Candidates "+candidates.size()+"\n");
		System.out.print(" Size of hashtable "+dict.size()+"\n");
		System.out.flush();

		// ********************************************************** //
		/*
		float[][] matrix = null;
		try {				
			Collections.sort(candidates);
			//
			//note: searching in only one direction here is fine since 
			//table 'alignments' is structured in the way that the 
			//sequences_sequenceid_query is always smaller than the  
			//sequences_sequenceid_hit
			//and the specified candidates list is sorted in ascending order
			//
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("SELECT sequences_sequenceid_hit,identity FROM alignments where sequences_sequenceid_query=?");
			matrix = getDefaultMatrix(candidates.size(), 2);
			for(int i=0; i<candidates.size(); i++) {
				int id1 = candidates.get(i).intValue();
				pstm.setInt(1, id1);
				ResultSet rs = pstm.executeQuery();
				while(rs.next()) {
					int id2 = rs.getInt("sequences_sequenceid_hit");
					float identity = rs.getFloat("identity");
					Integer id2I = Integer.valueOf(id2);
					if(candidates.contains(id2I)) {
						int index = candidates.indexOf(id2I);
						matrix[i][index] = identity;
					}
				}
				rs.close();					
			}			
			pstm.close();
		}catch(Exception e) {
			e.printStackTrace();
		}
		return matrix;
		 */
		// ************************************************ //

		matrix = getDefaultMatrix(candidates.size(), 2);
		for(int i=0; i<candidates.size(); i++) {
			int id1 = candidates.get(i).intValue();	// query
			// search the dict for this query and get all its hits and populate as it is
			if (dict.containsKey(id1)){	// contains the query as key
				Dictionary temp = (Dictionary)dict.get(id1);
				// find the hits for presence of candidates and put in matrix
				// for all the hits... are they present in cadidates
				for (int j =0; j<=temp.protein.size()-1;j++){
					Integer id2I = temp.protein.get(j);	// hit
					if(candidates.contains(id2I)) {			// if the hit is a part of selected candidates 
						int index = candidates.indexOf(id2I);	// index of hit
						float identity = temp.score.get(j);
						matrix[i][index] = identity;
						//System.out.print(".");
						//System.out.flush();
					}
				}
			}
		}

		System.out.print(" End of Matrix formulation for number of candidates "+candidates.size()+"\n");
		System.out.flush();
		return matrix;
	}


	/*
	 * Returns the specified number of most divergent sequences (given by sequence id)
	 * from the specified set of sequences by parsing the specified matrix.
	 * 
	 * matrix         - matrix containing the pairwise identity values
	 * ids            - set of sequences (given by sequence id)
	 * wantedEntries  - number of most divergent sequences that will be returned
	 */
	private static ArrayList<Integer> getMostDivergent(float[][] matrix, ArrayList<Integer> ids, int wantedEntries) {

		//number of already found most divergent sequences
		int selectedEntries = 0;

		//list of sequence ids (subset of 'ids') that represent the most
		//divergent sequences
		ArrayList<Integer> result = new ArrayList<Integer>();

		float[][] copyMatrix = matrix.clone();
		ArrayList<Integer> copyIds = (ArrayList<Integer>) ids.clone();

		//indices of already found most divergent sequences
		int[] alreadyFound = new int[wantedEntries];

		//get first two entries
		int size = copyMatrix.length;

		double min = Double.MAX_VALUE;
		int rowIndex = -1;
		int columnIndex = -1;

		//only parse right triangular matrix
		for(int i=0; i<size; i++) {

			for(int j=i+1; j<size; j++) {

				int row = i;
				int column = j;
				double value = copyMatrix[i][j];

				if(value<min) {
					min = value;
					rowIndex = row;
					columnIndex = column;
				}					
			}				
		}

		result.add(ids.get(rowIndex));
		result.add(ids.get(columnIndex));
		copyIds.set(rowIndex, Integer.valueOf(-1));		//mark sequence id as already filtered
		copyIds.set(columnIndex,Integer.valueOf(-1));	//mark sequence id as already filtered
		alreadyFound[0] = rowIndex;
		alreadyFound[1] = columnIndex;

		selectedEntries += 2;


		//
		//get remaining entries	by using the following procedure:
		//
		// - all already found sequence ids are compared with each sequence id
		//   that is still left
		// - all pairwise identity (PI) values are summed up
		// - the sequence id with the smallest sum is chosen
		//
		// example: 
		// ids = (2,6,9,15)
		// already found ids: (2,6)
		// calculation 1: (2,6) against 9  => PI(2,9) + PI(6,9) = x
		// calculation 2: (2,6) against 15 => PI(2,15)+ PI(6,15)= y
		// if x < y, then ids = (2,6,9)
		// else ids = (2,6,15)
		//
		while(selectedEntries < wantedEntries) {

			double minSum = Double.MAX_VALUE;
			int index = -1;	//index of next most divergent sequence to be found

			for(int i=0; i<copyIds.size(); i++) {

				Integer id = copyIds.get(i);
				if(id.intValue() == -1) {	//id is already in result list
					continue;
				}

				double sum = 0;
				for(int j=0;j<selectedEntries; j++) {

					int currentIndex = alreadyFound[j];

					if(i<currentIndex) {
						sum += copyMatrix[i][currentIndex];
					}
					else {
						sum += copyMatrix[currentIndex][i];
					}

				}

				if(sum < minSum) {
					minSum = sum;
					index = i;
				}
			}

			result.add(ids.get(index));
			copyIds.set(index,Integer.valueOf(-1));
			alreadyFound[selectedEntries] = index;

			selectedEntries++;
		}

		copyMatrix = null;
		copyIds = null;

		return result;
	}


	/**
	 * Create cross references to external databases 
	 * (CATH, GO, GPCRDB, OPM, PFAM, SCOP, SUPERFAMILY, TCDB);
	 */
	private static HashMap <String, ArrayList<Integer>> ptbl1 = new HashMap<String, ArrayList<Integer>>();
	public void crossDB() {
		try {
			// get common used elements
			System.out.print("Getting Clusters and their Members...\n");
			PreparedStatement ptm1 = CAMPS_CONNECTION.prepareStatement("SELECT code,sequenceid FROM fh_clusters");
			ResultSet r1 = ptm1.executeQuery();
			ptbl1 = new HashMap<String, ArrayList<Integer>>(); 
			// key is the code and value is seqIds split by -
			while (r1.next()){
				String code = r1.getString("code");
				int sequenceID = r1.getInt("sequenceid");
				if (ptbl1.containsKey(code)){
					ArrayList<Integer> temp = ptbl1.get(code);
					temp.add(sequenceID);
					ptbl1.put(code, temp);
				}
				else{
					ArrayList<Integer> temp = new ArrayList<Integer>();
					temp.add(sequenceID);
					ptbl1.put(code, temp);
				}
			}


			System.out.println("Insert CATH cross links");
			addCATHcrosslinks();

			System.out.println("Insert GO cross links");
			addGOcrosslinks();

			System.out.println("Insert GPCRDB cross links");
			addGPCRDBcrosslinks();

			System.out.println("Insert OPM cross links");
			addOPMcrosslinks();

			System.out.println("Insert PFAM cross links");
			addPFAMcrosslinks();

			System.out.println("Insert SCOP cross links");
			addSCOPcrosslinks();

			System.out.println("Insert SUPERFAMILY cross links");
			addSUPERFAMILYcrosslinks();

			System.out.println("Insert TCDB cross links");
			addTCDBcrosslinks();

		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}



	private static void addCATHcrosslinks() {
		try {
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_cross_db " +
							"(code, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?)");
			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM fh_clusters WHERE code=?");
			//take only best PDBTM hit
			/*
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT distinct(sequences_other_database_sequenceid) " +
							"FROM other_database " +
							"WHERE sequenceid=? AND " +
							"db=\"pdbtm\" AND " +
							"query_coverage>="+CROSS_COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+CROSS_COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc LIMIT 1");
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT name FROM sequences_other_database WHERE sequenceid=? AND db=\"pdbtm\"");
			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT classification FROM pdbtm2scop_cath WHERE pdb_id=? AND db=\"cath\"");
			PreparedStatement pstm5 = CAMPS_CONNECTION.prepareStatement("SELECT description FROM other_database_hierarchies WHERE key_=? AND db=\"cath\"");
			 */
			PreparedStatement ptm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid, sequences_other_database_sequenceid " +
							"FROM other_database " +
							"WHERE " +
							"db=\"pdbtm\" AND " +
							"query_coverage>="+CROSS_COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+CROSS_COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc");
			System.out.print("\n Getting PDBTm hits \n");
			HashMap <Integer, Integer> ptbl2 = new HashMap<Integer, Integer>(); // key is the sequenceid and value is sequences_other_database_sequenceid

			ResultSet rptm2 = ptm2.executeQuery();
			while(rptm2.next()){
				int Sid = rptm2.getInt("sequenceid");
				int pdbtmSequenceid = rptm2.getInt("sequences_other_database_sequenceid");

				if(!ptbl2.containsKey(Sid)){
					ptbl2.put(Sid, pdbtmSequenceid);
				}
			}
			rptm2.close();
			ptm2.close();



			//PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT name FROM sequences_other_database WHERE sequenceid=? AND db=\"pdbtm\"");
			System.out.print("\n Getting cath classifications \n");
			HashMap <Integer, String> ptbl3 = new HashMap<Integer, String>(); // key is the seq_id and value is name
			PreparedStatement ptm3 = CAMPS_CONNECTION.prepareStatement("SELECT name,sequenceid FROM sequences_other_database WHERE db=\"pdbtm\"");
			ResultSet rptm3 = ptm3.executeQuery();
			while(rptm3.next()){
				String name = rptm3.getString(1);
				int id = rptm3.getInt(2);
				if(!ptbl3.containsKey(id)){
					ptbl3.put(id, name);
				}
			}
			ptm3.close();
			rptm3.close();

			//PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT classification FROM pdbtm2scop_cath WHERE pdb_id=? AND db=\"cath\"");
			// get all data in hash instead of pstm4

			System.out.print("\n Getting cath classifications \n");
			HashMap <String, String> ptbl4 = new HashMap<String, String>(); // key is the pdb_id and value is classification
			PreparedStatement ptm4 = CAMPS_CONNECTION.prepareStatement("SELECT classification,pdb_id FROM pdbtm2scop_cath WHERE db=\"cath\"");
			ResultSet rptm4 = ptm4.executeQuery();
			while(rptm4.next()){
				String c = rptm4.getString(1);
				String p = rptm4.getString(2);
				if(!ptbl4.containsKey(p)){
					ptbl4.put(p, c);
				}
				else if(ptbl4.containsKey(p)){
					String cc = ptbl4.get(p);
					c = c + "*" +cc;
				}
			}
			ptm4.close();
			rptm4.close();

			System.out.print("\n Getting cath descriptions \n");
			//PreparedStatement pstm5 = CAMPS_CONNECTION.prepareStatement("SELECT description FROM other_database_hierarchies WHERE key_=? AND db=\"cath\"");
			// get all data in hash instead of pstm5
			PreparedStatement ptm5 = CAMPS_CONNECTION.prepareStatement("SELECT description,key_ FROM other_database_hierarchies WHERE db=\"cath\"");
			ResultSet rptm5 = ptm5.executeQuery();
			HashMap <String, String> ptbl5 = new HashMap<String, String>(); // key is the key_ and value is description
			while(rptm5.next()){
				String d = rptm5.getString(1);
				String k = rptm5.getString(2);
				if(!ptbl5.containsKey(k)){
					ptbl5.put(k, d);
				}
			}
			ptm5.close();
			rptm5.close();
			System.out.print("\n Running over all clusters Now \n");

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT code FROM fh_clusters_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				String code = rs.getString("code");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				BitSet members = new BitSet();
				ArrayList<Integer> temp = ptbl1.get(code);
				for (int i =0;i<=temp.size()-1;i++){
					int sequenceID = temp.get(i);
					members.set(sequenceID);
				}

				int clusterSize = members.cardinality();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get pdbtm and cath data for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {


					//pstm2.setInt(1, sequenceid);
					//ResultSet rs2 = pstm2.executeQuery();
					if (ptbl2.containsKey(sequenceid)){
						//while(rs2.next()) {

						//int pdbtmSequenceid = rs2.getInt("sequences_other_database_sequenceid");
						int pdbtmSequenceid = ptbl2.get(sequenceid);

						//pstm3.setInt(1, pdbtmSequenceid);
						//ResultSet rs3 = pstm3.executeQuery();
						if (ptbl3.containsKey(pdbtmSequenceid)){
							//while(rs3.next()) {

							//String pdbCode = rs3.getString("name");
							String pdbCode = ptbl3.get(pdbtmSequenceid);

							//
							//caution: pdb sequences can have multiple CATH
							//classifications (multidomain sequences!)
							//
							String link = "";

							//pstm4.setString(1, pdbCode);
							//ResultSet rs4 = pstm4.executeQuery();
							if(ptbl4.containsKey(pdbCode)){
								//while(rs4.next()) {
								/*
								//String cathClassification = rs4.getString("classification");
								String tmp[] = cathClassification.split("\\.");
								String cathFoldClassification = tmp[0]+"."+tmp[1]+"."+tmp[2];

								String cathDescription = "";
								pstm5.setString(1, cathFoldClassification);
								ResultSet rs5 = pstm5.executeQuery();
								while(rs5.next()) {
									cathDescription = rs5.getString("description");
								}
								rs5.close();

								String tmpLink = cathFoldClassification+" - " +cathDescription;
								link += "#"+tmpLink;
								 */
								String cathClassifications = ptbl4.get(pdbCode);
								String cathclasses[] = cathClassifications.split("\\*");

								for(int x =0;x<=cathclasses.length-1;x++){
									String tmp[] = cathclasses[x].split("\\.");
									String cathFoldClassification = tmp[0]+"."+tmp[1]+"."+tmp[2];

									String cathDescription = "";

									if (ptbl5.containsKey(cathFoldClassification)){
										cathDescription = ptbl5.get(cathFoldClassification);
									}

									String tmpLink = cathFoldClassification+" - " +cathDescription;
									link += "#"+tmpLink + "*";
								}
							}
							//rs4.close();

							if(link.equals("")) {
								continue;
							}

							link = link.substring(1);

							int count = 0;
							if(link2count.containsKey(link)) {
								count = link2count.get(link);
							}
							count = count+1;

							link2count.put(link, new Integer(count));
							//}
						}
						//rs3.close();
						//}
					}
					//rs2.close();
				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setString(1, code);
					pstm.setString(2, "cath");
					pstm.setString(3, link);
					pstm.setInt(4, count);
					pstm.setDouble(5, perc);

					pstm.executeUpdate();
				}
				if (statusCount % 1000 == 0){
					System.out.print(statusCount+"\n");
				}
			}
			rs.close();
			stm.close();


			pstm.close();
			//pstm2.close();
			//pstm3.close();
			//pstm4.close();
			//pstm5.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	private static void addGOcrosslinks() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_cross_db " +
							"(code, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?)");



			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM fh_clusters WHERE code=?");			

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT distinct(accession) FROM go_annotations WHERE sequenceid=?");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT name,term_type FROM go_annotations WHERE accession=? limit 1");


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT code FROM fh_clusters_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				String code = rs.getString("code");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				BitSet members = new BitSet();
				ArrayList<Integer> temp = ptbl1.get(code);
				for (int i =0;i<=temp.size()-1;i++){
					int sequenceID = temp.get(i);
					members.set(sequenceID);
				}

				int clusterSize = members.cardinality();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get go annotations for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					pstm2.setInt(1, sequenceid);					
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {

						String acc = rs2.getString("accession");					

						int count = 0;
						if(link2count.containsKey(acc)) {
							count = link2count.get(acc);
						}
						count = count+1;

						link2count.put(acc, new Integer(count));
					}
					rs2.close();

				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String acc = en.nextElement();						

					int count = link2count.get(acc).intValue();

					double perc = 100 * ((double) count/clusterSize);

					String description = "";
					String type = "";
					pstm3.setString(1, acc);
					ResultSet rs3 = pstm3.executeQuery();
					while(rs3.next()) {
						description = rs3.getString("name");
						type = rs3.getString("term_type");
					}
					rs3.close();

					if(type.equals("biological_process")) {
						type = "BP";
					}
					else if(type.equals("cellular_component")) {
						type = "CC";
					}
					else if(type.equals("molecular_function")) {
						type = "MF";
					}

					pstm.setString(1, code);
					pstm.setString(2, "go");
					pstm.setString(3, acc+" - " +type + " - " +description);
					pstm.setInt(4, count);
					pstm.setDouble(5, perc);

					pstm.executeUpdate();
				}



			}
			rs.close();
			stm.close();


			pstm.close();
			//pstm1.close();
			pstm2.close();
			pstm3.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	private static void addGPCRDBcrosslinks() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_cross_db " +
							"(code, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?)");



			//			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM fh_clusters WHERE code=?");

			//take only best GPCRDB hit
			/*
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT distinct(sequences_other_database_sequenceid) " +
							"FROM other_database " +
							"WHERE sequenceid=? AND " +
							"db=\"gpcrdb\" AND " +
							"query_coverage>="+CROSS_COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+CROSS_COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc LIMIT 1");
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT additional_information FROM sequences_other_database WHERE sequenceid=? AND db=\"gpcrdb\"");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT description FROM other_database_hierarchies WHERE key_=? AND db=\"gpcrdb\"");
			 */
			PreparedStatement ptm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid,sequences_other_database_sequenceid " +
							"FROM other_database " +
							"WHERE " +
							"db=\"gpcrdb\" AND " +
							"query_coverage>="+CROSS_COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+CROSS_COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc");
			System.out.print("\n Getting GPCRdb hits \n");
			HashMap <Integer, Integer> ptbl2 = new HashMap<Integer, Integer>(); // key is the sequenceid and value is sequences_other_database_sequenceid

			ResultSet rptm2 = ptm2.executeQuery();
			while(rptm2.next()){
				int Sid = rptm2.getInt("sequenceid");
				int gpcrdbSequenceid = rptm2.getInt("sequences_other_database_sequenceid");

				if(!ptbl2.containsKey(Sid)){
					ptbl2.put(Sid, gpcrdbSequenceid);
				}
			}
			rptm2.close();
			ptm2.close();



			PreparedStatement ptm3 = CAMPS_CONNECTION.prepareStatement("SELECT additional_information,sequenceid FROM sequences_other_database WHERE db=\"gpcrdb\"");
			System.out.print("\n Getting additionalInfo of gpcrdb proteins \n");
			HashMap <Integer, String> ptbl3 = new HashMap<Integer, String>(); // key is the seq_id and value is additional Info
			ResultSet rptm3 = ptm3.executeQuery();
			while(rptm3.next()){
				String info = rptm3.getString(1);
				int id = rptm3.getInt(2);
				if(!ptbl3.containsKey(id)){
					ptbl3.put(id, info);
				}
			}
			ptm3.close();
			rptm3.close();


			PreparedStatement ptm4 = CAMPS_CONNECTION.prepareStatement("SELECT description,key_ FROM other_database_hierarchies WHERE db=\"gpcrdb\"");
			ResultSet rptm4 = ptm4.executeQuery();
			HashMap <String, String> ptbl4 = new HashMap<String, String>(); // key is the key_ and value is description
			while(rptm4.next()){
				String d = rptm4.getString(1);
				String k = rptm4.getString(2);
				if(!ptbl4.containsKey(k)){
					ptbl4.put(k, d);
				}
			}
			ptm4.close();
			rptm4.close();


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT code FROM fh_clusters_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				String code = rs.getString("code");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				BitSet members = new BitSet();
				ArrayList<Integer> temp = ptbl1.get(code);
				for (int i =0;i<=temp.size()-1;i++){
					int sequenceID = temp.get(i);
					members.set(sequenceID);
				}

				int clusterSize = members.cardinality();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get gpcrdb data for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {
					/*
					pstm2.setInt(1, sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {

						int gpcrdbSequenceid = rs2.getInt("sequences_other_database_sequenceid");

						pstm3.setInt(1, gpcrdbSequenceid);
						ResultSet rs3 = pstm3.executeQuery();
						while(rs3.next()) {

							String gpcrdbClassification = rs3.getString("additional_information");

							String gpcrdbDescription = "";
							pstm4.setString(1, gpcrdbClassification);
							ResultSet rs4 = pstm4.executeQuery();
							while(rs4.next()) {
								gpcrdbDescription = rs4.getString("description");
							}
							rs4.close();

							String link = gpcrdbClassification+" - " +gpcrdbDescription;

							int count = 0;
							if(link2count.containsKey(link)) {
								count = link2count.get(link);
							}
							count = count+1;

							link2count.put(link, new Integer(count));

						}
						rs3.close();
					}
					rs2.close();
					 */
					//int sequenceid = sids.get(i);
					if (ptbl2.containsKey(sequenceid)){
						int gpcrdbSequenceid = ptbl2.get(sequenceid);
						if (ptbl3.containsKey(gpcrdbSequenceid)){
							String gpcrdbClassification = ptbl3.get(gpcrdbSequenceid);
							String gpcrdbDescription = "";
							if (ptbl4.containsKey(gpcrdbClassification)){
								gpcrdbDescription = ptbl4.get(gpcrdbClassification);
							}
							String link = gpcrdbClassification+" - " +gpcrdbDescription;
							int count = 0;
							if(link2count.containsKey(link)) {
								count = link2count.get(link);
							}
							count = count+1;
							link2count.put(link, new Integer(count));
						}
					}
				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setString(1, code);
					pstm.setString(2, "gpcrdb");
					pstm.setString(3, link);
					pstm.setInt(4, count);
					pstm.setDouble(5, perc);

					pstm.executeUpdate();
				}
				if (statusCount % 1000 == 0){
					System.out.print(statusCount+"\n");
				}
			}
			rs.close();
			stm.close();


			pstm.close();
			//pstm1.close();
			//pstm2.close();
			//pstm3.close();
			//pstm4.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	private static void addOPMcrosslinks() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_cross_db " +
							"(code, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?)");



			//			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM fh_clusters WHERE code=?");

			//take only best PDBTM hit
			/*
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT distinct(sequences_other_database_sequenceid) " +
							"FROM other_database " +
							"WHERE sequenceid=? AND " +
							"db=\"pdbtm\" AND " +
							"query_coverage>="+CROSS_COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+CROSS_COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc LIMIT 1");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT name FROM sequences_other_database WHERE sequenceid=? AND db=\"pdbtm\"");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT classification FROM pdbtm2opm WHERE pdb_id=?");
			PreparedStatement pstm5 = CAMPS_CONNECTION.prepareStatement("SELECT description FROM other_database_hierarchies WHERE key_=? AND db=\"opm\"");
			 */
			PreparedStatement ptm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid, sequences_other_database_sequenceid " +
							"FROM other_database " +
							"WHERE " +
							"db=\"pdbtm\" AND " +
							"query_coverage>="+CROSS_COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+CROSS_COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc");
			System.out.print("\n Getting PDBTm hits \n");
			HashMap <Integer, Integer> ptbl2 = new HashMap<Integer, Integer>(); // key is the sequenceid and value is sequences_other_database_sequenceid
			ResultSet rptm2 = ptm2.executeQuery();
			while(rptm2.next()){
				int Sid = rptm2.getInt("sequenceid");
				int pdbtmSequenceid = rptm2.getInt("sequences_other_database_sequenceid");
				if(!ptbl2.containsKey(Sid)){
					ptbl2.put(Sid, pdbtmSequenceid);
				}
			}
			rptm2.close();
			ptm2.close();

			System.out.print("\n Getting names \n");
			HashMap <Integer, String> ptbl3 = new HashMap<Integer, String>(); // key is the seq_id and value is name
			PreparedStatement ptm3 = CAMPS_CONNECTION.prepareStatement("SELECT name,sequenceid FROM sequences_other_database WHERE db=\"pdbtm\"");
			ResultSet rptm3 = ptm3.executeQuery();
			while(rptm3.next()){
				String name = rptm3.getString(1);
				int id = rptm3.getInt(2);
				if(!ptbl3.containsKey(id)){
					ptbl3.put(id, name);
				}
			}
			ptm3.close();
			rptm3.close();

			System.out.print("\n Getting opm classifications \n");
			HashMap <String, String> ptbl4 = new HashMap<String, String>(); // key is the pdb_id and value is classification
			PreparedStatement ptm4 = CAMPS_CONNECTION.prepareStatement("SELECT classification,pdb_id FROM pdbtm2opm ");
			ResultSet rptm4 = ptm4.executeQuery();
			while(rptm4.next()){
				String c = rptm4.getString(1); // classification opm
				String p = rptm4.getString(2); // pdb Id
				if(!ptbl4.containsKey(p)){
					ptbl4.put(p, c);
				}
				else if(ptbl4.containsKey(p)){
					String cc = ptbl4.get(p);
					c = c + "*" +cc;
				}
			}
			ptm4.close();
			rptm4.close();

			PreparedStatement ptm5 = CAMPS_CONNECTION.prepareStatement("SELECT description,key_ FROM other_database_hierarchies WHERE db=\"opm\"");
			System.out.print("\n Getting opm descriptions \n");
			ResultSet rptm5 = ptm5.executeQuery();
			HashMap <String, String> ptbl5 = new HashMap<String, String>(); // key is the key_ and value is description
			while(rptm5.next()){
				String d = rptm5.getString(1);
				String k = rptm5.getString(2);
				if(!ptbl5.containsKey(k)){
					ptbl5.put(k, d);
				}
			}
			ptm5.close();
			rptm5.close();


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT code FROM fh_clusters_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				String code = rs.getString("code");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				BitSet members = new BitSet();
				ArrayList<Integer> temp = ptbl1.get(code);
				for (int i =0;i<=temp.size()-1;i++){
					int sequenceID = temp.get(i);
					members.set(sequenceID);
				}
				int clusterSize = members.cardinality();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get pdbtm and opm data for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					/*
					pstm2.setInt(1, sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						int pdbtmSequenceid = rs2.getInt("sequences_other_database_sequenceid");
						pstm3.setInt(1, pdbtmSequenceid);
						ResultSet rs3 = pstm3.executeQuery();
						while(rs3.next()) {

							String pdbCode = rs3.getString("name");
							pdbCode = pdbCode.split("_")[0].toLowerCase();

							pstm4.setString(1, pdbCode);
							ResultSet rs4 = pstm4.executeQuery();
							while(rs4.next()) {
								String opmClassification = rs4.getString("classification");

								String description = "";
								pstm5.setString(1, opmClassification);
								ResultSet rs5 = pstm5.executeQuery();
								while(rs5.next()) {
									description = rs5.getString("description");
								}
								rs5.close();

								String link = opmClassification+" - " +description;

								int count = 0;
								if(link2count.containsKey(link)) {
									count = link2count.get(link);
								}
								count = count+1;

								link2count.put(link, new Integer(count));
							}
							rs4.close();
						}
						rs3.close();
					}
					rs2.close();
					 */
					//int sequenceid = sids.get(i);
					int pdbtmSequenceid = 0;
					if(ptbl2.containsKey(sequenceid)){
						pdbtmSequenceid = ptbl2.get(sequenceid);
					}
					else {
						continue;
					}

					String pdbCode = ptbl3.get(pdbtmSequenceid);
					pdbCode = pdbCode.split("_")[0].toLowerCase();
					String opmClassification = "";
					if(ptbl4.containsKey(pdbCode)){
						opmClassification = ptbl4.get(pdbCode);
					}
					else{
						continue;
					}
					String description = "";
					description = ptbl5.get(opmClassification);
					String link = opmClassification+" - " +description;
					int count = 0;
					if(link2count.containsKey(link)) {
						count = link2count.get(link);
					}
					count = count+1;
					link2count.put(link, new Integer(count));
				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						
					int count = link2count.get(link).intValue();
					double perc = 100 * ((double) count/clusterSize);
					pstm.setString(1, code);
					pstm.setString(2, "opm");
					pstm.setString(3, link);
					pstm.setInt(4, count);
					pstm.setDouble(5, perc);

					pstm.executeUpdate();
				}
			}
			rs.close();
			stm.close();

			pstm.close();
			//pstm1.close();
			//pstm2.close();
			//pstm3.close();
			//pstm4.close();
			//pstm5.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	private static void addPFAMcrosslinks() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_cross_db " +
							"(code, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?)");



			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM fh_clusters WHERE code=?");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT accession,description FROM domains_pfam WHERE sequenceid=? ORDER BY begin");


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT code FROM fh_clusters_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				String code = rs.getString("code");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				BitSet members = new BitSet();
				ArrayList<Integer> temp = ptbl1.get(code);
				for (int i =0;i<=temp.size()-1;i++){
					int sequenceID = temp.get(i);
					members.set(sequenceID);
				}

				int clusterSize = members.cardinality();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get pfam data for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					//
					//caution: sequences can have multiple PFAM
					//classifications (multidomain sequences!)
					//
					String link = "";

					pstm2.setInt(1, sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {

						String pfamAccession = rs2.getString("accession");
						String pfamDescription= rs2.getString("description");

						String tmpLink = pfamAccession+" - " +pfamDescription;
						link += "#"+tmpLink;												
					}

					if(link.equals("")) {
						continue;
					}

					link = link.substring(1);

					int count = 0;
					if(link2count.containsKey(link)) {
						count = link2count.get(link);
					}
					count = count+1;

					link2count.put(link, new Integer(count));


					rs2.close();
				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setString(1, code);
					pstm.setString(2, "pfam");
					pstm.setString(3, link);
					pstm.setInt(4, count);
					pstm.setDouble(5, perc);

					pstm.executeUpdate();
				}
			}
			rs.close();
			stm.close();


			pstm.close();
			//pstm1.close();
			pstm2.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	private static void addSCOPcrosslinks() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_cross_db " +
							"(code, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?)");
			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM fh_clusters WHERE code=?");

			//take only best PDBTM hit
			/*
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT distinct(sequences_other_database_sequenceid) " +
							"FROM other_database " +
							"WHERE sequenceid=? AND " +
							"db=\"pdbtm\" AND " +
							"query_coverage>="+CROSS_COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+CROSS_COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc LIMIT 1");
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT name FROM sequences_other_database WHERE sequenceid=? AND db=\"pdbtm\"");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT classification FROM pdbtm2scop_cath WHERE pdb_id=? AND db=\"scop\"");
			PreparedStatement pstm5 = CAMPS_CONNECTION.prepareStatement("SELECT description FROM other_database_hierarchies WHERE key_=? AND db=\"scop\"");
			 */
			PreparedStatement ptm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid, sequences_other_database_sequenceid " +
							"FROM other_database " +
							"WHERE " +
							"db=\"pdbtm\" AND " +
							"query_coverage>="+CROSS_COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+CROSS_COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc");
			System.out.print("\n Getting PDBTm hits \n");
			HashMap <Integer, Integer> ptbl2 = new HashMap<Integer, Integer>(); // key is the sequenceid and value is sequences_other_database_sequenceid

			ResultSet rptm2 = ptm2.executeQuery();
			while(rptm2.next()){
				int Sid = rptm2.getInt("sequenceid");
				int pdbtmSequenceid = rptm2.getInt("sequences_other_database_sequenceid");

				if(!ptbl2.containsKey(Sid)){
					ptbl2.put(Sid, pdbtmSequenceid);
				}
			}
			rptm2.close();
			ptm2.close();

			System.out.print("\n Getting scop classifications \n");
			HashMap <Integer, String> ptbl3 = new HashMap<Integer, String>(); // key is the seq_id and value is name
			PreparedStatement ptm3 = CAMPS_CONNECTION.prepareStatement("SELECT name,sequenceid FROM sequences_other_database WHERE db=\"pdbtm\"");
			ResultSet rptm3 = ptm3.executeQuery();
			while(rptm3.next()){
				String name = rptm3.getString(1);
				int id = rptm3.getInt(2);
				if(!ptbl3.containsKey(id)){
					ptbl3.put(id, name);
				}
			}
			ptm3.close();
			rptm3.close();

			// get all data in hash instead of pstm4

			System.out.print("\n Getting scop classifications \n");
			HashMap <String, String> ptbl4 = new HashMap<String, String>(); // key is the pdb_id and value is classification
			PreparedStatement ptm4 = CAMPS_CONNECTION.prepareStatement("SELECT classification,pdb_id FROM pdbtm2scop_cath WHERE db=\"scop\"");
			ResultSet rptm4 = ptm4.executeQuery();
			while(rptm4.next()){
				String c = rptm4.getString(1);
				String p = rptm4.getString(2);
				if(!ptbl4.containsKey(p)){
					ptbl4.put(p, c);
				}
				else if(ptbl4.containsKey(p)){
					String cc = ptbl4.get(p);
					c = c + "*" +cc;
				}
			}
			ptm4.close();
			rptm4.close();

			System.out.print("\n Getting scop descriptions \n");

			// get all data in hash instead of pstm5
			PreparedStatement ptm5 = CAMPS_CONNECTION.prepareStatement("SELECT description,key_ FROM other_database_hierarchies WHERE db=\"scop\"");
			ResultSet rptm5 = ptm5.executeQuery();
			HashMap <String, String> ptbl5 = new HashMap<String, String>(); // key is the key_ and value is description
			while(rptm5.next()){
				String d = rptm5.getString(1);
				String k = rptm5.getString(2);
				if(!ptbl5.containsKey(k)){
					ptbl5.put(k, d);
				}
			}
			ptm5.close();
			rptm5.close();

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT code FROM fh_clusters_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				String code = rs.getString("code");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");
				//extract cluster members
				BitSet members = new BitSet();
				ArrayList<Integer> temp = ptbl1.get(code);
				for (int i =0;i<=temp.size()-1;i++){
					int sequenceID = temp.get(i);
					members.set(sequenceID);
				}

				int clusterSize = members.cardinality();
				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();
				//get pdbtm and scop data for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {
					/*
					pstm2.setInt(1, sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						int pdbtmSequenceid = rs2.getInt("sequences_other_database_sequenceid");
						pstm3.setInt(1, pdbtmSequenceid);
						ResultSet rs3 = pstm3.executeQuery();
						while(rs3.next()) {
							String pdbCode = rs3.getString("name");
							//
							//caution: pdb sequences can have multiple SCOP
							//classifications (multidomain sequences!)
							//
							String link = "";

							pstm4.setString(1, pdbCode);
							ResultSet rs4 = pstm4.executeQuery();
							while(rs4.next()) {
								String scopClassification = rs4.getString("classification");
								String tmp[] = scopClassification.split("\\.");
								String scopFoldClassification = tmp[0]+"."+tmp[1];

								String scopDescription = "";
								pstm5.setString(1, scopFoldClassification);
								ResultSet rs5 = pstm5.executeQuery();
								while(rs5.next()) {
									scopDescription = rs5.getString("description");
								}
								rs5.close();

								String tmpLink = scopFoldClassification+" - " +scopDescription;
								link += "#"+tmpLink;							
							}
							rs4.close();

							if(link.equals("")) {
								continue;
							}

							link = link.substring(1);

							int count = 0;
							if(link2count.containsKey(link)) {
								count = link2count.get(link);
							}
							count = count+1;

							link2count.put(link, new Integer(count));							
						}
						rs3.close();
					}
					rs2.close();
					 */
					//int sequenceid = sids.get(i);
					if (ptbl2.containsKey(sequenceid)){
						int pdbtmSequenceid = ptbl2.get(sequenceid);

						if(ptbl3.containsKey(pdbtmSequenceid)){
							String pdbCode = ptbl3.get(pdbtmSequenceid);

							//caution: pdb sequences can have multiple CATH
							//classifications (multidomain sequences!)
							//
							String link = "";
							// commented out changes here due to hashtable
							if(ptbl4.containsKey(pdbCode)){
								String scopClassifications = ptbl4.get(pdbCode);// retrieve the classification for this pdb code
								String sclasses[] = scopClassifications.split("\\*");
								for(int x =0;x<=sclasses.length-1;x++){
									String tmp[] = sclasses[x].split("\\.");
									String scopFoldClassification = tmp[0]+"."+tmp[1]+"."+tmp[2];
									String scopDescription = "";
									if (ptbl5.containsKey(scopFoldClassification)){
										scopDescription = ptbl5.get(scopFoldClassification);
									}
									String tmpLink = scopFoldClassification+" - " +scopDescription;
									link += "#"+tmpLink;
								}
								//String tmp[] = scopClassification.split("\\.");
							}
							if(link.equals("")) {
								continue;
							}
							link = link.substring(1);
							int count = 0;
							if(link2count.containsKey(link)) {
								count = link2count.get(link);
							}
							count = count+1;
							link2count.put(link, new Integer(count));
						}
					}
				}
				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setString(1, code);
					pstm.setString(2, "scop");
					pstm.setString(3, link);
					pstm.setInt(4, count);
					pstm.setDouble(5, perc);

					pstm.executeUpdate();
				}
			}
			rs.close();
			stm.close();

			pstm.close();
			//pstm1.close();
			//pstm2.close();
			//pstm3.close();
			//pstm4.close();
			//pstm5.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	private static void addSUPERFAMILYcrosslinks() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_cross_db " +
							"(code, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?)");



			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM fh_clusters WHERE code=?");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT accession,description FROM domains_superfamily WHERE sequenceid=? ORDER BY begin");


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT code FROM fh_clusters_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				String code = rs.getString("code");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				BitSet members = new BitSet();
				ArrayList<Integer> temp = ptbl1.get(code);
				for (int i =0;i<=temp.size()-1;i++){
					int sequenceID = temp.get(i);
					members.set(sequenceID);
				}

				int clusterSize = members.cardinality();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get superfamily data for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					//
					//caution: sequences can have multiple PFAM
					//classifications (multidomain sequences!)
					//
					String link = "";

					pstm2.setInt(1, sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {

						String sfAccession = rs2.getString("accession");
						String sfDescription= rs2.getString("description");

						String tmpLink = sfAccession+" - " +sfDescription;
						link += "#"+tmpLink;												
					}

					if(link.equals("")) {
						continue;
					}

					link = link.substring(1);

					int count = 0;
					if(link2count.containsKey(link)) {
						count = link2count.get(link);
					}
					count = count+1;

					link2count.put(link, new Integer(count));


					rs2.close();
				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setString(1, code);
					pstm.setString(2, "superfamily");
					pstm.setString(3, link);
					pstm.setInt(4, count);
					pstm.setDouble(5, perc);

					pstm.executeUpdate();
				}
			}
			rs.close();
			stm.close();
			pstm.close();
			//pstm1.close();
			pstm2.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	private static void addTCDBcrosslinks() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_cross_db " +
							"(code, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?)");



			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM fh_clusters WHERE code=?");

			//take only best TCDB hit
			/*
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT distinct(sequences_other_database_sequenceid) " +
							"FROM other_database " +
							"WHERE sequenceid=? AND " +
							"db=\"tcdb\" AND " +
							"query_coverage>="+CROSS_COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+CROSS_COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc LIMIT 1");
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT name FROM sequences_other_database WHERE sequenceid=? AND db=\"tcdb\"");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT description FROM other_database_hierarchies WHERE key_=? AND db=\"tcdb\"");
			 */
			PreparedStatement ptm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid, sequences_other_database_sequenceid " +
							"FROM other_database " +
							"WHERE " +
							"db=\"tcdb\" AND " +
							"query_coverage>="+CROSS_COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+CROSS_COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc");
			System.out.print("\n Getting TCDB hits \n");
			HashMap <Integer, Integer> ptbl2 = new HashMap<Integer, Integer>(); // key is the sequenceid and value is sequences_other_database_sequenceid
			ResultSet rptm2 = ptm2.executeQuery();
			while(rptm2.next()){
				int Sid = rptm2.getInt("sequenceid");
				int tcdbSequenceid = rptm2.getInt("sequences_other_database_sequenceid");
				if(!ptbl2.containsKey(Sid)){
					ptbl2.put(Sid, tcdbSequenceid);
				}
			}
			rptm2.close();
			ptm2.close();

			//PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT name FROM sequences_other_database WHERE sequenceid=? AND db=\"tcdb\"");
			System.out.print("\n Getting names \n");
			HashMap <Integer, String> ptbl3 = new HashMap<Integer, String>(); // key is the seq_id and value is name
			PreparedStatement ptm3 = CAMPS_CONNECTION.prepareStatement("SELECT name,sequenceid FROM sequences_other_database WHERE db=\"tcdb\"");
			ResultSet rptm3 = ptm3.executeQuery();
			while(rptm3.next()){
				String name = rptm3.getString(1);
				int id = rptm3.getInt(2);
				if(!ptbl3.containsKey(id)){
					ptbl3.put(id, name);
				}
			}
			ptm3.close();
			rptm3.close();

			//PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT description FROM other_database_hierarchies WHERE key_=? AND db=\"tcdb\"");
			PreparedStatement ptm4 = CAMPS_CONNECTION.prepareStatement("SELECT description,key_ FROM other_database_hierarchies WHERE db=\"tcdb\"");
			System.out.print("\n Getting tcdb descriptions \n");
			ResultSet rptm4 = ptm4.executeQuery();
			HashMap <String, String> ptbl4 = new HashMap<String, String>(); // key is the key_ and value is description
			while(rptm4.next()){
				String d = rptm4.getString(1);
				String k = rptm4.getString(2);
				if(!ptbl4.containsKey(k)){
					ptbl4.put(k, d);
				}
			}
			ptm4.close();
			rptm4.close();


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT code FROM fh_clusters_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				String code = rs.getString("code");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				BitSet members = new BitSet();
				ArrayList<Integer> temp = ptbl1.get(code);
				for (int i =0;i<=temp.size()-1;i++){
					int sequenceID = temp.get(i);
					members.set(sequenceID);
				}

				int clusterSize = members.cardinality();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get tcdb data for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					/*
					pstm2.setInt(1, sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						int tcdbSequenceid = rs2.getInt("sequences_other_database_sequenceid");
						pstm3.setInt(1, tcdbSequenceid);
						ResultSet rs3 = pstm3.executeQuery();
						while(rs3.next()) {

							String tcdbCode = rs3.getString("name");
							String[] tmp = tcdbCode.split("\\.");
							String tcdbClassification = tmp[0]+"."+tmp[1]+"."+tmp[2];

							String tcdbDescription = "";
							pstm4.setString(1, tcdbClassification);
							ResultSet rs4 = pstm4.executeQuery();
							while(rs4.next()) {
								tcdbDescription = rs4.getString("description");
							}
							rs4.close();

							String link = tcdbClassification+" - " +tcdbDescription;

							int count = 0;
							if(link2count.containsKey(link)) {
								count = link2count.get(link);
							}
							count = count+1;

							link2count.put(link, new Integer(count));

						}
						rs3.close();
					}
					rs2.close();
					 */
					//int sequenceid = sids.get(i);
					int tcdbSequenceid = 0;
					if(ptbl2.containsKey(sequenceid)){
						tcdbSequenceid = ptbl2.get(sequenceid);
					}
					else{
						continue;
					}
					String tcdbCode = "";
					if(ptbl3.containsKey(tcdbSequenceid)){
						tcdbCode = ptbl3.get(tcdbSequenceid);
					}
					else{
						continue;
					}


					String[] tmp = tcdbCode.split("\\.");
					String tcdbClassification = tmp[0]+"."+tmp[1]+"."+tmp[2];

					String tcdbDescription = "";
					tcdbDescription = ptbl4.get(tcdbClassification);

					String link = tcdbClassification+" - " +tcdbDescription;

					int count = 0;
					if(link2count.containsKey(link)) {
						count = link2count.get(link);
					}
					count = count+1;

					link2count.put(link, new Integer(count));
				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setString(1, code);
					pstm.setString(2, "tcdb");
					pstm.setString(3, link);
					pstm.setInt(4, count);
					pstm.setDouble(5, perc);

					pstm.executeUpdate();
				}
			}
			rs.close();
			stm.close();


			pstm.close();
			//pstm1.close();
			//pstm2.close();
			//pstm3.close();
			//pstm4.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Associate FH-clusters with PDB structures.
	 * version2 as it is based on the pdb identity 30% whereas the version 1 is based on identity 90
	 * 
	 */
	public void structures0() {
		try {

			String sequences ="";
			ArrayList <String> done  = new ArrayList<String>();

			PreparedStatement ptm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid, sequences_other_database_sequenceid," +
							"query_coverage,hit_coverage,bitscore,evalue,ident " +
							"FROM other_database " +
					"WHERE db=\"pdbtm\"");
			ResultSet rst1 = ptm1.executeQuery();
			HashMap <Integer, ArrayList <OtherDatabase_Strucutre>> ptbl1 = new HashMap<Integer, ArrayList<OtherDatabase_Strucutre>>();
			//key -> sequenceid and value is the rest
			while(rst1.next()){
				int sequenceid = rst1.getInt("sequenceid");
				int sequenceidpdb = rst1.getInt("sequences_other_database_sequenceid");
				float queryCoverage = rst1.getFloat("query_coverage");
				float hitCoverage = rst1.getFloat("hit_coverage");
				float bitscore = rst1.getFloat("bitscore");
				double evalue = rst1.getDouble("evalue");
				float identity = rst1.getFloat("ident");

				if (ptbl1.containsKey(sequenceid)){
					ArrayList <OtherDatabase_Strucutre> temp = ptbl1.get(sequenceid);
					OtherDatabase_Strucutre entry = new OtherDatabase_Strucutre();
					entry.set(sequenceidpdb, queryCoverage, hitCoverage, bitscore, evalue, identity);
					temp.add(entry);
					ptbl1.put(sequenceid, temp);
				}
				else{
					ArrayList <OtherDatabase_Strucutre> temp = new ArrayList<OtherDatabase_Strucutre>();
					OtherDatabase_Strucutre entry = new OtherDatabase_Strucutre();
					entry.set(sequenceidpdb, queryCoverage, hitCoverage, bitscore, evalue, identity);
					temp.add(entry);
					ptbl1.put(sequenceid, temp);
				}
			}
			rst1.close();
			ptm1.close();
			System.out.print("Populated Other Db\n");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT name " +
							"FROM sequences_other_database " +
					"WHERE sequenceid=? AND db=\"pdbtm\"");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_structures0 " +
							"(code, pdbid, method, resolution, query_coverage, hit_coverage, bitscore, evalue, ident) " +
							"VALUES " +
					"(?,?,?,?,?,?,?,?,?)");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_structures0 " +
							"(code, pdbid, method, query_coverage, hit_coverage, bitscore, evalue, ident) " +
							"VALUES " +
					"(?,?,?,?,?,?,?,?)");

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT code FROM fh_clusters_nr_info");
			int count = 0;
			while(rs.next()) {
				count++;
				String code = rs.getString("code");
				System.out.print("Processing: "+code+" ("+count+")\n");

				BitSet members = getMembers(code);

				ArrayList<String> alreadyFoundStructures = new ArrayList<String>();

				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					//if (){
					//pstm1.setInt(1, sequenceid);
					//ResultSet rs1 = pstm1.executeQuery();
					if (ptbl1.containsKey(sequenceid)){
						//while(rs1.next()) {

						/*
						int sequenceidpdb = rs1.getInt("sequences_other_database_sequenceid");
						float queryCoverage = rs1.getFloat("query_coverage");
						float hitCoverage = rs1.getFloat("hit_coverage");
						float bitscore = rs1.getFloat("bitscore");
						double evalue = rs1.getDouble("evalue");
						float identity = rs1.getFloat("ident");
						 */
						ArrayList<OtherDatabase_Strucutre> returns= ptbl1.get(sequenceid);
						// sort the above array highest to lowest based on identity
						// so the best hits are reported first
						if(returns.size()>1){
							//System.out.println("Original");
							//ClustersAndStructures_ForMcl0.print(returns);
							returns = ClustersAndStructures_ForMcl0.sort(returns);
							//System.out.println("Sorted");
							//ClustersAndStructures_ForMcl0.print(returns);
						}
						for(int x =0;x<=returns.size()-1;x++){
							OtherDatabase_Strucutre yo = returns.get(x);

							int sequenceidpdb = yo.sequences_other_database_sequenceid;
							float queryCoverage = yo.query_coverage;
							float hitCoverage = yo.hit_coverage;
							float bitscore = yo.bitscore;
							double evalue = yo.evalue;
							float identity = yo.ident;

							//if(queryCoverage >= QUERY_COVERAGE_THRESHOLD && hitCoverage >= HIT_COVERAGE_THRESHOLD && identity >= IDENTITY_THRESHOLD) {

								String name = "";
								pstm2.setInt(1, sequenceidpdb);
								ResultSet rs2 = pstm2.executeQuery();
								while(rs2.next()) {
									name = rs2.getString("name");
								}
								rs2.close();

								String pdbid = name.split("_")[0];
								if(!done.contains(pdbid)){
									sequences = sequences+","+pdbid.trim();
									done.add(pdbid);

								}
								if(alreadyFoundStructures.contains(name)) {
									continue;
								}

								//get method and resolution from downloaded files

								float resolution = -1;
								String method = "";

								//URL url = new URL (BASE_URL+pdbid+".pdb");
								//System.out.print("\n"+url+"\n");
								//URLConnection urlConn = url.openConnection();

								//DataInputStream input = new DataInputStream (urlConn.getInputStream ());
								//InputStreamReader isr = new InputStreamReader(input);
								//BufferedReader br = new BufferedReader(isr);

								//BASE_Address
								String file_address = BASE_Address+pdbid+".pdb";

								File f = new File(file_address);
								if(f.exists() && !f.isDirectory()) { 

									BufferedReader br = new BufferedReader(new FileReader(f));
									String line;
									while((line = br.readLine()) != null) {
										line = line.trim();
										if(line.startsWith("EXPDTA")) {						
											method = line.replace("EXPDTA", "").trim();
											if(method.startsWith("NMR")) {
												method = method.split(",")[0].trim();
											}	
											if(!(method.equals("ELECTRON DIFFRACTION") || method.equals("X-RAY DIFFRACTION"))) {
												break;
											}
										}
										else if(line.startsWith("REMARK")) {
											Matcher m = RESOLUTION_PATTERN.matcher(line);
											if(m.matches()) {
												resolution = Float.parseFloat(m.group(1));
												break;
											}						
										}
									}
									//input.close();
									br.close();
								}
								//****** System.out.println(id+"\t"+method+"\t"+resolution);
								if(resolution != -1) {

									pstm3.setString(1, code);
									pstm3.setString(2, name);
									pstm3.setString(3, method);
									pstm3.setFloat(4, resolution);
									pstm3.setFloat(5, queryCoverage);
									pstm3.setFloat(6, hitCoverage);
									pstm3.setFloat(7, bitscore);
									pstm3.setDouble(8, evalue);
									pstm3.setFloat(9, identity);
									pstm3.executeUpdate();
								}
								else {

									pstm4.setString(1, code);
									pstm4.setString(2, name);
									pstm4.setString(3, method);
									pstm4.setFloat(4, queryCoverage);
									pstm4.setFloat(5, hitCoverage);
									pstm4.setFloat(6, bitscore);
									pstm4.setDouble(7, evalue);
									pstm4.setFloat(8, identity);
									pstm4.executeUpdate();
								}


								alreadyFoundStructures.add(name);
							//}

							//	}
						}
					}
					//rs1.close();
				}
			}
			rs.close();
			stm.close();

			//pstm1.close();
			pstm2.close();
			pstm3.close();
			pstm4.close();
			//System.out.print("\n\n"+sequences+"\n\n");


		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Associate FH-clusters with PDB structures.
	 */
	public void structures() {
		try {

			String sequences ="";
			ArrayList <String> done  = new ArrayList<String>();

			PreparedStatement ptm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid, sequences_other_database_sequenceid," +
							"query_coverage,hit_coverage,bitscore,evalue,ident " +
							"FROM other_database " +
					"WHERE db=\"pdbtm\"");
			ResultSet rst1 = ptm1.executeQuery();
			HashMap <Integer, ArrayList <OtherDatabase_Strucutre>> ptbl1 = new HashMap<Integer, ArrayList<OtherDatabase_Strucutre>>();
			//key -> sequenceid and value is the rest
			while(rst1.next()){
				int sequenceid = rst1.getInt("sequenceid");
				int sequenceidpdb = rst1.getInt("sequences_other_database_sequenceid");
				float queryCoverage = rst1.getFloat("query_coverage");
				float hitCoverage = rst1.getFloat("hit_coverage");
				float bitscore = rst1.getFloat("bitscore");
				double evalue = rst1.getDouble("evalue");
				float identity = rst1.getFloat("ident");

				if (ptbl1.containsKey(sequenceid)){
					ArrayList <OtherDatabase_Strucutre> temp = ptbl1.get(sequenceid);
					OtherDatabase_Strucutre entry = new OtherDatabase_Strucutre();
					entry.set(sequenceidpdb, queryCoverage, hitCoverage, bitscore, evalue, identity);
					temp.add(entry);
					ptbl1.put(sequenceid, temp);
				}
				else{
					ArrayList <OtherDatabase_Strucutre> temp = new ArrayList<OtherDatabase_Strucutre>();
					OtherDatabase_Strucutre entry = new OtherDatabase_Strucutre();
					entry.set(sequenceidpdb, queryCoverage, hitCoverage, bitscore, evalue, identity);
					temp.add(entry);
					ptbl1.put(sequenceid, temp);
				}
			}
			rst1.close();
			ptm1.close();
			System.out.print("Populated Other Db\n");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT name " +
							"FROM sequences_other_database " +
					"WHERE sequenceid=? AND db=\"pdbtm\"");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_structures " +
							"(code, pdbid, method, resolution, query_coverage, hit_coverage, bitscore, evalue, ident) " +
							"VALUES " +
					"(?,?,?,?,?,?,?,?,?)");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_structures " +
							"(code, pdbid, method, query_coverage, hit_coverage, bitscore, evalue, ident) " +
							"VALUES " +
					"(?,?,?,?,?,?,?,?)");

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT code FROM fh_clusters_nr_info");
			int count = 0;
			while(rs.next()) {
				count++;
				String code = rs.getString("code");
				System.out.print("Processing: "+code+" ("+count+")\n");

				BitSet members = getMembers(code);

				ArrayList<String> alreadyFoundStructures = new ArrayList<String>();

				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					//if (){
					//pstm1.setInt(1, sequenceid);
					//ResultSet rs1 = pstm1.executeQuery();
					if (ptbl1.containsKey(sequenceid)){
						//while(rs1.next()) {

						/*
						int sequenceidpdb = rs1.getInt("sequences_other_database_sequenceid");
						float queryCoverage = rs1.getFloat("query_coverage");
						float hitCoverage = rs1.getFloat("hit_coverage");
						float bitscore = rs1.getFloat("bitscore");
						double evalue = rs1.getDouble("evalue");
						float identity = rs1.getFloat("ident");
						 */
						ArrayList<OtherDatabase_Strucutre> returns= ptbl1.get(sequenceid);
						for(int x =0;x<=returns.size()-1;x++){
							OtherDatabase_Strucutre yo = returns.get(x);

							int sequenceidpdb = yo.sequences_other_database_sequenceid;
							float queryCoverage = yo.query_coverage;
							float hitCoverage = yo.hit_coverage;
							float bitscore = yo.bitscore;
							double evalue = yo.evalue;
							float identity = yo.ident;

							if(queryCoverage >= QUERY_COVERAGE_THRESHOLD && hitCoverage >= HIT_COVERAGE_THRESHOLD && identity >= IDENTITY_THRESHOLD) {

								String name = "";
								pstm2.setInt(1, sequenceidpdb);
								ResultSet rs2 = pstm2.executeQuery();
								while(rs2.next()) {
									name = rs2.getString("name");
								}
								rs2.close();

								String pdbid = name.split("_")[0];
								if(!done.contains(pdbid)){
									sequences = sequences+","+pdbid.trim();
									done.add(pdbid);

								}
								if(alreadyFoundStructures.contains(name)) {
									continue;
								}

								//get method and resolution from downloaded files

								float resolution = -1;
								String method = "";

								//URL url = new URL (BASE_URL+pdbid+".pdb");
								//System.out.print("\n"+url+"\n");
								//URLConnection urlConn = url.openConnection();

								//DataInputStream input = new DataInputStream (urlConn.getInputStream ());
								//InputStreamReader isr = new InputStreamReader(input);
								//BufferedReader br = new BufferedReader(isr);

								//BASE_Address
								String file_address = BASE_Address+pdbid+".pdb";

								File f = new File(file_address);
								if(f.exists() && !f.isDirectory()) { 

									BufferedReader br = new BufferedReader(new FileReader(f));
									String line;
									while((line = br.readLine()) != null) {
										line = line.trim();
										if(line.startsWith("EXPDTA")) {						
											method = line.replace("EXPDTA", "").trim();
											if(method.startsWith("NMR")) {
												method = method.split(",")[0].trim();
											}	
											if(!(method.equals("ELECTRON DIFFRACTION") || method.equals("X-RAY DIFFRACTION"))) {
												break;
											}
										}
										else if(line.startsWith("REMARK")) {
											Matcher m = RESOLUTION_PATTERN.matcher(line);
											if(m.matches()) {
												resolution = Float.parseFloat(m.group(1));
												break;
											}						
										}
									}
									//input.close();
									br.close();
								}
								//****** System.out.println(id+"\t"+method+"\t"+resolution);
								if(resolution != -1) {

									pstm3.setString(1, code);
									pstm3.setString(2, name);
									pstm3.setString(3, method);
									pstm3.setFloat(4, resolution);
									pstm3.setFloat(5, queryCoverage);
									pstm3.setFloat(6, hitCoverage);
									pstm3.setFloat(7, bitscore);
									pstm3.setDouble(8, evalue);
									pstm3.setFloat(9, identity);
									pstm3.executeUpdate();
								}
								else {

									pstm4.setString(1, code);
									pstm4.setString(2, name);
									pstm4.setString(3, method);
									pstm4.setFloat(4, queryCoverage);
									pstm4.setFloat(5, hitCoverage);
									pstm4.setFloat(6, bitscore);
									pstm4.setDouble(7, evalue);
									pstm4.setFloat(8, identity);
									pstm4.executeUpdate();
								}


								alreadyFoundStructures.add(name);
							}

							//	}
						}
					}
					//rs1.close();
				}
			}
			rs.close();
			stm.close();

			//pstm1.close();
			pstm2.close();
			pstm3.close();
			pstm4.close();
			//System.out.print("\n\n"+sequences+"\n\n");


		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}


	private static BitSet getMembers(String code) {

		BitSet members = null;

		try {

			members = new BitSet();

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery(
					"SELECT sequenceid " +
							"FROM fh_clusters " +
							"WHERE code=\""+code+"\"");

			while(rs.next()) {
				int sequenceID = rs.getInt("sequenceid");
				members.set(sequenceID);
			}
			rs.close();
			stm.close();

		}catch(Exception e) {
			e.printStackTrace();
		}

		return members;
	}


	/*
	 * Add number of cores to each FH-cluster.
	 */
	public void addNumCores1() {
		try {	


			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"SELECT max(tms_core_id) " +
							"FROM fh_clusters_tms_cores " +
					"WHERE code=?");

			Statement stm = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery(
					"SELECT id, " +
							"code,cores " +
					"FROM fh_clusters_info FOR UPDATE");
			int statusCounter = 0;
			while(rs.next()) {

				statusCounter++;
				if (statusCounter % 1000 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter % 100000 == 0) {
					System.out.write('\n');
					System.out.flush();
				}

				String code = rs.getString("code");

				int numCores = 0;

				pstm.setString(1, code);
				ResultSet rsx = pstm.executeQuery();
				while(rsx.next()) {
					numCores = rsx.getInt("max(tms_core_id)");
				}
				rsx.close();

				rs.updateInt("cores", numCores);
				rs.updateRow();

			}
			rs.close();

			stm.close();
			pstm.close();


		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if (CAMPS_CONNECTION != null) {/*
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			 */
			}
		}
	}


	/*
	 * Add number of cores to each FH-cluster.
	 */
	public void addNumCores2() {
		try {	


			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"SELECT max(tms_core_id) " +
							"FROM fh_clusters_tms_cores " +
					"WHERE code=?");

			Statement stm = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery(
					"SELECT id, " +
							"code,cores " +
					"FROM fh_clusters_nr_info FOR UPDATE");
			int statusCounter = 0;
			while(rs.next()) {

				statusCounter++;
				if (statusCounter % 1000 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter % 100000 == 0) {
					System.out.write('\n');
					System.out.flush();
				}

				String code = rs.getString("code");

				int numCores = 0;

				pstm.setString(1, code);
				ResultSet rsx = pstm.executeQuery();
				while(rsx.next()) {
					numCores = rsx.getInt("max(tms_core_id)");
				}
				rsx.close();

				rs.updateInt("cores", numCores);
				rs.updateRow();

			}
			rs.close();

			stm.close();
			pstm.close();


		} catch(Exception e) {
			e.printStackTrace();
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


	/**
	 * Add mean and median pairwise sequence identity for each FH-cluster
	 * to table fh_clusters_info.
	 */
	public void addAPSI1() {
		try {	

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid " +
							"FROM fh_clusters " +
					"WHERE code=?");
			// to have the sequences in FH clusters
			HashMap <Integer,Integer> SeqsFH = new HashMap<Integer,Integer>();
			PreparedStatement pstmx = CAMPS_CONNECTION.prepareStatement(
					"SELECT distinct(sequenceid) FROM fh_clusters ");
			System.out.print("\n Running Query \n");
			System.out.flush();
			ResultSet rsx = pstmx.executeQuery();
			System.out.print("\n Populating \n");
			System.out.flush();
			while(rsx.next()){
				Integer id = rsx.getInt(1);
				if (!SeqsFH.containsKey(id)){
					SeqsFH.put(id, 0);
				}
			}
			System.out.print("\n Hashtable of SeqFh "+SeqsFH.size()+"\n");
			System.out.flush();

			//PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
			//		"SELECT sequenceid_hit,identity FROM alignments WHERE sequenceid_query=?");
			// removing pstm2 and populating the alignments_initial 
			PreparedStatement pstm = null;
			try{
				if(dictbool == false){

					int idx =1;
					for (int i=1; i<=170;i++){
						System.out.print("\n Running Query \n");
						System.out.flush();
						pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial limit "+idx+","+1000000);
						System.out.print("\n Query Done \n");
						System.out.flush();
						//pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial");
						idx = i*1000000;
						System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
						System.out.flush();
						ResultSet rs = pstm.executeQuery();
						while(rs.next()) {
							int idhit = rs.getInt("seqid_hit");
							int key = rs.getInt ("seqid_query");
							float identity = rs.getFloat("identity");

							if(SeqsFH.containsKey(idhit) || SeqsFH.containsKey(key)){
								if (dict.containsKey(key)){ // the key already doesnt exist...so that you dont delete the previous info of the key
									Dictionary temp = (Dictionary)dict.get(key);
									temp.set(idhit, identity);
									dict.put(key,temp);
								}
								else{
									Dictionary temp = new Dictionary(idhit, identity);
									dict.put(key,temp);
								}
							}
						}

						// dict population complete
						rs.close();
					}
					dictbool = true;
					pstm.close();
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}

			Statement stm = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery(
					"SELECT id, " +
							"code,mean_psi, median_psi " +
					"FROM fh_clusters_info FOR UPDATE");
			int statusCounter = 0;
			while(rs.next()) {
				statusCounter++;
				if (statusCounter % 10 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter % 100 == 0) {
					System.out.write('\n');
					System.out.flush();
				}
				String code = rs.getString("code");

				//
				//get cluster members
				//
				BitSet sequenceIDs = new BitSet();

				pstm1.setString(1, code);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {

					int sequenceID = rs1.getInt("sequenceid");
					sequenceIDs.set(sequenceID);

				}
				rs1.close();
				rs1 = null;
				//
				//compute apsi
				//
				File rInputFile = File.createTempFile("rInput_", ".dat");
				PrintWriter pw1 = new PrintWriter(new FileWriter(rInputFile));
				pw1.println("SI");
				int pairs = 0;
				for(int sequenceID = sequenceIDs.nextSetBit(0); sequenceID>=0; sequenceID = sequenceIDs.nextSetBit(sequenceID+1)) {
					if (dict.containsKey(sequenceID)){
						Dictionary temp = dict.get(sequenceID);
						for (int j =0; j<=temp.protein.size()-1;j++){
							Integer hitID = temp.protein.get(j);	// hit
							float identity = temp.score.get(j);
							if(sequenceIDs.get(hitID)) {
								pairs++;
								pw1.println(identity);
							}
						}	
					}
					/*
					pstm2.setInt(1, sequenceID);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {

						int hitID = rs2.getInt("sequences_sequenceid_hit");
						float identity = 100* rs2.getFloat("identity");

						if(sequenceIDs.get(hitID)) {
							pairs++;

							pw1.println(identity);
						}
					}
					rs2.close();
					 */
				}

				pw1.close();

				float meanPSI = 0;
				float medianPSI = 0;

				if(pairs > 0) {					 

					File rScriptFile = File.createTempFile("rScript_", ".txt");
					File rOutputFile = File.createTempFile("rOutput_", ".dat");
					PrintWriter pw2 = new PrintWriter(new FileWriter(rScriptFile));

					pw2.println("sink(file=\""+rOutputFile.getAbsolutePath()+"\")");
					pw2.println("dat <- read.table(\""+rInputFile.getAbsolutePath()+"\", header=TRUE)");
					pw2.println("meanPSI <- mean(dat$SI)");
					pw2.println("print(meanPSI)");
					pw2.println("medianPSI <- median(dat$SI)");
					pw2.println("print(medianPSI)");
					pw2.println("sink()");

					pw2.close();

					//
					//run R
					//				
					String cmd = "R CMD BATCH "+rScriptFile.getAbsolutePath();
					Process p = Runtime.getRuntime().exec(cmd);
					BufferedReader br1 = new BufferedReader(new InputStreamReader(p.getInputStream()));
					String line1;
					while((line1=br1.readLine()) != null) {
						System.out.println(line1);			    				    	
					}		    
					p.waitFor();
					p.destroy();			
					br1.close();

					//
					//parse R output
					//
					BufferedReader br2 = new BufferedReader(new FileReader(rOutputFile));
					String line2;
					int line2Count = 0;
					while((line2 = br2.readLine()) != null) {

						line2Count++;

						if(line2Count == 1) {
							meanPSI = Float.parseFloat(line2.split("\\s+")[1]);	
						}
						else if(line2Count == 2) {
							medianPSI = Float.parseFloat(line2.split("\\s+")[1]);
						}					
					}

					rScriptFile.delete();
					rOutputFile.delete();
					File rScriptFile2 = new File(rScriptFile.getAbsoluteFile()+".Rout");
					rScriptFile2.delete();
				}

				rInputFile.delete();

				//rs.updateFloat("mean_psi", meanPSI);
				//rs.updateFloat("median_psi", medianPSI);
				//rs.updateRow();

			}
			rs.close();

			stm.close();
			pstm1.close();
			//pstm2.close();


		} catch(Exception e) {
			e.printStackTrace();
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


	/**
	 * Add mean and median pairwise sequence identity for each FH-cluster
	 * to table fh_clusters_nr_info.
	 */
	public void addAPSI2() {
		try {	

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid " +
							"FROM fh_clusters " +
					"WHERE code=? AND redundant=\"No\"");

			//			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
			//					"SELECT sequences_sequenceid_hit,identity FROM alignments WHERE sequences_sequenceid_query=?");

			PreparedStatement pstm = null;
			try{
				if(dictbool == false){

					int idx =1;
					for (int i=1; i<=17;i++){				
						pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial limit "+idx+","+10000000);
						//pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial");
						idx = i*10000000;
						System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
						ResultSet rs = pstm.executeQuery();
						while(rs.next()) {
							int idhit = rs.getInt("seqid_hit");
							int key = rs.getInt ("seqid_query");
							float identity = rs.getFloat("identity");

							if (dict.containsKey(key)){ // the key already doesnt exist...so that you dont delete the previous info of the key
								Dictionary temp = (Dictionary)dict.get(key);
								temp.set(idhit, identity);
								dict.put(key,temp);
							}
							else{
								Dictionary temp = new Dictionary(idhit, identity);
								dict.put(key,temp);
							}
						}

						// dict population complete
						rs.close();
					}
					dictbool = true;
					pstm.close();
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}

			Statement stm = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery(
					"SELECT id, " +
							"code,mean_psi, median_psi " +
					"FROM fh_clusters_nr_info FOR UPDATE");
			int statusCounter = 0;
			while(rs.next()) {

				statusCounter++;
				if (statusCounter % 10 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter % 100 == 0) {
					System.out.write('\n');
					System.out.flush();
				}

				String code = rs.getString("code");


				//
				//get cluster members
				//
				BitSet sequenceIDs = new BitSet();

				pstm1.setString(1, code);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {

					int sequenceID = rs1.getInt("sequenceid");
					sequenceIDs.set(sequenceID);

				}
				rs1.close();
				rs1 = null;


				//
				//compute apsi
				//
				File rInputFile = File.createTempFile("rInput_", ".dat");
				PrintWriter pw1 = new PrintWriter(new FileWriter(rInputFile));
				pw1.println("SI");
				int pairs = 0;

				for(int sequenceID = sequenceIDs.nextSetBit(0); sequenceID>=0; sequenceID = sequenceIDs.nextSetBit(sequenceID+1)) {
					if (dict.containsKey(sequenceID)){
						Dictionary temp = dict.get(sequenceID);
						for (int j =0; j<=temp.protein.size()-1;j++){
							Integer hitID = temp.protein.get(j);	// hit
							float identity = temp.score.get(j);
							if(sequenceIDs.get(hitID)) {
								pairs++;
								pw1.println(identity);
							}
						}	
					}
					/*
					pstm2.setInt(1, sequenceID);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {

						int hitID = rs2.getInt("sequences_sequenceid_hit");
						float identity = 100* rs2.getFloat("identity");

						if(sequenceIDs.get(hitID)) {
							pairs++;

							pw1.println(identity);
						}
					}
					rs2.close();
					 */
				}

				pw1.close();

				float meanPSI = 0;
				float medianPSI = 0;

				if(pairs > 0) {					 

					File rScriptFile = File.createTempFile("rScript_", ".txt");
					File rOutputFile = File.createTempFile("rOutput_", ".dat");
					PrintWriter pw2 = new PrintWriter(new FileWriter(rScriptFile));

					pw2.println("sink(file=\""+rOutputFile.getAbsolutePath()+"\")");
					pw2.println("dat <- read.table(\""+rInputFile.getAbsolutePath()+"\", header=TRUE)");
					pw2.println("meanPSI <- mean(dat$SI)");
					pw2.println("print(meanPSI)");
					pw2.println("medianPSI <- median(dat$SI)");
					pw2.println("print(medianPSI)");
					pw2.println("sink()");

					pw2.close();

					//
					//run R
					//				
					String cmd = "R CMD BATCH "+rScriptFile.getAbsolutePath();
					Process p = Runtime.getRuntime().exec(cmd);
					BufferedReader br1 = new BufferedReader(new InputStreamReader(p.getInputStream()));
					String line1;
					while((line1=br1.readLine()) != null) {
						System.out.println(line1);			    				    	
					}		    
					p.waitFor();
					p.destroy();			
					br1.close();

					//
					//parse R output
					//
					BufferedReader br2 = new BufferedReader(new FileReader(rOutputFile));
					String line2;
					int line2Count = 0;
					while((line2 = br2.readLine()) != null) {

						line2Count++;

						if(line2Count == 1) {
							meanPSI = Float.parseFloat(line2.split("\\s+")[1]);	
						}
						else if(line2Count == 2) {
							medianPSI = Float.parseFloat(line2.split("\\s+")[1]);
						}					
					}

					rScriptFile.delete();
					rOutputFile.delete();
					File rScriptFile2 = new File(rScriptFile.getAbsoluteFile()+".Rout");
					rScriptFile2.delete();
				}

				rInputFile.delete();

				rs.updateFloat("mean_psi", meanPSI);
				rs.updateFloat("median_psi", medianPSI);
				rs.updateRow();

			}
			rs.close();

			stm.close();
			pstm1.close();
			//pstm2.close();


		} catch(Exception e) {
			e.printStackTrace();
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


	public static void addMedianLength1() {
		try {

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid " +
							"FROM fh_clusters " +
					"WHERE code=?");

			//PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
			//		"SELECT length FROM sequences WHERE sequenceid=? LIMIT 1");
			PreparedStatement ptm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid,length FROM sequences2 ");
			Hashtable<Integer, Integer> SeqToLen = new Hashtable<Integer, Integer>();
			ResultSet rst1 = ptm2.executeQuery();
			while(rst1.next()){
				int seqid = rst1.getInt(1);
				int len = rst1.getInt(2);
				if(!SeqToLen.containsKey(seqid)){
					SeqToLen.put(seqid, len);
				}
			}


			Statement stm = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery(
					"SELECT id, " +
							"code,median_length " +
					"FROM fh_clusters_info FOR UPDATE");
			int statusCounter = 0;
			while(rs.next()) {

				statusCounter++;
				if (statusCounter % 1000 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter % 100000 == 0) {
					System.out.write('\n');
					System.out.flush();
				}

				String code = rs.getString("code");


				//
				//get cluster members
				//
				BitSet sequenceIDs = new BitSet();

				pstm1.setString(1, code);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {

					int sequenceID = rs1.getInt("sequenceid");
					sequenceIDs.set(sequenceID);

				}
				rs1.close();
				rs1 = null;


				//
				//extract sequence length information
				//
				File rInputFile = File.createTempFile(code, ".data");
				PrintWriter pw1 = new PrintWriter(new FileWriter(rInputFile));
				pw1.println("Length");

				for(int sequenceID = sequenceIDs.nextSetBit(0); sequenceID>=0; sequenceID = sequenceIDs.nextSetBit(sequenceID+1)) {
					if(SeqToLen.containsKey(sequenceID)){
						int length = SeqToLen.get(sequenceID);
						pw1.println(length);
					}
				}
				pw1.close();

				//
				//run R and calculate median
				//
				File rScriptFile = File.createTempFile("rScript_", ".txt");
				File rOutputFile = File.createTempFile("rOutput_", ".dat");
				PrintWriter pw2 = new PrintWriter(new FileWriter(rScriptFile));

				pw2.println("sink(file=\""+rOutputFile.getAbsolutePath()+"\")");
				pw2.println("dat <- read.table(\""+rInputFile.getAbsolutePath()+"\", header=TRUE)");
				pw2.println("medianLength <- median(dat$Length)");
				pw2.println("print(medianLength)");
				pw2.println("sink()");

				pw2.close();

				//
				//run R
				//				
				String cmd = "R CMD BATCH "+rScriptFile.getAbsolutePath();
				Process p = Runtime.getRuntime().exec(cmd);
				BufferedReader br1 = new BufferedReader(new InputStreamReader(p.getInputStream()));
				String line1;
				while((line1=br1.readLine()) != null) {
					System.out.println(line1);			    				    	
				}		    
				p.waitFor();
				p.destroy();			
				br1.close();

				//
				//parse R output
				//
				float medianLength = 0;
				BufferedReader br2 = new BufferedReader(new FileReader(rOutputFile));
				String line2;
				int line2Count = 0;
				while((line2 = br2.readLine()) != null) {

					line2Count++;

					if(line2Count == 1) {
						medianLength = Float.parseFloat(line2.split("\\s+")[1]);
					}									
				}

				rInputFile.delete();
				rScriptFile.delete();
				rOutputFile.delete();
				File rScriptFile2 = new File(rScriptFile.getAbsoluteFile()+".Rout");
				rScriptFile2.delete();


				//
				//update row
				//
				rs.updateFloat("median_length", medianLength);
				rs.updateRow();
			}
			rs.close();

			stm.close();
			pstm1.close();
			//pstm2.close();


		}catch(Exception e) {
			e.printStackTrace();
		} finally {
			/*
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			 */
		}
	}


	public static void addMedianLength2() {
		try {

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid " +
							"FROM fh_clusters " +
					"WHERE code=? AND redundant=\"No\"");

			//PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
			//		"SELECT length FROM sequences WHERE sequenceid=? LIMIT 1");
			PreparedStatement ptm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid,length FROM sequences2 ");
			Hashtable<Integer, Integer> SeqToLen = new Hashtable<Integer, Integer>();
			ResultSet rst1 = ptm2.executeQuery();
			while(rst1.next()){
				int seqid = rst1.getInt(1);
				int len = rst1.getInt(2);
				if(!SeqToLen.containsKey(seqid)){
					SeqToLen.put(seqid, len);
				}
			}


			Statement stm = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery(
					"SELECT id, " +
							"code,median_length " +
					"FROM fh_clusters_nr_info FOR UPDATE");
			int statusCounter = 0;
			while(rs.next()) {

				statusCounter++;
				if (statusCounter % 1000 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter % 100000 == 0) {
					System.out.write('\n');
					System.out.flush();
				}

				String code = rs.getString("code");


				//
				//get cluster members
				//
				BitSet sequenceIDs = new BitSet();

				pstm1.setString(1, code);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {

					int sequenceID = rs1.getInt("sequenceid");
					sequenceIDs.set(sequenceID);

				}
				rs1.close();
				rs1 = null;


				//
				//extract sequence length information
				//
				File rInputFile = File.createTempFile(code, ".data");
				PrintWriter pw1 = new PrintWriter(new FileWriter(rInputFile));
				pw1.println("Length");

				for(int sequenceID = sequenceIDs.nextSetBit(0); sequenceID>=0; sequenceID = sequenceIDs.nextSetBit(sequenceID+1)) {
					if(SeqToLen.containsKey(sequenceID)){
						int length = SeqToLen.get(sequenceID);
						pw1.println(length);
					}
				}
				pw1.close();

				//
				//run R and calculate median
				//
				File rScriptFile = File.createTempFile("rScript_", ".txt");
				File rOutputFile = File.createTempFile("rOutput_", ".dat");
				PrintWriter pw2 = new PrintWriter(new FileWriter(rScriptFile));

				pw2.println("sink(file=\""+rOutputFile.getAbsolutePath()+"\")");
				pw2.println("dat <- read.table(\""+rInputFile.getAbsolutePath()+"\", header=TRUE)");
				pw2.println("medianLength <- median(dat$Length)");
				pw2.println("print(medianLength)");
				pw2.println("sink()");

				pw2.close();

				//
				//run R
				//				
				String cmd = "R CMD BATCH "+rScriptFile.getAbsolutePath();
				Process p = Runtime.getRuntime().exec(cmd);
				BufferedReader br1 = new BufferedReader(new InputStreamReader(p.getInputStream()));
				String line1;
				while((line1=br1.readLine()) != null) {
					System.out.println(line1);			    				    	
				}		    
				p.waitFor();
				p.destroy();			
				br1.close();

				//
				//parse R output
				//
				float medianLength = 0;
				BufferedReader br2 = new BufferedReader(new FileReader(rOutputFile));
				String line2;
				int line2Count = 0;
				while((line2 = br2.readLine()) != null) {

					line2Count++;

					if(line2Count == 1) {
						medianLength = Float.parseFloat(line2.split("\\s+")[1]);
					}									
				}

				rInputFile.delete();
				rScriptFile.delete();
				rOutputFile.delete();
				File rScriptFile2 = new File(rScriptFile.getAbsoluteFile()+".Rout");
				rScriptFile2.delete();


				//
				//update row
				//
				rs.updateFloat("median_length", medianLength);
				rs.updateRow();
			}
			rs.close();

			stm.close();
			pstm1.close();
			//pstm2.close();


		}catch(Exception e) {
			e.printStackTrace();
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





	public static void main(String[] args) {

		try {

			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );

			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");

			FHClustering2_Postprocessing hc = new FHClustering2_Postprocessing();

			//
			// Homology Cleanup
			//			
			//hc.doHomologyCleanup();

			// **** DONE TILL HERE ****

			//
			// Cluster properties
			//
			//CreateDatabaseTables.create_table_fh_clusters_info();
			//hc.computeClusterProperties();
			//CreateDatabaseTables.create_table_fh_clusters_nr_info();
			//hc.computeClusterProperties_NR();

			//addSuperkingdomComposition1();
			//addSuperkingdomComposition2();


			//
			// TMS cores extraction
			//
			//CreateDatabaseTables.create_table_fh_clusters_tms_cores();
			//CreateDatabaseTables.create_table_fh_clusters_tms_blocks();
			//hc.extractTmsCores();
			//DBAdaptor.createIndex("camps4","fh_clusters_tms_cores",new String[]{"code"},"code");
			//DBAdaptor.createIndex("camps4","fh_clusters_tms_cores",new String[]{"code","sequenceid"},"cindex1");
			//DBAdaptor.createIndex("camps4","fh_clusters_tms_cores",new String[]{"code","tms_core_id"},"cindex2");
			//DBAdaptor.createIndex("camps4","fh_clusters_tms_blocks",new String[]{"code"},"code");
			//DBAdaptor.createIndex("camps4","fh_clusters_tms_blocks",new String[]{"code","sequenceid"},"cindex1");
			//DBAdaptor.createIndex("camps4","fh_clusters_tms_blocks",new String[]{"code","tms_block_id"},"cindex2");


			//
			// Clusters cross db
			//
			//System.out.print("\nRunning cross db stuff\n");
			//			CreateDatabaseTables.create_table_fh_clusters_cross_db();
			//		hc.crossDB();
			//	DBAdaptor.createIndex("camps4","fh_clusters_cross_db",new String[]{"code"},"code");
			//DBAdaptor.createIndex("camps4","fh_clusters_cross_db",new String[]{"code","db"},"cindex1");


			//
			//Association to PDB structures
			//
			System.out.print("\nRunning structures stuff\n");
			//CreateDatabaseTables.create_table_fh_clusters_structures();
			//hc.structures();
			//DBAdaptor.createIndex("camps4","fh_clusters_structures",new String[]{"pdbid"},"pdbid");
			//DBAdaptor.createIndex("camps4","fh_clusters_structures",new String[]{"code"},"code");
			
			CreateDatabaseTables.create_table_fh_clusters_structures0();
			hc.structures0();
			DBAdaptor.createIndex("camps4","fh_clusters_structures0",new String[]{"pdbid"},"pdbid");
			DBAdaptor.createIndex("camps4","fh_clusters_structures0",new String[]{"code"},"code");


			//
			// Add number of cores to info tables
			//
			//System.out.print("\nAdding cores to info tables\n");
			//hc.addNumCores1();
			//hc.addNumCores2();


			//
			// Add mean,median PSI into info tables
			//
			//hc.addAPSI1();
			//hc.addAPSI2();


			//
			// Add median length information
			//
			//System.out.print("\nAdding MedianLen1\n");
			//addMedianLength1();
			//System.out.print("\nAdding MedianLen2\n");
			//addMedianLength2();

			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");


		} catch(Exception e) {
			e.printStackTrace();
		}	

	}

}
