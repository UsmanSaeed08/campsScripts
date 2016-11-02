/*
 * ClusterPropertiesMCL_NR
 * 
 * Version 3.0
 * 
 * 2016-May
 * 
 * Same as before but with threading to speed up the process -- made to reRun CAMPS
 * 
 * Differences to version 1:
 * - functional homogeneity is calculated using Schlicker score
 * - taxonomic information is inserted by numbers not by category
 * 
 */

package reRUN_CAMPS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.Date;
import java.util.Hashtable;

import properties.ConfigFile;
import utils.DBAdaptor;



/**
 * Computes the following cluster properties for all initial non-redundant 
 * clusters in CAMPS (stored in table 'clusters_mcl'):
 * 
 * sequences			      - total number of cluster sequences
 * average_length			  - average sequence length of all cluster members
 * tms_range                  - TMS range of cluster members
 * structural_homogeneity     - structural homogeneity of cluster members
 * sequences_with_GOA         - number of cluster sequences that have GO annotation
 * functional_homogeneity     - functional homogeneity according to Schlicker score
 * proteins                   - total number of cluster proteins
 * proteins_archaea           - number of cluster proteins from Archaea
 * proteins_bacteria          - number of cluster proteins from Bacteria
 * proteins_eukaryota         - number of cluster proteins from Eukaryota
 * proteins_viruses           - number of cluster proteins from Viruses
 * proteins_misc			  - number of other cluster proteins (unclassified etc)
 */
public class ClusterPropertiesMCL_NR2 extends Thread {
	
	private Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	//private static final Connection SIMAP_CONNECTION = DBAdaptor.getConnection("simap");
	
	private final int BLOB_SIZE = 15000;	// the size of cluster members as a limit threshold for calc
	
	//private static final File CLUSTER_LIST = new File("/home/users/saeed/workspace/data/mcl_clusters_camps4.txt");
	
	private final File CLUSTER_LIST = new File("/localscratch/CAMPS/clusterList/mcl_clusters2_camps4.txt");
	//private static final File CLUSTER_LIST = new File("F:/mcl_clusters_camps4.txt");
	
	
	private int start;
	private int end;
	private int threadid;
	private int len;
	
	public ClusterPropertiesMCL_NR2(int start, int length,int t) {
		this.start = start;		
		this.end = (start+length)-1;
		this.threadid = t;
		this.len = length;
	}
	
		
	
	public void run() {
		
		try {
			int batchSize = 300;
			int batchCounter = 0;
			//
			//get sequence lengths for all sequences
			//
			System.out.println("\n\t[INFO] Get sequence lengths" + this.threadid);
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
			System.out.println("\t...DONE"+ this.threadid);
			
			//
			//get number of TMS for all sequences
			//
			System.out.println("\n\t[INFO] Get number of TMS" + this.threadid);
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
			System.out.println("\t...DONE" + this.threadid);
			
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
					" FROM clusters_mcl2" +
					" WHERE cluster_id=? AND cluster_threshold=? AND redundant=\"No\"");	
			
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_mcl_nr_info2" +
					" (cluster_id, cluster_threshold," +
					" sequences, average_length," +
					" tms_range, structural_homogeneity," +
					" sequences_with_GOA, functional_homogeneity," +
					" proteins, proteins_archaea, proteins_bacteria, proteins_eukaryota, proteins_viruses, proteins_misc) " +
					"VALUES " +
					"(?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			
			
			int counter = 0;
			BufferedReader br = new BufferedReader(new FileReader(CLUSTER_LIST));
			String line;
			int lineCount = 0;
			boolean start = false;
			while((line = br.readLine()) != null) {
				
				lineCount++;
				
				if(lineCount == this.start) {
					start = true;
				}
				
				if(start) {
					
					counter++;
					/*
					if (counter % 100 == 0) {
						System.out.write('.');
						System.out.flush();
					}
					if (counter % 10000 == 0) {					
						System.out.write('\n');
						System.out.flush();
					}
					*/
					String cluster = line.trim();
					int clusterID = Integer.parseInt(cluster.split("\t")[0]);
					float clusterThreshold = Float.parseFloat(cluster.split("\t")[1]);
									
					//get distinct sequences for each cluster
					BitSet clusterMembers = new BitSet();
					pstm1.setInt(1, clusterID);
					pstm1.setFloat(2, clusterThreshold);
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {
						int sequenceid = rs1.getInt("sequenceid");
						clusterMembers.set(sequenceid);
					}
					rs1.close();
					rs1 = null;
					if(clusterMembers.cardinality() == 0){ // because only nr sequences are processed
												// if a cluster has all the members which are redundant then this bitset is empty 
												// and gives an arithmetic exception divided by zero.. 
						continue;
					}
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
					int averageLength = 0;
					try{
						averageLength = (int) ((cumulativeLength/numberSequences) + 0.5);
					}
					catch(Exception e){
						e.printStackTrace();
						System.out.println();
						System.out.println("Cumulative Length: "+ cumulativeLength);
						System.out.println("No of Seq: "+ numberSequences);
						System.out.println("ClusId: "+ clusterID);
						System.out.println("Thresh: "+ clusterThreshold);
						System.exit(0);
					}
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
					// removing the part of functional homogenity as there is problem in calculating it. 
					// So now would do it later on. For now enter -1 for all
					// 1. number of Seqs With GO Annotation
					// 2. functional homogenity
					
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
					pstm2.setInt(1, clusterID);
					pstm2.setFloat(2, clusterThreshold);
					pstm2.setInt(3, numberSequences);
					pstm2.setInt(4, averageLength);
					pstm2.setString(5, tmsRange);
					pstm2.setFloat(6, structuralHomogeneity);
					//pstm2.setInt(7, numSeqsWithGOA);
					//pstm2.setFloat(8, functionalHomogeneity);
					pstm2.setInt(7, -1);
					pstm2.setFloat(8, -1f);
					pstm2.setInt(9, numberProteins);
					pstm2.setInt(10, numberProteinsArchaea);
					pstm2.setInt(11, numberProteinsBacteria);
					pstm2.setInt(12, numberProteinsEukaryota);
					pstm2.setInt(13, numberProteinsViruses);
					pstm2.setInt(14, numberProteinsMisc);
					
					pstm2.addBatch();
					
					if(batchCounter % batchSize == 0) {								
						pstm2.executeBatch();
						pstm2.clearBatch();
						System.out.println("Processed: "+batchCounter+ " of "+this.len +" -- "+this.threadid);
					}
				}
			
				if(lineCount == this.end) {
					break;  //stop process
				}
			}
			br.close();
			pstm1.close();
			pstm1 = null;	
			pstm2.executeBatch();		//add remaining entries
			pstm2.clearBatch();
			pstm2.close();
			pstm2 = null;	
						
			
		}
		catch(Exception e) {
			System.err.println("Exception in ClusterPropertiesMCL_NR.run(): " + this.threadid+"\n"+ e.getMessage());
			e.printStackTrace();
		
		} finally {
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}/*	
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
	private Hashtable<String,Integer> getSuperkingdomDistribution(BitSet clusterMembers) {
		
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
		

	private Hashtable<String,String> getFunctionalHomogeneity(BitSet clusterMembers) {
		return null; // -- as not calculating functional homogeneity here!!!
		/*
		Hashtable<String,String> result = null;
		try {
			
			result = new Hashtable<String,String>();
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT md5 FROM sequences2 WHERE sequenceid=?");
			PreparedStatement pstm2 = SIMAP_CONNECTION.prepareStatement("SELECT sequenceid FROM sequence WHERE md5=?");
			
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
		*/
	}
	
	
	/*
	 * Add superkingdom composition to each MCL cluster.
	 */
	/*
	public void addSuperkingdomComposition() {
		try {			
			
			Statement stm = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery(
					"SELECT id, " +
					"proteins_archaea, proteins_bacteria, proteins_eukaryota, proteins_viruses, proteins_misc, superkingdom_composition " +
					"FROM clusters_mcl_nr_info FOR UPDATE");
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
			
			stm.close();
									
			
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
	*/
	
	/*
	 * Add number of cores to each MCL cluster.
	 
	public void addNumCores() {
		try {	
			
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT max(tms_core_id) " +
					"FROM tms_cores " +
					"WHERE cluster_id=? AND cluster_threshold=?");
			
			//added 2011-Mar-28
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT max(tms_core_id) " +
					"FROM md_clusters_tms_cores " +
					"WHERE cluster_id=? AND cluster_threshold=?");
			
			Statement stm = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery(
					"SELECT id, " +
					"cluster_id,cluster_threshold,cores " +
					"FROM clusters_mcl_nr_info FOR UPDATE");
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
				
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");
				
				int numCores = 0;
				
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rsx = pstm1.executeQuery();
				while(rsx.next()) {
					numCores = rsx.getInt("max(tms_core_id)");
				}
				rsx.close();
				
				pstm2.setInt(1, clusterID);
				pstm2.setFloat(2, clusterThreshold);
				ResultSet rsy = pstm2.executeQuery();
				while(rsy.next()) {
					Object tmp = rsy.getObject("max(tms_core_id)");
					
					//
					//this is important! 
					//if md_clusters_tms_cores has no core informations for a
					//given cluster, then rs.getInt() (which would be null)
					//would return 0, since it is being cast to int
					//=> cores for SC-cluster cannot be found in md_clusters_tms_cores
					// thus pstm2 would always give 0 and thus real value (found
					//with pstm1) would be overwritten
					//
					if(tmp != null) {
						numCores = Integer.parseInt(String.valueOf(tmp));
					}
				}
				rsy.close();
				
				rs.updateInt("cores", numCores);
				rs.updateRow();
				
			}
			rs.close();
			
			stm.close();
			pstm1.close();
			pstm2.close();
									
			
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
	
	
	public void addAPSI() {
		try {	
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequences_sequenceid " +
					"FROM clusters_mcl " +
					"WHERE cluster_id=? AND cluster_threshold=? AND redundant=\"No\"");
			
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequences_sequenceid_hit,identity FROM alignments WHERE sequences_sequenceid_query=?");
			
//			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
//			"SELECT * FROM cp_clusters WHERE cluster_id=? AND cluster_threshold=? AND type=\"sc_cluster\"");
	
//			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
//					"SELECT * FROM cp_clusters WHERE cluster_id=? AND cluster_threshold=? AND type=\"md_cluster\"");
			
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"SELECT * FROM cp_clusters WHERE cluster_id=? AND cluster_threshold=? AND type in (\"sc_cluster\",\"md_cluster\")");
			
			Statement stm = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery(
					"SELECT id, " +
					"cluster_id,cluster_threshold,mean_psi, median_psi " +
					"FROM clusters_mcl_nr_info FOR UPDATE");
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
				
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");
				
				//
				//Since computation of APSI takes a lot of time, first start
				//computing APSI only for those clusters being SC-clusters or MD-clusters!
				//
				boolean isValidCluster = false;
				pstm3.setInt(1, clusterID);
				pstm3.setFloat(2, clusterThreshold);
				ResultSet rs3 = pstm3.executeQuery();
				while(rs3.next()) {
					isValidCluster = true;
				}
				rs3.close();
				
				if(!isValidCluster) {
					continue;
				}
				
				System.err.println("\tIn progress: "+clusterID+"#"+clusterThreshold);
				
				
				//
				//get cluster members
				//
				BitSet sequenceIDs = new BitSet();
				
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					
					int sequenceID = rs1.getInt("sequences_sequenceid");
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
			pstm2.close();
									
			
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
	
	
	public static void addMedianLength() {
		try {
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequences_sequenceid " +
					"FROM clusters_mcl " +
			        "WHERE cluster_id=? AND cluster_threshold=? AND redundant=\"No\"");
			
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT length FROM sequences WHERE sequenceid=? LIMIT 1");
			
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"SELECT * FROM cp_clusters " +
					"WHERE cluster_id=? AND cluster_threshold=? " +
					"AND type in (\"sc_cluster\",\"md_cluster\")");
			
			
			Statement stm = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery(
					"SELECT id, " +
					"cluster_id,cluster_threshold,median_length " +
					"FROM clusters_mcl_nr_info FOR UPDATE");
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
				
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");
				
				
				//
				//Since computation of median length takes a lot of time, first start
				//computing it only for those clusters being SC-clusters or MD-clusters!
				//
				boolean isValidCluster = false;
				pstm3.setInt(1, clusterID);
				pstm3.setFloat(2, clusterThreshold);
				ResultSet rs3 = pstm3.executeQuery();
				while(rs3.next()) {
					isValidCluster = true;
				}
				rs3.close();
				
				if(!isValidCluster) {
					continue;
				}
				
				System.err.println("\tIn progress: "+clusterID+"#"+clusterThreshold);
				
				
				//
				//get cluster members
				//
				BitSet sequenceIDs = new BitSet();
				
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					
					int sequenceID = rs1.getInt("sequences_sequenceid");
					sequenceIDs.set(sequenceID);
					
				}
				rs1.close();
				rs1 = null;
				
				
				//
				//extract sequence length information
				//
				File rInputFile = File.createTempFile(clusterID+"_"+clusterThreshold, ".data");
				PrintWriter pw1 = new PrintWriter(new FileWriter(rInputFile));
				pw1.println("Length");
				
				for(int sequenceID = sequenceIDs.nextSetBit(0); sequenceID>=0; sequenceID = sequenceIDs.nextSetBit(sequenceID+1)) {
					
					pstm2.setInt(1, sequenceID);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						
						int length = rs2.getInt("length");
						pw1.println(length);
					}
					rs2.close();
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
				//File rScriptFile2 = new File(rScriptFile.getAbsoluteFile()+".Rout");
				File rScriptFile2 = new File("/home/users/sneumann/workspace/CAMPS3/"+rScriptFile.getName()+".Rout");
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
			pstm2.close();
			pstm3.close();
			
			
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
	*/
	
	
}

