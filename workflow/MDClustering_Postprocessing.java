/*
 * MDClustering_Postprocessing
 * 
 * Version 1.0
 * 
 * 2011-03-28
 * 
 */

package workflow;

import general.CreateDatabaseTables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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
import java.util.Hashtable;

import properties.ConfigFile;

import utils.DBAdaptor;
import utils.FastaReader;
import utils.FastaReader.FastaEntry;



/**
 * 
 * @author sneumann
 * 
 * 
 * Computes core regions for all MD-cluster smaller than 15 members. 
 * 
 * Background:
 * Right now, all cores contained in 'tms_cores' correspond to clusters
 * with at least 15 members. This was necessary for the construction of SC-cluster.
 * However, MD-clusters must not have the same minimal size than SC-clusters.
 * As in CAMPS1.0 the minimal size for MD-clusters is 8. Thus, no cores existed
 * so far for MD-clusters having 8-15 members. 
 * This 'gap' is filled using this script. 
 *
 */
public class MDClustering_Postprocessing {
	
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	private static boolean dictbool = false;
	static Hashtable<Integer, Dictionary> dict = new Hashtable<Integer, Dictionary>();
	
	private static final int BLOB_SIZE = 10000;
	
	
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
	
	
	
	
	
	/**
	 * Extract tms cores for MD-clusters that do not have cores yet (see above).
	 */
	public void extractTmsCores() {		
		try {
			
			int batchSize = 50;
			int batchCounter = 0;
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT code FROM cp_clusters WHERE cluster_id=? AND cluster_threshold=? AND type=\"md_cluster\"");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT * FROM tms_cores WHERE cluster_id=? AND cluster_threshold=?");
			
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl WHERE cluster_id=? and cluster_threshold=? AND redundant=\"No\"");
			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT begin,end FROM tms WHERE sequenceid=? order by begin");
			PreparedStatement pstm5 = CAMPS_CONNECTION.prepareStatement("SELECT length FROM sequences2 WHERE sequenceid=?");
			PreparedStatement pstm6 = CAMPS_CONNECTION.prepareStatement("SELECT sequence FROM sequences2 WHERE sequenceid=?");
						
			PreparedStatement pstm7 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO md_clusters_tms_cores " +
					"(cluster_id,cluster_threshold,tms_core_id,sequenceid," +
					"sequence_aligned,begin_aligned,end_aligned,length_aligned," +
					"sequence_unaligned,begin_unaligned,end_unaligned,length_unaligned) " +
					"VALUES " +
					"(?,?,?,?,?,?,?,?,?,?,?,?)");
			PreparedStatement pstm8 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO md_clusters_tms_blocks " +
					"(cluster_id,cluster_threshold,tms_block_id,sequenceid," +
					"sequence_aligned,begin_aligned,end_aligned,length_aligned," +
					"sequence_unaligned,begin_unaligned,end_unaligned,length_unaligned) " +
					"VALUES " +
					"(?,?,?,?,?,?,?,?,?,?,?,?)");
			
						
			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold, tms_range FROM clusters_mcl_nr_info");	
			int counter = 0;
			while(rs.next()) {
				counter++;
				int clusterID = rs.getInt("cluster_id");
				int clusterThreshold = (int) rs.getFloat("cluster_threshold");
				
				//
				//check if cluster is MD-cluster
				//
				boolean isMDCluster = false;
				pstm1.setInt(1, clusterID);		
				pstm1.setFloat(2, clusterThreshold);					
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					isMDCluster = true;				
				}
				rs1.close();
				rs1 = null;
				
				if(!isMDCluster) {
					continue;
				}
				
				
				//
				//check if cores already available
				//
				boolean coresAvailable = false;
				pstm2.setInt(1, clusterID);		
				pstm2.setFloat(2, clusterThreshold);					
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()) {
					coresAvailable = true;				
				}
				rs2.close();
				rs2 = null;
				
				if(coresAvailable) {
					continue;
				}
									
				
				//get distinct sequences for each cluster
				System.out.println("\n\n\t[INFO] Get cluster members for: "+clusterID+"\t"+clusterThreshold+"  ("+counter+")");
				BitSet sequenceIDs = new BitSet();
				pstm3.setInt(1, clusterID);		
				pstm3.setFloat(2, clusterThreshold);					
				ResultSet rs3 = pstm3.executeQuery();
				while(rs3.next()) {
					int sequenceid = rs3.getInt("sequenceid");					
					sequenceIDs.set(sequenceid);					
				}
				rs3.close();
				rs3 = null;
				
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
					pstm4.setInt(1, sequenceid);
					ResultSet rs4 = pstm4.executeQuery();
					while(rs4.next()) {
						numTms++;						
						int tmsBegin = rs4.getInt("begin");
						int tmsEnd = rs4.getInt("end");						
						tmsPredictions.add(tmsBegin + "-" + tmsEnd);
					}
					rs4.close();
					rs4 = null;
					
					//get length
					int length = -1;
					pstm5.setInt(1, sequenceid);
					ResultSet rs5 = pstm5.executeQuery();
					while(rs5.next()) {
						length = rs5.getInt("length");
					}
					rs5.close();
					
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
				File alignmentInfileTMP = File.createTempFile("cluster"+clusterID+"_E"+clusterThreshold, ".fasta");		
				PrintWriter pw = new PrintWriter(new FileWriter(alignmentInfileTMP));
			    for(Integer candidate: candidates) {
			    	String sequence = null;
			    	pstm6.setInt(1, candidate.intValue());
			    	ResultSet rs6 = pstm6.executeQuery();
			    	while(rs6.next()) {
			    		sequence = rs6.getString("sequence").toUpperCase();
			    	}
			    	rs6.close();
			    	rs6 = null;
			    	
			    	pw.println(">"+candidate.intValue()+"\n"+sequence);
			    }
			    pw.close();
			    
			    //
			    //run alignment program
			    //
			    File alignmentOutfileTMP = File.createTempFile("cluster"+clusterID+"_E"+clusterThreshold, ".ali");					    
			    ConfigFile cf = new ConfigFile("/home/users/saeed/workspace/CAMPS3/config/config.xml");
			    String alignmentDir = cf.getProperty("apps:clustalw:install-dir");
			    String cmd = alignmentDir+" -INFILE=" + alignmentInfileTMP.getAbsolutePath() + " -OUTFILE=" + alignmentOutfileTMP.getAbsolutePath() + " -OUTPUT=FASTA";
			    		    
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
					System.err.println("\n\n"+clusterID+"\t"+clusterThreshold+"\tEmpty alignment file -> TMH cores cannot be determined!");
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
						/*
						int succCoreStart = tmsCoreID2position.get(Integer.valueOf(tmsCoreID+1))[0];
						int interspaceSizeRight = ((succCoreStart-1) - (coreEnd+1)) + 1;
						
						if(interspaceSizeRight >= 2*CORE_EXTENSION) {
							blockEnd = coreEnd + CORE_EXTENSION;
						}
						else {
							blockEnd = coreEnd + (interspaceSizeRight/2);
						}
						*/
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
						
						pstm7.setInt(1, clusterID);
						pstm7.setFloat(2, clusterThreshold);
						pstm7.setInt(3, tmsCoreID);
						pstm7.setInt(4, candidate.intValue());						
						pstm7.setString(5, coreSequenceAligned);
						pstm7.setInt(6, coreStart);
						pstm7.setInt(7, coreEnd);	
						pstm7.setInt(8, coreLengthAligned);							
						pstm7.setString(9, coreSequenceUnaligned);
						pstm7.setInt(10, coreStartInInitialSeq);
						pstm7.setInt(11, coreEndInInitialSeq);	
						pstm7.setInt(12, coreLengthUnaligned);							
						pstm7.addBatch();
						
						pstm8.setInt(1, clusterID);
						pstm8.setFloat(2, clusterThreshold);
						pstm8.setInt(3, tmsCoreID);
						pstm8.setInt(4, candidate.intValue());						
						pstm8.setString(5, blockSequenceAligned);
						pstm8.setInt(6, blockStart);
						pstm8.setInt(7, blockEnd);	
						pstm8.setInt(8, blockLengthAligned);							
						pstm8.setString(9, blockSequenceUnaligned);
						pstm8.setInt(10, blockStartInInitialSeq);
						pstm8.setInt(11, blockEndInInitialSeq);	
						pstm8.setInt(12, blockLengthUnaligned);						
						pstm8.addBatch();
						
						if(batchCounter % batchSize == 0) {								
							pstm7.executeBatch();
							pstm7.clearBatch();
							
							pstm8.executeBatch();
							pstm8.clearBatch();
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
			pstm4.close();
			pstm4 = null;
			pstm5.close();
			pstm5 = null;
			pstm6.close();
			pstm6 = null;
			pstm7.executeBatch();		//insert remaining entries
			pstm7.clearBatch();
			pstm7.close();
			pstm7 = null;
			pstm8.executeBatch();		//insert remaining entries
			pstm8.clearBatch();
			pstm8.close();
			pstm8 = null;
			
		} catch(Exception e) {
			System.err.println("Exception in MDClustering_Postprocessing.extracTmsCores(): " +e.getMessage());
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
	private static float[][] fillMatrix(ArrayList<Integer> candidates) {
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
				
	
	public static void main(String[] args) {
		
		try {
			
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			
			MDClustering_Postprocessing md = new MDClustering_Postprocessing();
			
			
			//
			// TMS cores extraction
			//
			
			CreateDatabaseTables.create_table_md_clusters_tms_cores();
			CreateDatabaseTables.create_table_md_clusters_tms_blocks();
			md.extractTmsCores();
			DBAdaptor.createIndex("camps4","md_clusters_tms_cores",new String[]{"cluster_id,cluster_threshold"},"cindex1");
			DBAdaptor.createIndex("camps4","md_clusters_tms_cores",new String[]{"cluster_id,cluster_threshold","sequenceid"},"cindex2");
			DBAdaptor.createIndex("camps4","md_clusters_tms_cores",new String[]{"cluster_id,cluster_threshold","tms_core_id"},"cindex3");
			DBAdaptor.createIndex("camps4","md_clusters_tms_blocks",new String[]{"cluster_id,cluster_threshold"},"cindex1");
			DBAdaptor.createIndex("camps4","md_clusters_tms_blocks",new String[]{"cluster_id,cluster_threshold","sequenceid"},"cindex2");
			DBAdaptor.createIndex("camps4","md_clusters_tms_blocks",new String[]{"cluster_id,cluster_threshold","tms_block_id"},"cindex3");
						
						
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}	
				
	}

}
