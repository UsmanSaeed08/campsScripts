package workflow;

/*
 * MCLClustering
 * 
 * Version 1.0
 * 
 * 2009-10-23
 * 
 */

//package workflow.initialClustering;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
//import java.util.ArrayList;
import java.util.BitSet;
import java.util.zip.GZIPOutputStream;

import utils.DBAdaptor;


public class MCLClustering {
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps3");
	
	private static final String MCLOUT_SUFFIX = ".clusters";
	private static final String MCLIN_MATRIX_SUFFIX = ".matrix.gz";
	private static final String MCLIN_TAB_SUFFIX = ".tab";
	
	private static final DecimalFormat SIMILARITY_FORMAT = new DecimalFormat("0.000");
	
	private static final int[] THRESHOLDS = new int[]{
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
		100		
		};
	
		
	private String mclOutdir;
	
	private int currentThreshold;
	
	private String outdir;
	
	/*
	 * @param
	 * 
	 * mclOutdir - path to directory that contains MCL clustering results from current evalue thresholds
	 * currentThreshold - evalue threshold that was used in last MCL clustering
	 * outdir - output directory where all matrices and tab files and MCL clustering results
	 * will be written for next evalue threshold run
	 */
	public MCLClustering(String mclOutdir, int currentThreshold, String outdir) {
		
		this.mclOutdir = mclOutdir;
		this.currentThreshold = currentThreshold;
		this.outdir = outdir;
		
	}
	
	
	public void run() {
		try {		
			
			File[] files = new File(mclOutdir).listFiles();
			
			int clusterID = 0;
			
			for(File file: files) {	//loop for all the files; for each file in files do below
				
				if(file.getName().endsWith(MCLOUT_SUFFIX)) {
					
					System.out.println("\nIn progress: " +file.getAbsolutePath());
					
					//read clusters
					BufferedReader br = new BufferedReader(new FileReader(file));
					String line;
					while((line = br.readLine()) != null) {
						
						String[] members = line.split("\\t");
						//ignore singletons
						if(members.length > 1) {
												
							BitSet cluster = new BitSet();
								
							for(String member: members) {
								int sequenceid = Integer.valueOf(member).intValue();
									
								cluster.set(sequenceid);
							}	
							
							//write to db
							writeClusteringResultToDB(cluster, clusterID);
							
							//write new matrices for next run
							if(currentThreshold < THRESHOLDS[THRESHOLDS.length-1]) {					
								int nextThreshold = nextThreshold();
								generateMatrices(cluster, clusterID, nextThreshold);
							}
												
							//set cluster id for next cluster output
							clusterID++;
							
							cluster = null;
						}				
					}
					br.close();					
					
				}
				
			}
			
			//write shell script for batch queue
			if(currentThreshold < THRESHOLDS[THRESHOLDS.length-1]) {	
				int nextThreshold = nextThreshold();
				writeShellScript(this.outdir+"mcl_E"+nextThreshold+".sh");
			}
						
			
		}catch(Exception e) {
			System.err.println("Exception in MCLClustering.run(): " +e.getMessage());
			e.printStackTrace();
		
		}finally{
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}
	
	
	private int nextThreshold() {
		
		int index = -1;
		for(int i=0; i<THRESHOLDS.length; i++) {
			
			int threshold = THRESHOLDS[i];
			
			if(threshold == this.currentThreshold) {
				index = i;
				break;
			}
			
		}
		
		int nextThreshold = THRESHOLDS[index+1];
		
		return nextThreshold;
	}
	
	
	/*
	 * Reads the MCL cluster from the specified BitSet and writes it
	 * into the table 'clusters';
	 */
	private void writeClusteringResultToDB(BitSet cluster, int clusterID) {
		
		try {
			
			int batchSize = 50;	
			
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_mcl " +
					"(cluster_id, cluster_threshold, sequences_sequenceid) " +
					"VALUES " +
					"(?,?,?)");
			
			
			int batchCounter = 0;			
					
			for(int id = cluster.nextSetBit(0); id>=0; id = cluster.nextSetBit(id+1)) {
														
				batchCounter++;
									
				pstm.setInt(1, clusterID);
				pstm.setFloat(2, this.currentThreshold);
				pstm.setInt(3, id);
																
				pstm.addBatch();
								
				if(batchCounter % batchSize == 0) {								
					pstm.executeBatch();
					pstm.clearBatch();
				}			
				
			}			
					
			pstm.executeBatch();	//insert remaining entries
			pstm.clearBatch();			
			
			pstm.close();
			pstm = null;		
				
		}
		catch(Exception e) {
			System.err.println("Exception in MCLClustering.writeClusteringResultsToDB(): " +e.getMessage());
			e.printStackTrace();
		}		
	}
	
	
	/*
	 * Generates new matrices for the next MCL run with a more stringent evalue threshold.
	 */
	private void generateMatrices(BitSet cluster, int clusterID, int nextThreshold) {
		try {
			
			double evalueThreshold = Double.parseDouble("1e-"+nextThreshold);
			
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("SELECT sequences_sequenceid_query,sequences_sequenceid_hit,evalue FROM alignments WHERE sequences_sequenceid_query=?");
			
			String outfileprefix = "E"+this.currentThreshold+"_"+clusterID+"_"+nextThreshold;
								
			//
			//set internal indices for sequences
			//
			int[] sequenceid2InternalIndex = new int[cluster.length()+1];
				
			int index =0;
			for (int sequenceid=cluster.nextSetBit(0); sequenceid >=0; sequenceid=cluster.nextSetBit(sequenceid+1)) {
				sequenceid2InternalIndex[sequenceid]=index;
				index ++;
			}
				
			//
			//Write tab file (makes MCL cluster output more readable)
			//
			try {
				BufferedWriter bw1 = new BufferedWriter(new FileWriter(this.outdir+outfileprefix+MCLIN_TAB_SUFFIX));
				for (int sequenceid=cluster.nextSetBit(0); sequenceid >=0; sequenceid=cluster.nextSetBit(sequenceid+1)) {
					bw1.write(sequenceid2InternalIndex[sequenceid]+" "+sequenceid+"\n");
				}
				bw1.close();
			} catch (IOException e) {
				System.err.println("Error when writing tab file "+e.getMessage());
			}
				
				
				
			//
			//Write matrix file
			//
			BufferedWriter bw2 = new BufferedWriter(new OutputStreamWriter(
					new GZIPOutputStream(new FileOutputStream(this.outdir+outfileprefix+MCLIN_MATRIX_SUFFIX))));
				
			// Write header
			bw2.write("(mclheader\n");
			bw2.write("mcltype matrix\n");
			bw2.write("dimensions "+cluster.cardinality()+"x"+cluster.cardinality()+"\n");
			bw2.write(")\n");
			bw2.write("(mclmatrix\n");
			bw2.write("begin\n");
				
			for(int id = cluster.nextSetBit(0); id>=0; id = cluster.nextSetBit(id+1)) {
					
				StringBuffer hitvector = new StringBuffer();
				int queryIdInternal = sequenceid2InternalIndex[id];
				hitvector.append(queryIdInternal);
					
				int countHits = 0;
				pstm.setInt(1, id);
				ResultSet rs = pstm.executeQuery();
				while(rs.next()) {
												
					int hitId = rs.getInt("sequences_sequenceid_hit");
					double evalue = rs.getDouble("evalue");						
						
					if(cluster.get(hitId)) {
							
						int hitIdInternal = sequenceid2InternalIndex[hitId];
							
						if(evalue <= evalueThreshold) {
								
							countHits++;
							hitvector.append(" "+hitIdInternal+":"+SIMILARITY_FORMAT.format(-Math.log10(evalue)));
						}							
					}						
				}
				rs.close();
					
				if(countHits>0) {
						
					synchronized(bw2) {
						try {
							bw2.write(hitvector.toString()+" $\n");
						} catch (IOException e) {
							System.err.println("Error when writing hit for sequenceid "+id+" to hitbuffer.");
						}
					}
					hitvector = null;
				}
			}
				
			// Write footer
			bw2.write(")\n");
				
			bw2.flush();
			bw2.close();		
			
			
		}catch(Exception e) {
			System.err.println("Exception in MCLClustering.createMatrices(): " +e.getMessage());
			e.printStackTrace();
		}
	}
	
	
//	/*
//	 * Reads clusters from specified MCL cluster file and returns it
//	 * as a list of BitSets (one BitSet corresponds to one cluster).
//	 * 
//	 * Clusters consisting of only one sequence (=singletons) are ignored
//	 * and not returned.
//	 */
//	private ArrayList<BitSet> readClustersFromFile(File mclClustersFile) {
//		
//		ArrayList<BitSet> clusters = null;
//		
//		try {
//			
//			clusters = new ArrayList<BitSet>();
//									
//			BufferedReader br = new BufferedReader(new FileReader(mclClustersFile));
//			String line;
//			while((line = br.readLine()) != null) {
//				
//				String[] members = line.split("\\t");
//				//ignore singletons
//				if(members.length > 1) {
//										
//					BitSet ids = new BitSet();
//						
//					for(String member: members) {
//						int sequenceid = Integer.valueOf(member).intValue();
//							
//						ids.set(sequenceid);
//					}	
//					
//					clusters.add(ids);
//				}				
//			}
//			br.close();			
//			
//		}catch(Exception e) {
//			System.err.println("Exception in MCLClustering.readClustersFromFile(): " +e.getMessage());
//			e.printStackTrace();
//		}
//		
//		return clusters;
//	}
	
	
	/*
	 * Writes all MCL jobs for the next evalue threshold to the specified file.
	 */
	private void writeShellScript(String shFile) {
		try {
			
			PrintWriter pw = new PrintWriter(new FileWriter(new File(shFile)));
			
			pw.println("#!/bin/sh");
			pw.println("#$-clear");
			pw.println("#$-cwd");
			pw.println("#$-l vf=7950m");
			pw.println("#$-pe serial 4");
			pw.println(". /etc/profile");
			pw.println();
			pw.println("MCL=/home/proj/BIMSC/apps/wzw/bin64/mcl"); //installation on BIM cluster
			pw.println();
			
			File[] files = new File(this.outdir).listFiles();
			for(File file: files) {
							
				if(file.getName().endsWith(MCLIN_MATRIX_SUFFIX)) {
					
					String matrixFileName = file.getName();
					String tabFileName = matrixFileName.replace(MCLIN_MATRIX_SUFFIX, "")+MCLIN_TAB_SUFFIX;
					String outfileName = matrixFileName.replace(MCLIN_MATRIX_SUFFIX, "")+MCLOUT_SUFFIX;
					
					pw.println("gunzip -c "+matrixFileName+" | $MCL - -I 1.1 -t 4 -scheme 7 -use-tab "+tabFileName+" -o "+outfileName);
				}
				
			}
			
			pw.println();
			pw.close();
			
		}catch(Exception e) {
			System.err.println("Exception in MCLClustering.writeShellScript(): " +e.getMessage());
			e.printStackTrace();
		}
	}

}
