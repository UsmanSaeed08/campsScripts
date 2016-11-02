/*
 * HomologyCleanup
 * 
 * Version 3.0
 * 
 * 2014-08-09
 * 
 */

package workflow.initialClustering;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;

import properties.ConfigFile;

import utils.DBAdaptor;
import utils.FastaReader;
import utils.FastaReader.FastaEntry;



/**
 * 
 * @author usman
 * 
 * Creates non-redundant initial clusters using cd-hit.
 *
 */
public class HomologyCleanup {
	
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	private static final File CLUSTER_LIST = new File("/home/users/saeed/workspace/data/mcl_clusters_camps4.txt");
	
	
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
	
	private int start;
	private int end;
	private int length;
	
	
	public HomologyCleanup(int start, int length) {
		this.start = start;
		this.length = length;
		this.end = (start+length)-1;
	}
	
	
	
	/**
	 * Runs the whole program.
	 */
	private void run() {		
		try {		
			
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT id,sequenceid FROM clusters_mcl WHERE cluster_id=? and cluster_threshold=?");
			
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT sequence FROM sequences2 WHERE sequenceid=? limit 1");
			
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("UPDATE clusters_mcl SET redundant=\"No\" WHERE id=?");
			
			Statement stm = CAMPS_CONNECTION.createStatement();
			
			int maxSequenceid = 100000;
			ResultSet rs = stm.executeQuery("SELECT max(sequenceid) FROM sequences2");
			while(rs.next()) {
				maxSequenceid = rs.getInt("max(sequenceid)");
			}
			rs.close();
			stm.close();
			
			//index := sequences_sequenceid, value: row number in clusters_mcl
			int[] sequenceid2rowid = new int[maxSequenceid+1];
			
			
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
					
					String cluster = line.trim();
					int clusterID = Integer.parseInt(cluster.split("\t")[0]);
					float clusterThreshold = Float.parseFloat(cluster.split("\t")[1]);
														
					//get distinct sequences for each cluster
					System.out.println("\n\n\t[INFO] Get cluster members for: "+clusterID+"\t"+clusterThreshold+"  ("+counter+"/"+this.length+")");
					BitSet sequenceIDs = new BitSet();
					pstm1.setInt(1, clusterID);		
					pstm1.setFloat(2, clusterThreshold);
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {
						int id = rs1.getInt("id");
						int sequenceid = rs1.getInt("sequenceid");
						sequenceIDs.set(sequenceid);
						
						sequenceid2rowid[sequenceid] = id;
					}
					rs1.close();
					rs1 = null;
					
					
					//write tmp FASTA file for cd-hit
					File cdhitInfileTmp = File.createTempFile("cluster"+clusterID+"_"+clusterThreshold, ".fasta");
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
					File cdhitOutfileTmp = File.createTempFile("cluster"+clusterID+"_"+clusterThreshold+"_NR", ".fasta");
					ConfigFile cf = new ConfigFile("/home/users/saeed/workspace/CAMPS3/config/config.xml");
					String cdhitDir = cf.getProperty("apps:cdhit:install-dir");
				    String cdhitCmd = cdhitDir + " -i " +cdhitInfileTmp.getAbsolutePath() + " -o " +cdhitOutfileTmp.getAbsolutePath() + " -c " + SIMILARITY_THRESHOLD + " -n " + WORD_LENGTH + " -aL 0.8 -aS 0.8";
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
						
						int rowid = sequenceid2rowid[sequenceid];
						
						pstm3.setInt(1, rowid);
											
						pstm3.executeUpdate();
						
					}
					
					//remove tmp files
					new File(cdhitOutfileTmp.getAbsoluteFile()+".bak.clstr").delete();
					new File(cdhitOutfileTmp.getAbsoluteFile()+".clstr").delete();
					cdhitInfileTmp.delete();
					cdhitOutfileTmp.delete();
					
				}
				
				if(lineCount == this.end) {
					break;  //stop process
				}
			}				
			
			pstm1.close();
			pstm1 = null;
			
			pstm2.close();
			pstm2 = null;
			
			pstm3.close();
			pstm3 = null;
			
			
		} catch(Exception e) {
			System.err.println("Exception in HomologyCleanup.run(): " +e.getMessage());
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
			
			int start = Integer.parseInt(args[0]);
			int length = Integer.parseInt(args[1]); 
									
			HomologyCleanup hc = new HomologyCleanup(start,length);
			hc.run();
			
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}	
				
	}

}
