/*
 * ClustersMCLTrack
 * 
 * Version 3.0
 * 
 * 2014-08-08
 *  
 */

package workflow.initialClustering;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;


import utils.DBAdaptor;

public class ClustersMCLTrack {
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	private static final int[] THRESHOLDS_EXPONENT = new int[] {
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
		100};
	
	//private static final File CLUSTER_LIST = new File("/home/users/saeed/workspace/data/mcl_clusters_camps4.txt");
	private static final File CLUSTER_LIST = new File("/localscratch/mcl_clusters2_camps4.txt"); // -- running at london
	

	
	public static void run() {
		try {
			
			int batchSize = 50;
			int batchCounter = 0;
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl2 WHERE cluster_id=? AND cluster_threshold=?");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT cluster_id FROM clusters_mcl2 WHERE cluster_threshold=? AND sequenceid=?");
			
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_mcl_track2 " +
					"(cluster_id, cluster_threshold, child_cluster_number, child_cluster_id, child_cluster_threshold) " +
					"VALUES " +
					"(?,?,?,?,?)");
			
			System.out.println("\n\t\t[INFO]: Start cluster tracking...");
			
			for(int i=0; i<THRESHOLDS_EXPONENT.length-1; i++) {
				
				int clusterThreshold = THRESHOLDS_EXPONENT[i];
				int childClusterThreshold = THRESHOLDS_EXPONENT[i+1];
				
				//read cluster ids from file
				ArrayList<Integer> clusterIDs = new ArrayList<Integer>();
				BufferedReader br = new BufferedReader(new FileReader(CLUSTER_LIST));
				String line;
				while((line = br.readLine()) != null) {
					
					String cluster = line.trim();
					int currentClusterID = Integer.parseInt(cluster.split("\t")[0]);
					float currentClusterThreshold = Float.parseFloat(cluster.split("\t")[1]);
					
					if(currentClusterThreshold == clusterThreshold) {
						clusterIDs.add(Integer.valueOf(currentClusterID));
					}					
				}
				br.close();
				
				Collections.sort(clusterIDs);
				
				
				for(Integer clusterID: clusterIDs) {
										
					BitSet clusterMembers = new BitSet();
					pstm1.setInt(1, clusterID.intValue());
					pstm1.setFloat(2, clusterThreshold);
					
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {
						int sequenceid = rs1.getInt("sequenceid");
						clusterMembers.set(sequenceid);
					}
					rs1.close();
					rs1 = null;
					
					int childClusterNumber = 0;
					BitSet childClusterIDs = new BitSet();
					
					for(int sequenceid = clusterMembers.nextSetBit(0); sequenceid>=0; sequenceid = clusterMembers.nextSetBit(sequenceid+1)) {
						
						boolean isMember = false;
						int childClusterID = -1;
						pstm2.setFloat(1, childClusterThreshold);
						pstm2.setInt(2, sequenceid);
						ResultSet rs2 = pstm2.executeQuery();
						while(rs2.next()) {
							isMember = true;
							childClusterID = rs2.getInt("cluster_id");
						}
						rs2.close();
						rs2 = null;
						
						if(!isMember) {
							continue;
						}
						
						if(childClusterIDs.get(childClusterID)) {
							continue;
						}
						
						childClusterIDs.set(childClusterID);
						childClusterNumber++;
						
						//System.out.println(clusterThreshold+"\t"+clusterID+"\t"+childClusterNumber+"\t"+childClusterThreshold+"\t"+childClusterID);
						
						batchCounter++;
						
						pstm3.setInt(1, clusterID.intValue());
						pstm3.setFloat(2, clusterThreshold);
						pstm3.setInt(3, childClusterNumber);
						pstm3.setInt(4, childClusterID);
						pstm3.setFloat(5, childClusterThreshold);
						pstm3.addBatch();
						
						if(batchCounter % batchSize == 0) {								
							pstm3.executeBatch();
							pstm3.clearBatch();							
							
						}
						
						
					}			
					
				}			
			}
			
			pstm1.close();
			pstm1 = null;
			pstm2.close();
			pstm2 = null;
			
			pstm3.executeBatch();		//insert remaining entries
			pstm3.clearBatch();
			pstm3.close();
			pstm3 = null;
			
			System.out.println("\n\t\t[INFO]: Update table clusters_mcl_track (intersection_size is being added).");
			addIntersectionSizeToClusterTrackTable();
			
			
		} catch(Exception e) {
			System.err.println("Exception in ClustersMCLTrack.run(): " +e.getMessage());
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
	
	private static void addIntersectionSizeToClusterTrackTable() {
		try {
			
			Statement stm1 = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs1 = stm1.executeQuery("SELECT id,cluster_id,cluster_threshold,child_cluster_id,child_cluster_threshold,intersection_size FROM clusters_mcl_track2 FOR UPDATE"); 
			
			PreparedStatement stm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid " +
					"FROM clusters_mcl2 " +
					"WHERE cluster_id=? AND cluster_threshold=?");
				
			int counter = 0;
			while (rs1.next()) {
				counter ++;
				if (counter % 100 == 0) {
					System.err.write('.');
					System.err.flush();
				}
				if (counter % 8000 == 0) {
					System.out.println(counter);
					System.err.write('\n');
					System.err.flush();
				}
				
				int clusterID = rs1.getInt("cluster_id");
				float clusterThreshold = rs1.getFloat("cluster_threshold");
				
				int childClusterID = rs1.getInt("child_cluster_id");
				float childClusterThreshold = rs1.getFloat("child_cluster_threshold");
				
				
				
				BitSet clusterMembers = new BitSet();				
				
				//extract members for cluster			
				stm2.setInt(1, clusterID);
				stm2.setFloat(2, clusterThreshold);
		        ResultSet rs2 = stm2.executeQuery();
		        while(rs2.next()) {	
		        	int sequenceID = rs2.getInt("sequenceid");
		        	clusterMembers.set(sequenceID);
		        }
		        rs2.close();
		        rs2 = null;
		        
		        
		        BitSet childClusterMembers = new BitSet();				
				
				//extract members for cluster			
				stm2.setInt(1, childClusterID);
				stm2.setFloat(2, childClusterThreshold);
		        ResultSet rs3 = stm2.executeQuery();
		        while(rs3.next()) {	
		        	int sequenceID = rs3.getInt("sequenceid");
		        	childClusterMembers.set(sequenceID);
		        }
		        rs3.close();
		        rs3 = null;
		        
		        
		        
		        int intersectionSize = 0;
		        
		        for(int sequenceid = childClusterMembers.nextSetBit(0); sequenceid>=0; sequenceid = childClusterMembers.nextSetBit(sequenceid+1)) {
		        	
		        	if(clusterMembers.get(sequenceid)) {
		        		intersectionSize++;
		        	}
		        }
				
				rs1.updateInt("intersection_size",intersectionSize);				
				rs1.updateRow();					 
			}
			
			rs1.close();
			rs1 = null;
			stm1.close();
			stm1 = null;	
			stm2.close();
			stm2 = null;
			
		} catch(Exception e) {
			e.printStackTrace();
			
		} 
	}

}
