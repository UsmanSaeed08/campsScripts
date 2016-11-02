package workflow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import utils.DBAdaptor;

/**
 * 
 * 
 * 
 * Tags all sequences and taxonomies that are associated with SC-clusters.
 *
 */
public class TagSequencesInSC {
	
	
	public static void run() {
		
		Connection conn = null;
		
		try {
			
			conn = DBAdaptor.getConnection("camps4");
			Statement stm = conn.createStatement();
			
			PreparedStatement pstm1 = conn.prepareStatement(
					"SELECT sequenceid " +
					"FROM clusters_mcl2 " +
					"WHERE cluster_id=? AND cluster_threshold=?");
			
			PreparedStatement pstm2 = conn.prepareStatement(
					"UPDATE sequences2 SET in_SC=\"Yes\" WHERE sequenceid=?");
			
			PreparedStatement pstm3 = conn.prepareStatement(
					"SELECT distinct(taxonomyid) " +
					"FROM proteins_merged " +
					"WHERE sequenceid=?");
			
			PreparedStatement pstm4 = conn.prepareStatement(
					"UPDATE taxonomies_merged SET in_SC=\"Yes\" WHERE taxonomyid=?");
			
			ResultSet rs = stm.executeQuery(
					"SELECT cluster_id, cluster_threshold " +
					"FROM cp_clusters2 " +
					"WHERE type=\"sc_cluster\"");
			
			int countUpdatesInSequenceTable = 0;
			int p =0;
			System.out.print("Running\n");
			while(rs.next()) {
				p++;
				if(p%100==0){
					System.out.print(p+"\n");
					System.out.flush();
				}
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");
				
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					
					int sequenceID = rs1.getInt("sequenceid");
					
					pstm2.setInt(1, sequenceID);
					int updatedRows = pstm2.executeUpdate();
					
					if(updatedRows > 0) {
						countUpdatesInSequenceTable++;
					}
					
					
					pstm3.setInt(1, sequenceID);
					ResultSet rs3 = pstm3.executeQuery();
					while(rs3.next()) {
						
						int taxonomyID = rs3.getInt("taxonomyid");
						
						pstm4.setInt(1, taxonomyID);
						pstm4.executeUpdate();
					}
					rs3.close();
					
				}
				rs1.close();
			}
			rs.close();
			
			stm.close();
			
			pstm1.close();
			pstm2.close();
			pstm3.close();
			pstm4.close();
			
			
			System.out.println("Number of updates rows in sequence table: "+countUpdatesInSequenceTable);
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if (conn != null) {
				try {
					conn.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		run();
	}

}
