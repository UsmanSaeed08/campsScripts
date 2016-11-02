package workflow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utils.DBAdaptor;

public class UpdateClusters_infoTable {

	/**
	 * @param args
	 */
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4"); //changed to test_camps3
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		System.out.println("Running...");
		
		Run();
		closeConnection();
		/*
		 * System.out.println("\tRead ontology file...");
			PreparedStatement pstm5 = CAMPS_CONNECTION.prepareStatement(
					"UPDATE go_annotations SET name=? WHERE accession=?");
			
			pstm5.setString(1, previousName);
						pstm5.setString(2, previousAccession);
						
						pstm5.executeUpdate();
		 */

	}

	private static void closeConnection() {
		// TODO Auto-generated method stub
		try{
			CAMPS_CONNECTION.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	/**
	 * This function updates the number of sequences to the actual count of sequences present in the cluster
	 * based on counting cluster members in clusters_mcl tables and updates in clusters_mcl_nr_info table in the column sequences_r
	 */
	private static void Run() {
		// TODO Auto-generated method stub
		try{
			// loop through each clustr in clusters_mcl_nr_info
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select cluster_id, cluster_threshold from clusters_mcl_nr_info"); 
			
			// find the number of members for respective clusters
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select count(*) from clusters_mcl where cluster_id=? and cluster_threshold=?");
			
			// update record in clusters_mcl_nr_info table ---> column name sequences_r
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("UPDATE clusters_mcl_nr_info SET sequences_r=? WHERE cluster_id=? and cluster_threshold=?");
			int count = 0;
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				count ++;
				Integer clusId = rs1.getInt(1);
				Float clusThresh = rs1.getFloat(2);
				
				pstm2.setInt(1, clusId);
				pstm2.setFloat(2, clusThresh);
				
				ResultSet rs2 = pstm2.executeQuery();
				Integer numberOfMembers = 0;
				while(rs2.next()){
					numberOfMembers = rs2.getInt(1);
				}
				pstm2.clearBatch();
				rs2.close();
				
				// now update		
				pstm3.setInt(1, numberOfMembers); 
				
				pstm3.setInt(2, clusId);
				pstm3.setFloat(3, clusThresh);
				pstm3.executeUpdate();
				pstm3.clearBatch();
				
				System.out.println("Clusid: "+ clusId + "\tThreshold: "+ clusThresh+"\tMembers: "+numberOfMembers + "    ---- "+ count);
			
			}
			pstm1.close();
			pstm2.close();
			pstm3.close();
			
			rs1.close();
			
		
		}
		catch(Exception e){
			e.printStackTrace();
			System.exit(0);
		}
		
	}

}
