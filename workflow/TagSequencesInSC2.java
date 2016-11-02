package workflow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import utils.DBAdaptor;
import utils.FastaReader.FastaEntry;

/**
 * 
 * 
 * 
 * Tags all sequences and taxonomies that are associated with SC-clusters.
 * 
 * Difference to TagSequencesInSC2:
 * - use table 'proteins' instead of 'proteins_merged'
 * - use table 'taxonomies' instead of 'taxonomies_merged'
 *
 */
public class TagSequencesInSC2 {


	public static void run() {

		Connection conn = null;

		try {

			conn = DBAdaptor.getConnection("camps4");
			Statement stm = conn.createStatement();

			//PreparedStatement pstm1 = conn.prepareStatement(
			//		"SELECT sequenceid " +
			//		"FROM clusters_mcl " +
			//		"WHERE cluster_id=? AND cluster_threshold=?");
			
			System.out.print("Populating clusters_mcl\n");
			HashMap<String, ArrayList<Integer>> ptbl1 = new HashMap<String, ArrayList<Integer>>(); 
			PreparedStatement ptm1 = conn.prepareStatement(
					"SELECT sequenceid, cluster_id, cluster_threshold " +
					"FROM clusters_mcl2 ");
			ResultSet rt1 = ptm1.executeQuery();
			while(rt1.next()){
				Integer sqid = rt1.getInt(1);
				Integer clusid = rt1.getInt(2);
				Float thresh = rt1.getFloat(3);
				String key = clusid.toString() +"_"+thresh.toString();
				if (ptbl1.containsKey(key)){
					ArrayList<Integer> temp = ptbl1.get(key);
					temp.add(sqid);
					ptbl1.put(key, temp);
				}
				else{
					ArrayList<Integer> temp = new ArrayList<Integer>();
					temp.add(sqid);
					ptbl1.put(key, temp);
				}
			}
			ptm1.close();
			rt1.close();


			//PreparedStatement pstm3 = conn.prepareStatement(
			//		"SELECT distinct(taxonomyid) " +
			//		"FROM proteins2 " +
			//		"WHERE sequenceid=?");
			System.out.print("Populating proteins2\n");
			PreparedStatement ptm3 = conn.prepareStatement(
					"SELECT taxonomyid, sequenceid " +
					"FROM proteins2 ");
			HashMap<Integer, ArrayList<Integer>> ptbl3 = new HashMap<Integer, ArrayList<Integer>>();
			ResultSet rt3 = ptm3.executeQuery();
			while(rt3.next()){
				Integer taxid = rt3.getInt(1);
				Integer sqid = rt3.getInt(2);
				if(ptbl3.containsKey(sqid)){
					ArrayList<Integer> x = ptbl3.get(sqid);
					if(!x.contains(taxid)){ // have distinct taxids
						x.add(taxid);
						ptbl3.put(sqid, x);
					}
				}
				else{
					ArrayList<Integer> x = new ArrayList<Integer>();
					x.add(taxid);
					ptbl3.put(sqid, x);
				}
			}
			ptm3.close();
			rt3.close();

			PreparedStatement pstm4 = conn.prepareStatement(
					"UPDATE taxonomies2 SET in_SC=\"Yes\" WHERE taxonomyid=?");

			ResultSet rs = stm.executeQuery(
					"SELECT cluster_id, cluster_threshold " +
							"FROM cp_clusters2 " +
					"WHERE type=\"sc_cluster\"");

			int p =0;
			System.out.print("Running\n");
			while(rs.next()) {
				p++;
				if(p%100==0){
					System.out.print(p+"\n");
					System.out.flush();
				}
				Integer clusterID = rs.getInt("cluster_id");
				Float clusterThreshold = rs.getFloat("cluster_threshold");

				String key = clusterID.toString() +"_"+clusterThreshold.toString();
				ArrayList<Integer> sequenceIds = ptbl1.get(key);
				
				//pstm1.setInt(1, clusterID);
				//pstm1.setFloat(2, clusterThreshold);
				//ResultSet rs1 = pstm1.executeQuery();
				//while(rs1.next()) {
				for(int i = 0; i<=sequenceIds.size()-1;i++){
					int sequenceId = sequenceIds.get(i);
							

					//int sequenceID = rs1.getInt("sequenceid");
					
					ArrayList<Integer> taxIds = ptbl3.get(sequenceId);


				//	pstm3.setInt(1, sequenceID);
				//	ResultSet rs3 = pstm3.executeQuery();
					//while(rs3.next()) {
					for(int j=0;j<=taxIds.size()-1;j++){

						//int taxonomyID = rs3.getInt("taxonomyid");
						int taxonomyID = taxIds.get(j);

						pstm4.setInt(1, taxonomyID);
						pstm4.executeUpdate();
					}
					//rs3.close();

				//}
				}
				//rs1.close();
			}
			rs.close();

			stm.close();

			//pstm1.close();

			//pstm3.close();
			pstm4.close();



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
