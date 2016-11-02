package workflow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import utils.DBAdaptor;

public class Taxonomy2CpClusterCount2 {
	
	private static int MIN_CLUSTER_SIZE = 2;
	
	
	public static void run() {
		
		Connection conn = null;
		
		try {
			
			conn = DBAdaptor.getConnection("camps4");
			
			PreparedStatement pstm1 = conn.prepareStatement(
					"SELECT cluster_code,type " +
					"FROM taxonomy2cpCluster2 " +
					"WHERE taxonomyid=?");
			//PreparedStatement pstm2 = conn.prepareStatement(
			//		"SELECT cluster_id,cluster_threshold " +
			//		"FROM cp_clusters " +
			//		"WHERE code=?");
			PreparedStatement ptm2 = conn.prepareStatement(
					"SELECT code,cluster_id,cluster_threshold " +
					"FROM cp_clusters ");
			HashMap <String,String> tbl2 = new HashMap<String,String>();
			ResultSet rt2 = ptm2.executeQuery();
			while(rt2.next()){
				String code = rt2.getString(1);
				Integer id = rt2.getInt(2);
				Float thresh = rt2.getFloat(3);
				String v = id.toString()+"_"+thresh.toString();
				tbl2.put(code, v);
			}
			rt2.close();
			ptm2.close();
			
			//PreparedStatement pstm3 = conn.prepareStatement(
			//		"SELECT sequences " +
			//		"FROM clusters_mcl_nr_info " +
			//		"WHERE cluster_id=? AND cluster_threshold=?");
			
			HashMap <String,Integer> tbl3 = new HashMap<String,Integer>();
			PreparedStatement ptm3 = conn.prepareStatement(
					"SELECT cluster_id,cluster_threshold,sequences " +
					"FROM clusters_mcl_nr_info ");
			ResultSet rt3 = ptm3.executeQuery();
			while(rt3.next()){
				
				Integer id = rt3.getInt(1);
				Float thresh = rt3.getFloat(2);
				Integer seqNo = rt3.getInt(3);
				
				String v = id.toString()+"_"+thresh.toString();
				tbl3.put(v, seqNo);
			}
			rt3.close();
			ptm3.close();
			
			//PreparedStatement pstm4 = conn.prepareStatement(
			//		"SELECT sequences " +
			//		"FROM fh_clusters_info " +
			//		"WHERE code=?");
			PreparedStatement ptm4 = conn.prepareStatement(
					"SELECT code,sequences " +
					"FROM fh_clusters_info ");
			HashMap <String,Integer> tbl4 = new HashMap<String,Integer>();
			ResultSet rt4 = ptm4.executeQuery();
			while(rt4.next()){
				String code = rt4.getString(1);
				Integer seqNo = rt4.getInt(2);
				tbl4.put(code, seqNo);
			}
			rt4.close();
			ptm4.close();
			
			PreparedStatement pstm5 = conn.prepareStatement(
					"INSERT INTO taxonomy2cpClusterCount2 "+
					"(taxonomyid, taxonomy, superkingdom, num_SC, num_FH, num_MD) " +
					"VALUES "+
					"(?,?,?,?,?,?)");
			Statement stm = conn.createStatement();
			ResultSet rs = stm.executeQuery("SELECT taxonomyid,taxonomy,superkingdom FROM taxonomies2 ORDER BY taxonomy");
			int count = 0;
			while(rs.next()) {
				count ++;
				System.out.print("Processing " + count+"\n");
				int taxonomyid = rs.getInt("taxonomyid");
				String taxonomy = rs.getString("taxonomy");
				String superkingdom = rs.getString("superkingdom");
				
				int countSC = 0;
				int countMD = 0;
				int countFH = 0;
				
				pstm1.setInt(1, taxonomyid);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					String code = rs1.getString("cluster_code");
					String type = rs1.getString("type");
					
					int size = 0;
					
					if(type.equals("sc_cluster") || type.equals("md_cluster")) {
						
						//int clusterID = -1;
						//float clusterThreshold = -1;					
																
						//pstm2.setString(1, code);
						//ResultSet rs2 = pstm2.executeQuery();
						//while(rs2.next()) {
							//clusterID = rs2.getInt("cluster_id");
							//clusterThreshold = rs2.getFloat("cluster_threshold");										
						//}
						//rs2.close();
						String temp = tbl2.get(code);
						//String v = id.toString()+"_"+thresh.toString();
						//String [] t = temp.split("_");
						//clusterID = Integer.parseInt(t[0].trim());
						//clusterThreshold = Float.parseFloat(t[1].trim());
						
						
						//pstm3.setInt(1, clusterID);
						//pstm3.setFloat(2, clusterThreshold);
						//ResultSet rs3 = pstm3.executeQuery();
						//while(rs3.next()) {
						//	size = rs3.getInt("sequences");
						//}
						//rs3.close();
						size = tbl3.get(temp);
					}
					else if(type.equals("fh_cluster")) {
						
						//pstm4.setString(1, code);						
						//ResultSet rs4 = pstm4.executeQuery();
						//while(rs4.next()) {
							//size = rs4.getInt("sequences");
						//}
						//rs4.close();
						size = tbl4.get(code);
					}
					
					
					if(size < MIN_CLUSTER_SIZE) {
						continue;
					}
					
					if(type.equals("sc_cluster")) {
						countSC++;
					}
					else if(type.equals("md_cluster")) {
						countMD++;
					}
					else if(type.equals("fh_cluster")) {
						countFH++;
					}
				}
				rs1.close();
				
				
				if(countSC > 0 || countMD > 0 || countFH > 0) {
					
					pstm5.setInt(1, taxonomyid);
					pstm5.setString(2, taxonomy);
					pstm5.setString(3, superkingdom);
					pstm5.setInt(4, countSC);
					pstm5.setInt(5, countFH);
					pstm5.setInt(6, countMD);
										
					pstm5.executeUpdate();
				}
			}
			rs.close();
			
			
			stm.close();
			pstm1.close();
			//pstm2.close();
			//pstm3.close();
			//pstm4.close();
			pstm5.close();
			
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
	
}
