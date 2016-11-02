package workflow.initialClustering;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import utils.DBAdaptor;

/**
 * 
 * @author usman
 * 
 * Creates a list of all distinct MCL clusters. This list is needed
 * to do the homology cleanup for these clusters.
 *
 */
public class CreateMCLClusterList {
	
	public static void run(File outfile) {
		
		Connection conn = null;
		
		try {
			
			conn = DBAdaptor.getConnection("camps4");
			
			PrintWriter pw = new PrintWriter(new FileWriter(outfile));
			
			Statement stm = conn.createStatement();
			ResultSet rs = stm.executeQuery("select distinct cluster_id,cluster_threshold from clusters_mcl2;");
			while(rs.next()) {
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");
				
				pw.println(clusterID+"\t"+clusterThreshold);
				System.out.println(clusterID+"\t"+clusterThreshold);
			}
			rs.close();
			
			pw.close();
			stm.close();
			
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
		
		//run(new File("/home/users/saeed/workspace/data/mcl_clusters_camps3.txt"));
		run(new File("F:/mcl_clusters2_camps3.txt"));

	}

}
