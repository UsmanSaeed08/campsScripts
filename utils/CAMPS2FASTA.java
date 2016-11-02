/*
 * CAMPS2FASTA
 * 
 * Version 1.0
 * 
 * 2009-08-28
 * 
 * 
 */

package utils;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CAMPS2FASTA {
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps3");
	
	
	public static void createFasta4AllSequences(String fastaFile) {
		try {
			
			PrintWriter pw = new PrintWriter(new FileWriter(new File(fastaFile)));
			
			
			Statement stm = CAMPS_CONNECTION.createStatement();
			stm.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = stm.executeQuery("SELECT sequenceid,sequence FROM sequences");
			while(rs.next()) {
				
				int sequenceid = rs.getInt("sequenceid");
				String sequence = rs.getString("sequence");
				sequence = sequence.toUpperCase();
				
				pw.println(">"+sequenceid+"\n"+sequence);
				
				
			}
			rs.close();
			stm.close();
			
			pw.close();
			
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
	
	
	public static void createFasta4Cluster(String fastaFile, int clusterID, float clusterThreshold) {
		try {
			
			PrintWriter pw = new PrintWriter(new FileWriter(new File(fastaFile)));
			
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequence FROM sequences WHERE sequenceid=?");
			
			
			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT sequences_sequenceid FROM clusters_mcl WHERE cluster_id="+clusterID+" AND cluster_threshold="+clusterThreshold);
			while(rs.next()) {
				
				int sequenceid = rs.getInt("sequences_sequenceid");
				
				pstm.setInt(1, sequenceid);
				ResultSet rs2 = pstm.executeQuery();
				while(rs2.next()) {
					String sequence = rs2.getString("sequence");
					sequence = sequence.toUpperCase();
					
					pw.println(">"+sequenceid+"\n"+sequence);
				}
				rs2.close();			
				
			}
			rs.close();
			stm.close();
			
			pw.close();
			
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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//createFasta4AllSequences("/scratch/sneumann/Camps3/camps3.fasta");
		//createFasta4Cluster("/tmp/cluster0_7.fasta", 0, 7.0f);
	}

}
