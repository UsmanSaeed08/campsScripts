package wokflow2;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.BitSet;

import utils.DBAdaptor;

public class CreateDownloadFiles {

	/**
	 * @param args
	 */
	
	private static final String DOWNLOAD_DIR = "/webclu/w3/htdocs/download/camps3.0/";
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Making Fasta File...");
		//createFASTAFile();
		createUniProtFile();

	}
	
	
public static void createUniProtFile() {
		
		Connection conn = null;
		
		try {
			
			conn = DBAdaptor.getConnection("camps4");
			
			Statement stm = conn.createStatement();
			
			
			PreparedStatement pstm1 = conn.prepareStatement(
					"SELECT name,taxonomyid " +
					"FROM sequences2names " +
					"WHERE sequenceid=?");
			
			PreparedStatement pstm2 = conn.prepareStatement(
					"SELECT entry_name,accession " +
					"FROM camps2uniprot " +
					"WHERE sequenceid=?");
					
					
			
			//
			//collect SC-cluster sequences
			//	
			BitSet sequencesInSC = new BitSet();
			ResultSet rs = stm.executeQuery("SELECT sequenceid FROM sequences2 WHERE in_SC=\"Yes\"");
			while(rs.next()) {
				
				int sequenceid = rs.getInt("sequenceid");
				
				sequencesInSC.set(sequenceid);				
			}
			rs.close();
					
			
			
			//
			//write Uniprot file
			//	
			
			PrintWriter pw = new PrintWriter(new FileWriter(new File(DOWNLOAD_DIR+"camps3.0_UniProt.txt")));
			pw.println("CAMPS_protein_name\tUniProt_entry_name\tUniProt_accession");
			
			
			for(int sequenceid = sequencesInSC.nextSetBit(0); sequenceid>=0; sequenceid = sequencesInSC.nextSetBit(sequenceid+1)) {
				
				String campsName = "";
					
				pstm1.setInt(1, sequenceid);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					campsName = rs1.getString("name");					
				}
				rs1.close();
				
				
				pstm2.setInt(1, sequenceid);
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()) {
					String name = rs2.getString("entry_name");
					String accession = rs2.getString("accession");
					
					pw.println(campsName+"\t"+name+"\t"+accession);
				}
				rs2.close();
			}
			
			pw.close();
						
			stm.close();
			
			pstm1.close();
			pstm2.close();
					
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
public static void createFASTAFile() {
		
		Connection conn = null;
		
		try {
			int n = 0;
			conn = DBAdaptor.getConnection("camps4");
			
			Statement stm = conn.createStatement();
			
			
			PreparedStatement pstm0 = conn.prepareStatement(
					"SELECT name,taxonomyid " +
					"FROM sequences2names " +
					"WHERE sequenceid=?");
			
			PreparedStatement pstm1 = conn.prepareStatement(
					"SELECT cluster_id,cluster_threshold " +
					"FROM clusters_mcl WHERE sequenceid=?");
			
			PreparedStatement pstm2 = conn.prepareStatement(
					"SELECT code " +
					"FROM cp_clusters " +
					"WHERE cluster_id=? AND cluster_threshold=? " +
					"AND type=\"sc_cluster\"");
			
			/*PreparedStatement pstm3 = conn.prepareStatement(
					"SELECT sequence " +
					"FROM sequences2 " +
					"WHERE sequenceid=?");*/
			
					
			//
			//write FASTA file
			//	
			PrintWriter pw1 = new PrintWriter(new FileWriter(new File(DOWNLOAD_DIR+"camps3.0.fasta")));
			
			BitSet sequencesInSC = new BitSet();
			//ResultSet rs = stm.executeQuery("SELECT sequenceid FROM sequences2");select sequenceid, sequence from sequences2 where in_SC="Yes" 
			ResultSet rs = stm.executeQuery("select sequenceid, sequence from sequences2 where in_SC=\"Yes\""); 
			while(rs.next()) {
				n++;
				int sequenceid = rs.getInt("sequenceid");
				String seq = rs.getString("sequence");
				
				boolean isInSC = false;
				String scCode = "";
				pstm1.setInt(1, sequenceid);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					
					int clusterID = rs1.getInt("cluster_id");
					float clusterThreshold = rs1.getFloat("cluster_threshold");
					
					pstm2.setInt(1, clusterID);
					pstm2.setFloat(2, clusterThreshold);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						
						isInSC =true;
						
						if(!scCode.equals("")) {
							System.out.println("WARNING: Multiple SC-cluster assignments for sequence: "+sequenceid);
						}
						
						scCode = rs2.getString("code");						
					}
					rs2.close();
				}
				rs1.close();
				
				if(isInSC) {
					
					sequencesInSC.set(sequenceid);
					
					String name = "";
					int taxonomyid = -1;
					//String sequence = "";
					String sequence = seq;
					
					pstm0.setInt(1, sequenceid);
					ResultSet rs0 = pstm0.executeQuery();
					while(rs0.next()) {
						name = rs0.getString("name");
						taxonomyid = rs0.getInt("taxonomyid");
					}
					rs0.close();
					
					/*
					pstm3.setInt(1, sequenceid);
					ResultSet rs3 = pstm3.executeQuery();
					while(rs3.next()) {
						
						sequence = rs3.getString("sequence");					
					}
					rs3.close();*/
					
					pw1.println(">"+name+"|TaxID:"+taxonomyid+"|SC-cluster:"+scCode+"\n"+sequence.toUpperCase());
				}
				if (n%10000 == 0){
					System.out.println("Processed Lines: " + n);
					System.out.flush();
				}
			}
			rs.close();
			
			pw1.close();
							
			stm.close();
			
			pstm0.close();
			pstm1.close();
			pstm2.close();
			//pstm3.close();
								
			
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
