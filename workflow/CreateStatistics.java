package workflow;

import general.CreateDatabaseTables;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;

import utils.DBAdaptor;

public class CreateStatistics {
	
	public static void run() {
		
		Connection conn = null;
		
		try {
			
			conn = DBAdaptor.getConnection("camps4");
			
			Statement stm = conn.createStatement();
			
			
			int numProteins = 0;
			int numSequences = 0;
			int numSpecies = 0;
			int numSCcluster = 0;
			int numFHcluster = 0;
			int numMDcluster = 0;
			
			int numProteinsInSC = 0;
			int numSequencesInSC = 0;
			int numSpeciesInSC = 0;
			
			
			//get number of proteins and sequences
			ResultSet rs1 = stm.executeQuery("SELECT count(*) FROM proteins2");
			while(rs1.next()) {
				numProteins = rs1.getInt("count(*)");
			}
			rs1.close();
			
			ResultSet rs2 = stm.executeQuery("SELECT count(*) FROM sequences2");
			while(rs2.next()) {
				numSequences = rs2.getInt("count(*)");
			}
			rs2.close();
			
			
			//get number of species
			ResultSet rs3 = stm.executeQuery("SELECT count(*) FROM taxonomies2");
			while(rs3.next()) {
				numSpecies = rs3.getInt("count(*)");
			}
			rs3.close();
			
			
			ArrayList<String> scClusters = new ArrayList<String>();
			//get number of camps clusters
			ResultSet rs4 = stm.executeQuery("SELECT cluster_id,cluster_threshold,type FROM cp_clusters");
			while(rs4.next()) {
				String type = rs4.getString("type");
				
				if(type.equals("sc_cluster")) {
					numSCcluster++;
					
					int clusterID = rs4.getInt("cluster_id");
					float clusterThreshold = rs4.getFloat("cluster_threshold");
					String cluster = clusterID +"#"+clusterThreshold;
					
					scClusters.add(cluster);
				}
				else if(type.equals("fh_cluster")) {
					numFHcluster++;
				}
				else if(type.equals("md_cluster")) {
					numMDcluster++;
				}
			}
			rs4.close();
					
			
			//get number of proteins, sequences and species in SC clusters
			PreparedStatement pstm = conn.prepareStatement(
					"SELECT cluster_id,cluster_threshold " +
					"FROM clusters_mcl " +
					"WHERE sequenceid=?");
			
			BitSet sequencesInSC = new BitSet();
			BitSet speciesInSC = new BitSet();
			ResultSet rs5 = stm.executeQuery("SELECT sequenceid,taxonomyid FROM proteins2");
			while(rs5.next()) {
				
				int sequenceid = rs5.getInt("sequenceid");
				int taxonomyid = rs5.getInt("taxonomyid");
				
				boolean isInSC = false;
				pstm.setInt(1, sequenceid);
				ResultSet rs = pstm.executeQuery();
				while(rs.next()) {
					int clusterID = rs.getInt("cluster_id");
					float clusterThreshold = rs.getFloat("cluster_threshold");
					String cluster = clusterID+"#"+clusterThreshold;
					
					if(scClusters.contains(cluster)) {
						isInSC = true;
					}
				}
				rs.close();
				
				if(isInSC) {
					numProteinsInSC++;
					sequencesInSC.set(sequenceid);
					speciesInSC.set(taxonomyid);
				}
			}
			rs5.close();
			
			numSequencesInSC = sequencesInSC.cardinality();
			numSpeciesInSC = speciesInSC.cardinality();
			
			stm.close();
			pstm.close();
			
			
			//insert data
			PreparedStatement pstmInsert = conn.prepareStatement(
					"INSERT INTO statistics " +
					"(statistic, value) " +
					"VALUES " +
					"(?,?)");
			
			pstmInsert.setString(1, "number_of_proteins");
			pstmInsert.setInt(2, numProteins);
			pstmInsert.executeUpdate();
			
			pstmInsert.setString(1, "number_of_sequences");
			pstmInsert.setInt(2, numSequences);
			pstmInsert.executeUpdate();
			
			pstmInsert.setString(1, "number_of_species");
			pstmInsert.setInt(2, numSpecies);
			pstmInsert.executeUpdate();
			
			pstmInsert.setString(1, "number_of_sc_cluster");
			pstmInsert.setInt(2, numSCcluster);
			pstmInsert.executeUpdate();
			
			pstmInsert.setString(1, "number_of_fh_cluster");
			pstmInsert.setInt(2, numFHcluster);
			pstmInsert.executeUpdate();
			
			pstmInsert.setString(1, "number_of_md_cluster");
			pstmInsert.setInt(2, numMDcluster);
			pstmInsert.executeUpdate();
			
			pstmInsert.setString(1, "number_of_proteins_in_sc_cluster");
			pstmInsert.setInt(2, numProteinsInSC);
			pstmInsert.executeUpdate();
			
			pstmInsert.setString(1, "number_of_sequences_in_sc_cluster");
			pstmInsert.setInt(2, numSequencesInSC);
			pstmInsert.executeUpdate();
			
			pstmInsert.setString(1, "number_of_species_in_sc_cluster");
			pstmInsert.setInt(2, numSpeciesInSC);
			pstmInsert.executeUpdate();
			
			pstmInsert.close();
			
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
		CreateDatabaseTables.create_table_statistics();
		run();
	}

}
