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
 * 
 */
public class Sequences2Names {
	
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	
	public static void run() {
		
		try{
					
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT name,description,databaseid,taxonomyid " +
					"FROM proteins_merged WHERE sequenceid=? ORDER BY databaseid");
			
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT code FROM clusters_mcl t1,cp_clusters t2 " +
					"WHERE t1.cluster_id=t2.cluster_id AND " +
					"t1.cluster_threshold=t2.cluster_threshold AND " +
					"t2.type=\"sc_cluster\" AND t1.sequenceid=?");
			
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"SELECT taxonomy FROM taxonomies_merged WHERE taxonomyid=?");		
			
			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO sequences2names " +
					"(sequenceid,md5,name,description,taxonomyid,taxonomy,in_SC,sc_code) " +
					"VALUES " +
					"(?,?,?,?,?,?,?,?)");
			
			
			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT sequenceid,md5,in_SC FROM sequences2");
			int count = 0;
			while(rs.next()) {
				count++;
				if(count%10000==0){
					System.out.print("Processed "+count+"\n");
					System.out.flush();
				}
				
				int sequenceid = rs.getInt("sequenceid");
				String md5 = rs.getString("md5");
				String inSC = rs.getString("in_SC");
				
				String uniprotName = null;
				String uniprotDescription = null;
				int uniprotTaxonomyid = -1;
				String alternativeName = null;
				String alternativeDescription = null;
				int alternativeTaxonomyid = -1;
				
				
				pstm1.setInt(1, sequenceid);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					
					String name = rs1.getString("name");
					String description = rs1.getString("description");
					int databaseid = rs1.getInt("databaseid");
					int taxonomyid = rs1.getInt("taxonomyid");
					
					
					if(databaseid == 313 || databaseid == 314) {
						
						if(uniprotName == null) {
							uniprotName = name;
							uniprotDescription = description;
							uniprotTaxonomyid = taxonomyid;
						}
					}
					else{
						
						if(alternativeName == null) {
							alternativeName = name;
							alternativeDescription = description;
							alternativeTaxonomyid = taxonomyid;
						}
					}
				}
				rs1.close();
				
				
				String scCode = null;
				pstm2.setInt(1, sequenceid);
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()) {
					scCode = rs2.getString("code");
				}
				rs2.close();
				
				
				if(uniprotName != null) {
					
					String taxonomy = null;
					
					pstm3.setInt(1, uniprotTaxonomyid);
					ResultSet rs3 = pstm3.executeQuery();
					while(rs3.next()) {
						taxonomy = rs3.getString("taxonomy");
					}
					rs3.close();
					
					pstm4.setInt(1, sequenceid);
					pstm4.setString(2, md5);
					pstm4.setString(3, uniprotName);
					pstm4.setString(4, uniprotDescription);
					pstm4.setInt(5, uniprotTaxonomyid);
					pstm4.setString(6, taxonomy);
					pstm4.setString(7, inSC);
					pstm4.setString(8, scCode);
					
					pstm4.executeUpdate();
				}
				else {
					
					String taxonomy = null;
					
					pstm3.setInt(1, alternativeTaxonomyid);
					ResultSet rs3 = pstm3.executeQuery();
					while(rs3.next()) {
						taxonomy = rs3.getString("taxonomy");
					}
					rs3.close();
					
					pstm4.setInt(1, sequenceid);
					pstm4.setString(2, md5);
					pstm4.setString(3, alternativeName);
					pstm4.setString(4, alternativeDescription);
					pstm4.setInt(5, alternativeTaxonomyid);
					pstm4.setString(6, taxonomy);
					pstm4.setString(7, inSC);
					pstm4.setString(8, scCode);
					
					pstm4.executeUpdate();
				}
			}
			rs.close();
			
			stm.close();
			
			pstm1.close();
			pstm2.close();
			pstm3.close();
			pstm4.close();
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}

}
