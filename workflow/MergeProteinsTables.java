/*
 * MergeProteinsTables
 * 
 * Version 2.0
 * 
 * 2014-06-08
 * 
 */

package workflow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.BitSet;

import utils.DBAdaptor;

/*
 * Merges CAMPS tables 'proteins' and 'camps2uniprot' and writes
 * all entries to table 'proteins_merged'. Also merges tables
 * 'taxonomies' and 'camps2uniprot_taxonomies' to 'taxonomies_merged'
 */
public class MergeProteinsTables {
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	
	public static void run() {
		int count =0;
		try {
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT * FROM camps2uniprot_taxonomies " +
					"WHERE taxonomyid=?");
			
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT * FROM taxonomies2 " +
					"WHERE taxonomyid=?");
			
			PreparedStatement pstm_insertProteins = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO proteins_merged " +
					"(sequenceid,name,description,databaseid,taxonomyid) VALUES " +
					"(?,?,?,?,?)");	
			
			PreparedStatement pstm_insertTaxonomies = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO taxonomies_merged " +
					"(taxonomyid, taxonomy, species, genus, family, order_, class, phylum, kingdom, superkingdom) VALUES " +
					"(?,?,?,?,?,?,?,?,?,?)");
			
			
			
			Statement stm = CAMPS_CONNECTION.createStatement();
			
			BitSet alreadyProcessedTaxIDs = new BitSet();
			System.out.print("\n Processing camps2uniprot \n");
			
			ResultSet rs1 = stm.executeQuery(
					"SELECT sequenceid,entry_name,description,taxonomyid,subset " +
					"FROM camps2uniprot");
			
			while(rs1.next()) {
				
				int sequenceid = rs1.getInt("sequenceid");
				String name = rs1.getString("entry_name");
				String description = rs1.getString("description");
				int taxonomyid = rs1.getInt("taxonomyid");
				String subset = rs1.getString("subset");
				
				int databaseid = -1;
				if(subset.equals("swissprot")) {
					databaseid = 313;
				}
				else if(subset.equals("trembl")) {
					databaseid = 314;
				}
				
				//
				//insert into proteins_merged
				//
				pstm_insertProteins.setInt(1, sequenceid);
				pstm_insertProteins.setString(2, name);
				pstm_insertProteins.setString(3, description);
				pstm_insertProteins.setInt(4, databaseid);
				pstm_insertProteins.setInt(5, taxonomyid);
				
				pstm_insertProteins.executeUpdate();
				
				
				
				//
				//fill table taxonomies_merged, if necessary
				//
				if(!alreadyProcessedTaxIDs.get(taxonomyid)) {
					
					String taxonomy = null;
					String species = null;
					String genus = null;
					String family = null;
					String order = null;
					String class_ = null;
					String phylum = null;
					String kingdom = null;
					String superkingdom = null;
					
					pstm1.setInt(1, taxonomyid);
					ResultSet rs = pstm1.executeQuery();
					while(rs.next()) {
						taxonomy = rs.getString("taxonomy");
						species = rs.getString("species");
						genus = rs.getString("genus");
						family = rs.getString("family");
						order = rs.getString("order_");
						class_ = rs.getString("class");
						phylum = rs.getString("phylum");
						kingdom = rs.getString("kingdom");
						superkingdom = rs.getString("superkingdom");
					}
					rs.close();
					
					pstm_insertTaxonomies.setInt(1, taxonomyid);
					pstm_insertTaxonomies.setString(2, taxonomy);
					pstm_insertTaxonomies.setString(3, species);
					pstm_insertTaxonomies.setString(4, genus);
					pstm_insertTaxonomies.setString(5, family);
					pstm_insertTaxonomies.setString(6, order);
					pstm_insertTaxonomies.setString(7, class_);
					pstm_insertTaxonomies.setString(8, phylum);
					pstm_insertTaxonomies.setString(9, kingdom);
					pstm_insertTaxonomies.setString(10, superkingdom);
					
					pstm_insertTaxonomies.executeUpdate();
					
					alreadyProcessedTaxIDs.set(taxonomyid);
				}
				count ++;
				if(count % 10000 == 0){
					System.out.print(".");
					System.out.flush();
					if (count % 1000000 == 0){
						System.out.print("\n");
						System.out.flush();
					}
				}
				
			}
			rs1.close();
			
			System.out.print("\n camps2uniprot complete \n");
			System.out.print("Processing Protiens now \n");
			count =0;
			
			ResultSet rs2 = stm.executeQuery(
					"SELECT sequenceid,name,description,databaseid,taxonomyid " +
					"FROM proteins2");
			
			while(rs2.next()) {
				
				int sequenceid = rs2.getInt("sequenceid");
				String name = rs2.getString("name");
				String description = rs2.getString("description");
				int databaseid = rs2.getInt("databaseid");
				int taxonomyid = rs2.getInt("taxonomyid");
				
				
				//
				//insert into proteins_merged
				//
				pstm_insertProteins.setInt(1, sequenceid);
				pstm_insertProteins.setString(2, name);
				pstm_insertProteins.setString(3, description);
				pstm_insertProteins.setInt(4, databaseid);
				pstm_insertProteins.setInt(5, taxonomyid);
				
				pstm_insertProteins.executeUpdate();
				
				
				
				//
				//fill table taxonomies_merged, if necessary
				//
				if(!alreadyProcessedTaxIDs.get(taxonomyid)) {
					
					String taxonomy = null;
					String species = null;
					String genus = null;
					String family = null;
					String order = null;
					String class_ = null;
					String phylum = null;
					String kingdom = null;
					String superkingdom = null;
					
					pstm2.setInt(1, taxonomyid);
					ResultSet rs = pstm2.executeQuery();
					while(rs.next()) {
						taxonomy = rs.getString("taxonomy");
						species = rs.getString("species");
						genus = rs.getString("genus");
						family = rs.getString("family");
						order = rs.getString("order_");
						class_ = rs.getString("class");
						phylum = rs.getString("phylum");
						kingdom = rs.getString("kingdom");
						superkingdom = rs.getString("superkingdom");
					}
					rs.close();
					
					pstm_insertTaxonomies.setInt(1, taxonomyid);
					pstm_insertTaxonomies.setString(2, taxonomy);
					pstm_insertTaxonomies.setString(3, species);
					pstm_insertTaxonomies.setString(4, genus);
					pstm_insertTaxonomies.setString(5, family);
					pstm_insertTaxonomies.setString(6, order);
					pstm_insertTaxonomies.setString(7, class_);
					pstm_insertTaxonomies.setString(8, phylum);
					pstm_insertTaxonomies.setString(9, kingdom);
					pstm_insertTaxonomies.setString(10, superkingdom);
					
					pstm_insertTaxonomies.executeUpdate();
					
					alreadyProcessedTaxIDs.set(taxonomyid);
				}
				count ++;
				if(count % 10000 == 0){
					System.out.print(".");
					System.out.flush();
					if (count % 1000000 == 0){
						System.out.print("\n");
						System.out.flush();
					}
				}
			}
			rs2.close();
			
			
			stm.close();
			pstm1.close();
			pstm2.close();
			
			pstm_insertProteins.close();
			pstm_insertTaxonomies.close();
			
		}catch(Exception e) {
			System.err.println("Exception in MergeProteinsTables.run(): " +e.getMessage());
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
}
