/**
 * 
 */
package extract_proteins;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.BitSet;
import java.util.Calendar;

import utils.DBAdaptor;
import java.util.Calendar;
import java.util.Date;
/**
 * @author Usman
 * Extracts all membrane protein sequences from SIMAP that have at least one
 * predicted transmembrane helices. Thereby, sequences from metagenomes are
 * ignored.
 * 
 * The resulting data is stored into the following tables:
 * - 'proteins': contains all membrane proteins (MPs)
 * - 'sequences': contains all membrane sequences
 * - 'databases_': contains database information where MPs come from
 * - 'taxonomies': contains taxonomy information for each MP
 * 
 *
 */
public class ExtractMembraneProteins {
private static final Connection SIMAP_CONNECTION = DBAdaptor.getConnection("simap"); 
	
	private static final Connection SIMAPF_CONNECTION = DBAdaptor.getConnection("simapfeatures");
	
	//private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps3");
	//private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("test_privilig");
	
	
		
	////////////////////////////////////////////////////////////////////////////
	//																		  //
	//				filter parameters										  //
	//      																  //
	////////////////////////////////////////////////////////////////////////////
	
	//minimal number of (predicted) transmembrane helices for a sequence to be
	//integrated into CAMPS
	private static final int MIN_NUMBER_TMH = 1;
	
	//
	//to get this list: go to NCBI Taxonomy Browser and search for '*metagenome*'
	//(use option: wild card!)
	//
	private static final BitSet TAXIDS_METAGENOME = new BitSet() {
		{
			set(256318);
			set(408169);
			set(408170);
			set(408171);
			set(408172);	//marine metagenome: note that sequences of this metagenome are also within Genbank!
			set(410656);
			set(410657);
			set(410658);
			set(410659);
			set(410661);
			set(410662);
			set(412754);
			set(412755);
			set(412756);
			set(412757);
			set(433724);
			set(433727);
			set(433733);
			set(444079);
			set(449393);
			set(452919);
			set(496920);
			set(496921);
			set(496922);
			set(496923);
			set(496924);
			set(506599);
			set(506600);
			set(527639);
			set(527640);
			set(539655);
			set(540485);
			set(556182);
			set(646099);
			set(652676);
			set(655179);
			set(662107);
		}
	};
	
	
	
	/**
	 * Runs the whole program.
	 */
	public static void run() {
		
		try {
			
			
			BufferedWriter out = new BufferedWriter(new FileWriter("file.txt")); //a file to write time log
			
			int batchSize = 50;
			//SIMAP sequence ids of membrane proteins
			BitSet membraneProteins = getMembraneProteins();
			System.out.println("get membrane proteins accomplished. \n");
			//SIMAP databaseids of completely sequenced genomes
			BitSet genomes = getCompleteGenomes();		
			System.out.println("get complete genomes accomplished. \n");
			/*
			 * Extract all proteins that are not of metagenome origin and save
			 * it to table 'proteins'.
			 */
//			Date a= new Date();
			
			out.write("[INFO]: Table 'proteins' is being filled."); //TIME LOG
			Date a = Calendar.getInstance().getTime();
	        out.write("\n The start time for table protein to be filled is: " + a);
			
			
			System.out.println("\t\t[INFO]: Table 'proteins' is being filled.");
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO proteins2 " +
					"(sequenceid,name,description,databaseid,taxonomyid) VALUES " +
					"(?,?,?,?,?)");	
			
			int statusCounter1 = 0;
			int batchCounter1 = 0;
			
			BitSet databaseids = new BitSet();
			BitSet taxonomyids = new BitSet();
			//candidates: membrane proteins with at least 3 TMH from completely sequenced genomes
			BitSet candidates = new BitSet();	
			
			Statement stm1 = SIMAP_CONNECTION.createStatement();
			stm1.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs1 = stm1.executeQuery("SELECT sequenceid,name,description,databaseid,taxonomyid FROM protein");
			while(rs1.next()) {
				int sequenceid = rs1.getInt("sequenceid");
				if (sequenceid == 1511323){
					System.out.print("\nFOUND\n");
				}
				String name = rs1.getString("name");
				String description = rs1.getString("description");
				int databaseid = rs1.getInt("databaseid");
				int taxonomyid = rs1.getInt("taxonomyid");
				
								
				//ignore proteins that are not membrane proteins
				if(!membraneProteins.get(sequenceid)) {
					continue;
				}
				//ignore proteins that are not from completely sequenced genomes
				if(!genomes.get(databaseid)) {
					continue;
				}
				
				
				databaseids.set(databaseid);
				taxonomyids.set(taxonomyid);
				candidates.set(sequenceid);
				
				//add protein to table
				statusCounter1 ++;
				if (statusCounter1 % 100 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter1 % 10000 == 0) {
					System.out.write('\n');
					System.out.flush();
				}						
				
				batchCounter1++;							
							
				pstm1.setInt(1, sequenceid);
				pstm1.setString(2, name);
				pstm1.setString(3, description);
				pstm1.setInt(4, databaseid);
				pstm1.setInt(5, taxonomyid);
							
				pstm1.addBatch();
				if(batchCounter1 % batchSize == 0) {								
					pstm1.executeBatch();
					pstm1.clearBatch();
				}							
			}
			
			//insert remaining entries
			pstm1.executeBatch();
			pstm1.clearBatch();
			
			pstm1.close();	
			pstm1 = null;				
			
			rs1.close();
			
			membraneProteins = null;	//not needed any more
			a = Calendar.getInstance().getTime();
	        out.write("\n The End time for table protein to be filled is: " + a);
			
			/*
			 * Extract all sequences that are not of metagenome origin and save
			 * it to table 'sequences'.
			 */		
			System.out.println("\t\t[INFO]: Table 'sequences' is being filled.");
			
			a = Calendar.getInstance().getTime();
	        out.write("\n The Start time for table sequences to be filled is: " + a);
			
			
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO sequences2 " +
					"(sequenceid,md5,length,selfscore,sequence,redundant) VALUES " +
					"(?,?,?,?,?,\"yes\")");	
			
			int statusCounter2 = 0;
			int batchCounter2 = 0;
			
			Statement stm2 = SIMAP_CONNECTION.createStatement();
			stm2.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs2 = stm2.executeQuery("SELECT sequenceid,md5,length,selfscore,sequence FROM sequence");
			while(rs2.next()) {
				int sequenceid = rs2.getInt("sequenceid");
				
				String md5 = rs2.getString("md5");
				int length = rs2.getInt("length");
				int selfscore = rs2.getInt("selfscore");
				String sequence = rs2.getString("sequence");
				
				//ignore sequences that are not in the candidates list
				if(!candidates.get(sequenceid)) {
					continue;
				}
				
				//add sequence to table
				statusCounter2 ++;
				if (statusCounter2 % 100 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter2 % 10000 == 0) {
					System.out.write('\n');
					System.out.flush();
				}						
				
				batchCounter2++;							
							
				pstm2.setInt(1, sequenceid);
				pstm2.setString(2, md5);
				pstm2.setInt(3, length);
				pstm2.setInt(4, selfscore);
				pstm2.setString(5, sequence);
							
				pstm2.addBatch();
				if(batchCounter2 % batchSize == 0) {								
					pstm2.executeBatch();
					pstm2.clearBatch();
				}
			}			
			
			//insert remaining entries
			pstm2.executeBatch();
			pstm2.clearBatch();
			
			pstm2.close();	
			pstm2 = null;				
			
			rs2.close();
			
			candidates = null;	//not needed any more
			
			a = Calendar.getInstance().getTime();
	        out.write("\n The End time for table sequences to be filled is: " + a);
			
			
			/*
			 * Extract all database informations from SIMAP and save 
			 * it to table 'databases'.
			 */		
	        
	        a = Calendar.getInstance().getTime();
	        out.write("\n The Start time for table databases to be filled is: " + a);
			
	        
			System.out.println("\t\t[INFO]: Table 'databases' is being filled.");
			PreparedStatement pstm3 = SIMAP_CONNECTION.prepareStatement("SELECT "+
					"db.taxonomyid,db.name,db.description AS db_description,source.description as source_description " +
					"FROM db,source " +
					"WHERE db.sourceid=source.sourceid AND " +
					"db.databaseid=?");
			
			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO databases_2 " +
					"(databaseid,taxonomyid,name,description,source) VALUES " +
					"(?,?,?,?,?)");	
			
			for(int databaseid = databaseids.nextSetBit(0); databaseid>=0; databaseid = databaseids.nextSetBit(databaseid+1)) {
				
				pstm3.setInt(1, databaseid);
				ResultSet rs3 = pstm3.executeQuery();
				while(rs3.next()) {
					int taxonomyid = rs3.getInt("taxonomyid");
					String name = rs3.getString("name");
					String description = rs3.getString("db_description");
					String source = rs3.getString("source_description");
					
					pstm4.setInt(1, databaseid);
					pstm4.setInt(2, taxonomyid);
					pstm4.setString(3, name);
					pstm4.setString(4, description);
					pstm4.setString(5, source);
					pstm4.executeUpdate();
				}
				rs3.close();
				
			}
			
			pstm3.close();
			pstm4.close();
			
			a = Calendar.getInstance().getTime();
	        out.write("\n The End time for table databases to be filled is: " + a);
			
			/*
			 * Extract all taxonomy information from SIMAP and save
			 * it to table 'taxonomies'.
			 */		
	        
	        a = Calendar.getInstance().getTime();
	        out.write("\n The Start time for table taxonomies to be filled is: " + a);
			
			System.out.println("\t\t[INFO]: Table 'taxonomies' is being filled.");
			PreparedStatement pstm5 = SIMAP_CONNECTION.prepareStatement(
					"SELECT n1.node_id,n1.parent_node,n1.rank,n2.name " +
					"FROM ncbi_tax_node n1,ncbi_tax_name n2 " +
					"WHERE n1.node_id=? AND n2.class=\"scientific name\" AND n1.node_id=n2.node_id");
			
			PreparedStatement pstm6 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO taxonomies3 " +
					"(taxonomyid, species, genus, family, order_, class, phylum, kingdom, superkingdom) VALUES " +
					"(?,?,?,?,?,?,?,?,?)");
			
			for(int taxonomyid = taxonomyids.nextSetBit(0); taxonomyid>=0; taxonomyid = taxonomyids.nextSetBit(taxonomyid+1)) {
				
				String species = null;
				String genus = null;
				String family = null;
				String order = null;
				String class_ = null;
				String phylum = null;
				String kingdom = null;
				String superkingdom = null;
				
				if(taxonomyid == 0 || taxonomyid == 1) {
					continue;
				}
				
				int nodeID = taxonomyid;
				int parentNodeID = 0;
				while(parentNodeID != 1) {
					
					int countEntries = 0;
					pstm5.setInt(1, nodeID);
					ResultSet rs5 = pstm5.executeQuery();
					while(rs5.next()) {
						countEntries++;
						
						nodeID = rs5.getInt("node_id");
						parentNodeID = rs5.getInt("parent_node");
						String rank = rs5.getString("rank");
						String name = rs5.getString("name");
						if(rank.equals("species") && genus == null) {
							species = name;
						}
						else if(rank.equals("genus") && genus == null) {
							genus = name;
						}
						else if(rank.equals("family") && family == null) {
							family = name;
						}
						else if(rank.equals("order") && order == null) {
							order = name;
						}
						else if(rank.equals("class") && class_ == null) {
							class_ = name;
						}
						else if(rank.equals("phylum") && phylum == null) {
							phylum = name;
						}
						else if(rank.equals("kingdom") && kingdom == null) {
							kingdom = name;
						}
						else if(rank.equals("superkingdom") && superkingdom == null) {
							superkingdom = name;
						}
					}
					rs5.close();
					
					if(countEntries == 0) {
						System.out.println("WARNING: Could not found taxonomyid " +nodeID+ " in SIMAP!");
						break;
					}
					
					nodeID = parentNodeID;
				}						
				
				pstm6.setInt(1,taxonomyid);	
				pstm6.setString(2, species);
				pstm6.setString(3, genus);
				pstm6.setString(4, family);
				pstm6.setString(5, order);
				pstm6.setString(6, class_);
				pstm6.setString(7, phylum);
				pstm6.setString(8, kingdom);
				pstm6.setString(9, superkingdom);
				pstm6.executeUpdate();
				
			}
			
			pstm5.close();
			pstm6.close();
			a = Calendar.getInstance().getTime();
			out.write("\n The End time for table taxonomies to be filled is: " + a);
			out.close();			
			
			
			
		} catch(Exception e) {
			System.err.println("Exception in ExtractMembraneProteins.run(): " +e.getMessage());
			e.printStackTrace();
		
		} finally {
			
			if (SIMAPF_CONNECTION != null) {
				try {
					SIMAPF_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			if (SIMAP_CONNECTION != null) {
				try {
					SIMAP_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}
	
	
	/*
	 * Returns the latest SIMAPFeatures feature id for the PHOBIUS transmembrane
	 * helix prediction.
	 */
	private static Integer getPhobiusTMHFeatureID() {
		
		Integer id = null;
		String sql = "SELECT featureid FROM interpro_features " +
		"WHERE name like \"%Phobius%\" " +
		"AND description=\"TMHelix\"";
		
		try {
			
			Statement stm = SIMAPF_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery(sql);	
			while(rs.next()) {
				id = new Integer(rs.getInt("featureid"));
			}
			rs.close();
			rs = null;
			
			stm.close();
			stm = null;
			
		} catch(Exception e) {
			System.err.println("Exception in ExtractMembraneProteins.getPhobiusTMHFeatureID(): " +e.getMessage());
			e.printStackTrace();
		}
		
		if(id == null) {
			System.err.println("\t\tERROR: Could not determine SIMAPFeatures internal feature id for PHOBIUS TMH predictions" +
					"\n\t\tFollowing MySQL Statement failed: '"+sql+"'");
			System.out.print("\n Using Manually determined Phobius feature id of 9342\n");
			id = 9342;
		} else {
			System.out.println("\t\t[INFO]: Featureid "+id.toString()+" will be used to extract PHOBIUS TMH predictions");
		}		
		return id;
	}
	
	
	/*
	 * Returns the latest SIMAP source id for completely sequenced PEDANT 
	 * genomes.
	 */
	private static Integer getSourceID() {
		
		Integer id = null;
		String sql = "SELECT sourceid FROM source " +
		"WHERE description=\"MIPS:Pedant3 complete genomes\"";
		
		try {
			
			Statement stm = SIMAP_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery(sql);	
			while(rs.next()) {
				id = new Integer(rs.getInt("sourceid"));
			}
			rs.close();
			rs = null;
			
			stm.close();
			stm = null;
			
		} catch(Exception e) {
			System.err.println("Exception in ExtractMembraneProteins.getSourceID(): " +e.getMessage());
			e.printStackTrace();
		}
		
		if(id == null) {
			System.err.println("\t\tERROR: Could not determine SIMAP internal source id for completely sequenced genomes" +
					"\n\tFollowing MySQL Statement failed: '"+sql+"'");
		} else {
			System.out.println("\t\t[INFO]: Sourceid "+id.toString()+" will be used to extract complete genomes");
		}	
		return id;
	}
		
		
	/*
	 * Read out all membrane protein sequences from table 'interpro_hits' that 
	 * have the specified minimal number of predicted transmembrane helices 
	 * (see 'MIN_NUMBER_TMH'). The sequences are returned as a BitSet
	 * containing the (unique) SIMAP sequence identifiers.  
	 */
	private static BitSet getMembraneProteins() {
		BitSet membraneProteins = null;
		try {
			
			Integer phobiusTMHFeatureID = getPhobiusTMHFeatureID();
			
			if(phobiusTMHFeatureID != null) {
				
				Statement stm1 = SIMAPF_CONNECTION.createStatement();
				
				ResultSet rs1 = stm1.executeQuery("SELECT sequenceid FROM simap.sequence " +
						"ORDER BY sequenceid DESC LIMIT 1");
				int maxSIMAPSequenceid = 0;
				while(rs1.next()) {
					maxSIMAPSequenceid = rs1.getInt("sequenceid");
				}
				rs1.close();
				rs1 = null;
				
				stm1.close();
				stm1 = null;
				
				Statement stm2 = SIMAPF_CONNECTION.createStatement();
				
				//count number of predicted TMHs for every sequence in interpro_hits
				//int[] counts - index := SIMAP sequence id, value: number of predicted TMHs
				int[] counts = new int[maxSIMAPSequenceid+1];		
				stm2.setFetchSize(Integer.MIN_VALUE);
				ResultSet rs2 = stm2.executeQuery("SELECT sequenceid FROM interpro_hits " +
						"WHERE featureid="+phobiusTMHFeatureID.intValue());
						
				while(rs2.next()) {
					int sequenceid = rs2.getInt("sequenceid");
										
					counts[sequenceid]++;
				}
				rs2.close();
				rs2 = null;	
				
				stm2.close();
				stm2 = null;
				
				membraneProteins = new BitSet();
				for(int sequenceid=0; sequenceid<counts.length; sequenceid++) {
					int count = counts[sequenceid];
					if(count >= MIN_NUMBER_TMH) {						
						membraneProteins.set(sequenceid);										
					}
				}
				counts = null;				
				
				System.out.println("\t\t[INFO]: "+membraneProteins.cardinality() + " sequences with >="+MIN_NUMBER_TMH + " TMHs found!");
			}
			
		} catch(Exception e) {
			System.err.println("Exception in ExtractMembraneProteins.getMembraneProteins(): " +e.getMessage());
			e.printStackTrace();
		}
		return membraneProteins;
	}
	
	
	/*
	 * Read out all database identifiers that specify completely
	 * sequenced genomes. Metagenomes are excluded in the first place.
	 */
	private static BitSet getCompleteGenomes() {
		BitSet genomes = null;
		try {	
			
			Integer sourceID = getSourceID();
			
			if(sourceID != null) {
				
				genomes = new BitSet();
				
				Statement stm = SIMAP_CONNECTION.createStatement();
				ResultSet rs = stm.executeQuery("SELECT databaseid,taxonomyid FROM db " +
						"WHERE NOT name LIKE \"%metagenome%\" " +
						"AND sourceid="+sourceID.intValue());
				while(rs.next()) {
					int databaseid = rs.getInt("databaseid");
					int taxonomyid = rs.getInt("taxonomyid");
					//ignore root (top level of taxonomy)
					if(taxonomyid == -1) {
						continue;
					}
					//ignore metagenomes by taxonomy id
					if(TAXIDS_METAGENOME.get(taxonomyid)) {
						continue;
					}
					
					genomes.set(databaseid);	
														
				}
				rs.close();
				rs = null;
				stm.close();
				stm = null;
				
				System.out.println("\t\t[INFO]: "+genomes.cardinality() + " complete genomes found!");
								
			}			
			
		}catch (Exception e) {
			System.err.println("Exception in ExtractMembraneProteins.getCompleteGenomes(): " +e.getMessage());
			e.printStackTrace();
		}
		return genomes;
	}


}
