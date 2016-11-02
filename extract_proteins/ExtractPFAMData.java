

/*
 * ExtractPFAMData
 * 
 * Version 1.0
 * 
 * 2014-08-13
 * 
 */

package extract_proteins;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;

//import datastructures.Domain;
import datastructures.TMS;

import utils.DBAdaptor;
import utils.IDMapping;

/**
 * 
 * @author usman
 * 
 * Extracts PFAM-A hits from SIMAPFeatures for the whole membrane protein dataset 
 * of CAMPS (database 'CAMPS').
 * The predictions are stored in the MySQL database 'CAMPS' in table 'domains_pfam'.
 * 
 * Thereby, only those annotations that fulfill the following terms are considered:
 * - hits are global (i.e. a hit represents a complete domain)  
 * 
 * PFAM hits are further subdivided into pure soluble (those without
 * predicted transmembrane regions), transmembrane (those without soluble regions
 * longer than 120 residues) and hybrid (with predicted transmembrane domains 
 * and soluble regions longer than 120 residues).
 * 
 *
 */
public class ExtractPFAMData {
	
	private static final Connection SIMAP_CONNECTION = DBAdaptor.getConnection("simap");
	
	private static final Connection SIMAPF_CONNECTION1 = DBAdaptor.getConnection("simapfeatures");
	
	private static final Connection SIMAPF_CONNECTION2 = DBAdaptor.getConnection("simapfeatures");
	
	//CAMPS connection for extracting data
	private static final Connection CAMPS_CONNECTION_EXPORT = DBAdaptor.getConnection("camps4");
	
	//CAMPS connection for inserting data
	private static final Connection CAMPS_CONNECTION_IMPORT = DBAdaptor.getConnection("camps4");
	
	//number of records that should be inserted simultaneously
	private static final int BATCH_SIZE = 100;
	
	
		
	
	/**
	 * Runs the whole program.
	 */
	public static void run() {
		try {
			
						
			PreparedStatement pstm = CAMPS_CONNECTION_IMPORT.prepareStatement(
					"INSERT INTO domains_pfam " +
					"(sequenceid," +
					"accession," +
					"description," +
					"begin," +
					"end," +
					"length," +					
					"evalue) " +
					"VALUES " +
					"(?,?,?,?,?,?,?)");
			
						
			System.out.println("\t\t[INFO]: Perform mapping between SIMAP and CAMPS sequence ids.");
			
			Date a = Calendar.getInstance().getTime();
			System.out.println("\n The start time for Mapping between SIMAP and CAMPS sequence ids: " + a);
			
			
			IDMapping idm = new IDMapping();			
			BitSet simapSequenceIDs = idm.getSimapSequenceIDs();
			int[] mapping = idm.getMapping();  //index: SIMAP id, value: CAMPS id
			
			int pfamDatabaseid = getLatestPFAMDatabaseID().intValue();
			
			
			a = Calendar.getInstance().getTime();
			System.out.println("\n The end time for Mapping between SIMAP and CAMPS sequence ids: " + a);
			
			Statement stm = SIMAPF_CONNECTION2.createStatement();
			stm.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = stm.executeQuery(
					"SELECT h.sequenceid, h.begin_position, h.end_position, h.evalue, f.name, f.description " +
					"FROM interpro_hits h, interpro_features f " +
					"WHERE h.featureid=f.featureid " +
					"AND f.databaseid="+pfamDatabaseid);
			
			int batchCounter = 0;
			
			a = Calendar.getInstance().getTime();
			System.out.println("\n The start time for Populating domains_PFAM: " + a);
			
			while(rs.next()) {
				int simapSequenceid = rs.getInt("sequenceid");				
				int beginPosition = rs.getInt("begin_position");
				int endPosition = rs.getInt("end_position");
				int length = (endPosition - beginPosition) +1;
				double evalue = rs.getDouble("evalue");
				
				String accession = rs.getString("name");
				String description = rs.getString("description");
				
				if(simapSequenceIDs.get(simapSequenceid)) {
					
					int campsSequenceid = mapping[simapSequenceid];
					
					batchCounter++;
					
					pstm.setInt(1, campsSequenceid);
					pstm.setString(2, accession);
					pstm.setString(3, description);
					pstm.setInt(4, beginPosition);
					pstm.setInt(5, endPosition);
					pstm.setInt(6, length);					
					pstm.setDouble(7, evalue);		
					
					pstm.addBatch();
					
					if(batchCounter % BATCH_SIZE == 0) {								
						pstm.executeBatch();
						pstm.clearBatch();
					}
				}
			}
			rs.close();
			
			a = Calendar.getInstance().getTime();
			System.out.println("\n The end time for Populating domains_PFAM: " + a);
			
			

			if(pstm != null) {
				pstm.executeBatch();
				pstm.clearBatch();	
				pstm.close();
			}
			
			a = Calendar.getInstance().getTime();
			System.out.println("\n The start time for updating domains_PFAM subtype and id: " + a);
			
			
			System.out.println("\t\t[INFO]: Update table domains_pfam (subtype is being added).");
			//add subtype to each domain entry
			updateTableDomains();
			
			a = Calendar.getInstance().getTime();
			System.out.println("\n The end time for updating domains_PFAM subtype and id: " + a);
			
			
			
			System.out.println("\t\t[INFO]: Update table domains_pfam (number of residues in TMH is being added).");
			a = Calendar.getInstance().getTime();
			System.out.println("\n The start time for updating domains_PFAM number of residues in TMH: " + a);
			
			//add number of domain residues that are within transmembrane regions
			addNumberOfResiduesInTMH();
			
			a = Calendar.getInstance().getTime();
			System.out.println("\n The end time for updating domains_PFAM number of residues in TMH: " + a);
		
		} catch(Exception e) {
			System.err.println("Exception in ExtractPFAMData.run(): " +e.getMessage());
			e.printStackTrace();
		
		} finally {
			if (SIMAP_CONNECTION != null) {
				try {
					SIMAP_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			if (SIMAPF_CONNECTION1 != null) {
				try {
					SIMAPF_CONNECTION1.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			if (SIMAPF_CONNECTION2 != null) {
				try {
					SIMAPF_CONNECTION2.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			if (CAMPS_CONNECTION_EXPORT != null) {
				try {
					CAMPS_CONNECTION_EXPORT.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			if (CAMPS_CONNECTION_IMPORT != null) {
				try {
					CAMPS_CONNECTION_IMPORT.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}
	
	
	
	/*
	 * Returns the latest SIMAPFeatures database id for HMMPfam.
	 * Doesnt seem to be a very efficient way to return latest SIMAPFeatures
	 */
	private static Integer getLatestPFAMDatabaseID() {
		
		Integer id = null;
		String sql = "SELECT databaseid FROM interpro_databases " +
		"WHERE name=\"Pfam\"";
		
		try {
			
			Statement stm = SIMAPF_CONNECTION1.createStatement();
			ResultSet rs = stm.executeQuery(sql);
			while(rs.next()) {
				id = new Integer(rs.getInt("databaseid"));
			}
			rs.close();
			rs = null;
			
			stm.close();
			stm = null;
			
		} catch(Exception e) {
			System.err.println("Exception in ExtractPFAMData.getLatestPFAMDatabaseID(): " +e.getMessage());
			e.printStackTrace();
		}
		
		if(id == null) {
			System.err.println("\t\tERROR: Could not determine latest SIMAPFeatures database id for HMMPfam" +
					"\n\tFollowing MySQL Statement failed: '"+sql+"'");	
		}else {
			System.out.println("\t\t[INFO]: Databaseid "+id.toString()+" will be used to extract PFAM hits");
		}
		return id;
	}
		
		
	/*
	 * Returns true if the new domain is overlapping by more than 10 residues 
	 * with one of domains from the specified array. Thereby, an overlap of 
	 * 10 residues is allowed.
	 */
//	private static boolean overlapping(ArrayList<Domain> domains, int new_domain_begin, int new_domain_end) {
//		boolean overlap = false;
//		for(Domain domain:domains) {
//			int domain_begin = domain.get_begin_position();
//			int domain_end = domain.get_end_position();
//			
//			if ((new_domain_begin >= domain_begin && new_domain_end <= domain_end) || 
//					(domain_begin >= new_domain_begin && domain_end <= new_domain_end)) {	//subelement relationship
//				overlap = true;
//			}
//			else if (!(new_domain_end - domain_begin < 10) && 
//					!(domain_end - new_domain_begin < 10)) {		//partial overlap
//				overlap = true;
//			}						
//		}
//		return overlap;
//	}
	
	/*
	 * Defines the subtype of the new domain. 
	 * 
	 * Soluble - if domain has no predicted transmembrane regions
	 * Transmembrane - if domain has no soluble regions longer than 120 residues
	 * Hybrid - if domain has predicted transmembrane regions AND soluble regions
	 *          longer than 120 residues
	 * 
	 * Thereby, an overlap of 10 residues is allowed.
	 */
//	private static String defineDomainSubtype(ArrayList<TMS> tms_array, int new_domain_begin, int new_domain_end) {
//		String subtype =null;
//		try {
//			
//			int longest_hydrophilic_loop = 0;
//
//	        boolean first_tms = true;
//	        int last_tms = -1; 
//
//	        int tms_id = 0;
//	        for(TMS tms:tms_array){
//	        	int tms_begin = tms.get_start();
//	        	int tms_end = tms.get_end();
//	            if ((tms_end - new_domain_begin < 10) || (new_domain_end - tms_begin < 10)) {
//	            	continue;
//	            }
//	            int new_hydrophilic_loop;
//	            if (first_tms){
//	                new_hydrophilic_loop = tms_begin - new_domain_begin;			                    
//	                last_tms = tms_id;  			                
//	                first_tms = false;
//	            } 
//	            else {
//	                new_hydrophilic_loop = tms_begin - tms_array.get(last_tms).get_end();
//	            }
//	               
//	            if(new_hydrophilic_loop > longest_hydrophilic_loop) {
//	            	longest_hydrophilic_loop = new_hydrophilic_loop;
//	            }	
//	            last_tms = tms_id;
//	            
//	            tms_id++;
//	        }
//	        
//	        if (last_tms > -1){
//	            int new_hydrophilic_loop = new_domain_end - tms_array.get(last_tms).get_end();
//	            if(new_hydrophilic_loop > longest_hydrophilic_loop) {
//	            	longest_hydrophilic_loop = new_hydrophilic_loop;
//	            }			            
//	        }
//	        
//	        if (longest_hydrophilic_loop == 0){			    
//	            longest_hydrophilic_loop = new_domain_end - new_domain_begin + 1;
//	            subtype = "soluble";			            
//	        }
//	        else if (longest_hydrophilic_loop >= 120){
//	            subtype = "hybrid";
//
//	        }
//	        else{
//	            subtype = "transmembrane";
//	        }
//			
//		}
//		catch(Exception e) {
//			e.printStackTrace();
//		}
//		return subtype;
//	}
	
	private static String defineDomainSubtype(ArrayList<TMS> tms_array, int domain_begin, int domain_end) {
		String subtype = null;
		try {
			
			if(tms_array != null) {
				
				int numTMS = tms_array.size();
				
				int end_last_tms = tms_array.get(numTMS-1).get_end();
				int length = Math.max(end_last_tms,domain_end);
				
				
				char[] tms_prediction = new char[length];
				//initial setting (set all position to 'L' for loop region)
				for(int i=0;i<length;i++) {
					tms_prediction[i] = 'L';
				}
				//reset prediction
				for(TMS tms: tms_array) {
					int start = tms.get_start();
					int end = tms.get_end();
										
					for(int j=start-1; j<end; j++) {
						tms_prediction[j] = 'M';						
					}
				}					
								
				String topology = "";
				
				char last_state = '?';
				int count = 0;
				for(int i=domain_begin-1; i<domain_end;i++) {
					
					char current_state = tms_prediction[i];
					
					if(last_state == current_state) {
						count++;
					}
					else {
						
						if(last_state != '?') {
							topology+=","+last_state+":"+count;
							count = 1;
						}
						else {
							count = 1;
						}
					}
					
					last_state = current_state;
				}
				//add last states
				topology+=","+last_state+":"+count;
				topology = topology.substring(1);
				
				//System.out.println(topology);
				
				String[] tmp = topology.split(",");
				String begin_topology = tmp[0];
				String end_topology = tmp[tmp.length-1];
				
				int index_begin_topology = 0;
				int index_end_topology = tmp.length-1;
				
				//ignore overlaps of 10 residues
				if(begin_topology.startsWith("M")) {
					int num_states = Integer.parseInt(begin_topology.split(":")[1]);
					
					if(num_states <= 10) {
						index_begin_topology = 1;
					}
				}
				if(end_topology.startsWith("M")) {
					int num_states = Integer.parseInt(end_topology.split(":")[1]);
					
					if(num_states <= 10) {
						index_end_topology = tmp.length-2;
					}
				}
				
				topology = ""; //reset
				for(int i = index_begin_topology; i<=index_end_topology; i++) {
					topology += "," +tmp[i];
				}
				if(topology.equals("")) { //after ignoring small overlaps, nothing remained (domain is too short!)
					subtype = "NA";
					
				}
				else {
					topology = topology.substring(1);
					//System.out.println(topology);
					
					//
					int count_transmembrane_states = 0;
					int longest_loop = 0;
					tmp = topology.split(",");
					for(String s: tmp) {
						
						String state = s.split(":")[0];
						int num_states = Integer.parseInt(s.split(":")[1]);
						
						if(state.equals("M")) {
							count_transmembrane_states += num_states;
						}
						else if(state.equals("L")) {
							if(num_states > longest_loop) {
								longest_loop = num_states;
							}
						}
					}
					
					if(count_transmembrane_states == 0) {
						subtype = "soluble";
					}
					else if(count_transmembrane_states > 0 && longest_loop < 120) {
						subtype = "transmembrane";
					}
					else if(count_transmembrane_states > 0 && longest_loop >= 120) {
						subtype = "hybrid";
					}
				}
				
			}		
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return subtype;
	}
	
	private static void updateTableDomains() {
		try {
			
			PreparedStatement pstm = CAMPS_CONNECTION_IMPORT.prepareStatement(
					"UPDATE domains_pfam SET subtype=? WHERE id=?");
			
			int start = 0;
			int blockSize = 10000;
			
			Statement stm = CAMPS_CONNECTION_EXPORT.createStatement();
			
			boolean reachedEndOfTable = false;
			
			while(!reachedEndOfTable) {
				
				Hashtable<Integer,String> rowid2domainInfo = new Hashtable<Integer,String>();
				BitSet sequenceids = new BitSet();
				
				int countRows = 0;
				
				ResultSet rs1 = stm.executeQuery("SELECT id, sequenceid, begin, end FROM domains_pfam limit "+start+","+blockSize);
				while(rs1.next()) {
					countRows++;
					
					int id = rs1.getInt("id");
					int sequenceid = rs1.getInt("sequenceid");
					int begin = rs1.getInt("begin");
					int end = rs1.getInt("end");
					
					String domainInfo = sequenceid+"#"+begin+"-"+end;
					
					rowid2domainInfo.put(Integer.valueOf(id),domainInfo);
					sequenceids.set(sequenceid);
					
				}
				rs1.close();
				
				if(countRows == 0) {
					reachedEndOfTable = true;
					continue;
				}
				
				
				String idCollector = "";
				for(int sequenceid = sequenceids.nextSetBit(0); sequenceid>=0; sequenceid = sequenceids.nextSetBit(sequenceid+1)) {
					idCollector += "," +sequenceid;
				}
				
				idCollector = idCollector.substring(1);
				
				
				Hashtable<Integer, ArrayList<TMS>> sequenceid2tms = new Hashtable<Integer, ArrayList<TMS>>();
				ResultSet rs2 = stm.executeQuery("SELECT sequenceid,begin,end FROM tms WHERE sequenceid in ("+idCollector+") ORDER BY tms_id");
				while(rs2.next()) {
					
					int sequenceid = rs2.getInt("sequenceid");
					int tmsBegin = rs2.getInt("begin");
					int tmsEnd = rs2.getInt("end");
					
					ArrayList<TMS> tmsList = new ArrayList<TMS>();
					if(sequenceid2tms.containsKey(Integer.valueOf(sequenceid))) {
						tmsList = sequenceid2tms.get(Integer.valueOf(sequenceid));
					}
					
					tmsList.add(new TMS(tmsBegin,tmsEnd));
					sequenceid2tms.put(Integer.valueOf(sequenceid), tmsList);
				}
				rs2.close();
				
				Enumeration<Integer> rowids = rowid2domainInfo.keys();
				while(rowids.hasMoreElements()) {
					Integer rowid = rowids.nextElement();
					String domainInfo = rowid2domainInfo.get(rowid);
					
					int sequenceid = Integer.valueOf(domainInfo.split("#")[0]);
					int domainBegin = Integer.valueOf(domainInfo.split("#")[1].split("-")[0]);
					int domainEnd = Integer.valueOf(domainInfo.split("#")[1].split("-")[1]);
					
					ArrayList<TMS> tmsList = sequenceid2tms.get(Integer.valueOf(sequenceid));
										
					//divide domains in pure transmembrane, hybrid and pure soluble (10 residues boundaries allowed)				
			        String subtype = defineDomainSubtype(tmsList, domainBegin, domainEnd);
			        
			        pstm.setString(1, subtype);
			        pstm.setInt(2, rowid.intValue());
			        pstm.executeUpdate();
				}	
				
				
				start += blockSize;							
				
			}	
			
			stm.close();
			pstm.close();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	
	private static void addNumberOfResiduesInTMH() {
		try {
			
			Statement stm = CAMPS_CONNECTION_IMPORT.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery("SELECT id,sequenceid,begin,end,length,residues_in_tmh,perc_residues_in_tmh,covered_tmh FROM domains_pfam FOR UPDATE"); 
			
			PreparedStatement pstm = CAMPS_CONNECTION_EXPORT.prepareStatement("SELECT begin,end FROM tms WHERE sequenceid=?");
			
						
			while (rs.next()) {
				
				int residuesInTMH = 0;
				int coveredTMH = 0;
				
				int sequenceid = rs.getInt("sequenceid");
				int domainBegin = rs.getInt("begin");
				int domainEnd = rs.getInt("end");
				int domainLength = rs.getInt("length");
				
				pstm.setInt(1, sequenceid);
				ResultSet rs2 = pstm.executeQuery();				
				while(rs2.next()) {
					boolean withinTMH = false;
					int tmsBegin = rs2.getInt("begin");
					int tmsEnd = rs2.getInt("end");
					
					for(int i=tmsBegin; i<=tmsEnd; i++) {
						if(i >=domainBegin && i <= domainEnd) {
							residuesInTMH++;
							withinTMH = true;
						}
					}
					
					if(withinTMH) {
						coveredTMH++;
					}
				}
				rs2.close();
				
				double percResiduesInTMH = 100 * (residuesInTMH/((double) domainLength));
				BigDecimal bd = new BigDecimal(percResiduesInTMH);
				bd = bd.setScale(2, BigDecimal.ROUND_HALF_UP);
				percResiduesInTMH = bd.doubleValue();
				
				rs.updateInt("residues_in_tmh",residuesInTMH);
				rs.updateDouble("perc_residues_in_tmh",percResiduesInTMH);
				rs.updateInt("covered_tmh",coveredTMH);
				rs.updateRow();	
											 
			}
			
			rs.close();			
			stm.close();
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
