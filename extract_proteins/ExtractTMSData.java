
/*
 * ExtractTMSData
 * 
 * Version 2.0
 * 
 * 2009-08-25
 * 
 * changes in 2.0:
 * - mapping of CAMPS to SIMAP sequence ids implemented by using incremental
 * extraction of SIMAP ids (using "SELECT ... IN ()" queries) 
 * 
 */

package extract_proteins;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;

import utils.DBAdaptor;
import utils.IDMapping;

/**
 * 
 * @author Usman
 * 
 * Extracts the PHOBIUS predictions (both transmembrane helix and signal peptide
 * predictions) from SIMAPFeatures for the whole membrane protein dataset of 
 * CAMPS (database 'CAMPS').
 * The predictions are stored in the MySQL database 'CAMPS' in table 'tms' and
 * 'elements'.
 *  
 *
 */
public class ExtractTMSData {
	
	private static final Connection SIMAP_CONNECTION = DBAdaptor.getConnection("simap");
	
	private static final Connection SIMAPF_CONNECTION = DBAdaptor.getConnection("simapfeatures");
	
	private static final Connection CAMPS_CONNECTION_IMPORT = DBAdaptor.getConnection("camps4"); // changed from test_camps3
	
	//private static final Connection CAMPS_CONNECTION_EXPORT = DBAdaptor.getConnection("camps4");
	
	
	//number of records that should be inserted simultaneously
	private static final int BATCH_SIZE = 100;
	
	
	/**
	 * Runs the whole program.
	 */
	public static void run() {
		
		try {			
			
			Date a = Calendar.getInstance().getTime();
			System.out.println("\n The start time for geting Phobius TMHFeatureID and PhobiusSPFeature ID: " + a);

			int phobiusTMHFeatureID = getPhobiusTMHFeatureID().intValue();
			int phobiusSPFeatureID = getPhobiusSPFeatureID().intValue();
			
			
			a = Calendar.getInstance().getTime();
			System.out.println("\n The end time for geting Phobius TMHFeatureID and PhobiusSPFeature ID: " + a);

			
			PreparedStatement pstm1 = CAMPS_CONNECTION_IMPORT.prepareStatement(
					"INSERT INTO tms" +
					"(sequences_sequenceid, tms_id, begin, end, length) VALUES " +
					"(?,?,?,?,?)");	//added md5 and a ? in values
			
			PreparedStatement pstm2 = CAMPS_CONNECTION_IMPORT.prepareStatement(
					"INSERT INTO elements " +
					"(sequences_sequenceid, type, method, begin, end, length) VALUES " +
					"(?,?,?,?,?,?)");  //added md5 and a ? in values
			
			System.out.println("\n\t\t[INFO]: Mapping skipped ;) ");
			
			a = Calendar.getInstance().getTime();
			System.out.println("\n The start time for Mapping between SIMAP and CAMPS sequence ids: " + a);
			
			IDMapping idm = new IDMapping();
			int maxSimapSequenceid = idm.getMaxSimapSequenceid();
			BitSet simapSequenceIDs = idm.getSimapSequenceIDs();
			int[] mapping = idm.getMapping();  //index: SIMAP id, value: CAMPS id

			//countTMS: index == SIMAP sequence id, value == number of already found TMS
			int[] countTMS = new int[maxSimapSequenceid+1];
			
			int batchCounter1 = 0;
			int batchCounter2 = 0;

			
			//a = Calendar.getInstance().getTime();
			//System.out.println("\n The end time for Mapping between SIMAP and CAMPS sequence ids: " + a);

			
			
			//System.out.println("\t\t[INFO]: Parse complete interpro_hits table.");
			
			
			//a = Calendar.getInstance().getTime();
			System.out.println("\n The start time for parsing interpro hits table to populate table 'tms' and 'elements': " + a);

			
			Statement stm = SIMAPF_CONNECTION.createStatement();
			stm.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = stm.executeQuery(
					"SELECT sequenceid,featureid,begin_position,end_position " +
					"FROM interpro_hits " +
					"WHERE featureid in ("+phobiusTMHFeatureID+","+phobiusSPFeatureID+") order by begin_position");
			
			int count_number_tm_lesserThree = 0; //to count number of tm lesser than three			
			
			while(rs.next()) {
				int sid = rs.getInt("sequenceid");
				int featureid = rs.getInt("featureid");
				int begin_position = rs.getInt("begin_position");
				int end_position = rs.getInt("end_position");
				
				int length = (end_position - begin_position) + 1;
				
				if(!simapSequenceIDs.get(sid)) {
					continue;
				}
				
				int mappedCampsid = mapping[sid];						
				
				if(featureid == phobiusTMHFeatureID) {
					batchCounter1++;
					
					int currentCountTMS = countTMS[sid]+1;
					countTMS[sid]++;
					//add to table tms
					pstm1.setInt(1,mappedCampsid);						
					pstm1.setInt(2,currentCountTMS);
					pstm1.setInt(3,begin_position);
					pstm1.setInt(4,end_position);
					pstm1.setInt(5,length);
										
					pstm1.addBatch();
					
					if (countTMS[sid] >= 1 && countTMS[sid] < 3){
						count_number_tm_lesserThree++;
						//System.out.print("\n FOUND: sequence with TM "+ mappedCampsid +" with no of tm "+ countTMS[sid]+ "\n");
					}
					// modified code by adding if to see if there is any sequence with >1 <3 tm
					if(batchCounter1 % BATCH_SIZE == 0) {								
						pstm1.executeBatch();
						pstm1.clearBatch();
					}
				}
				else if(featureid == phobiusSPFeatureID) {
					batchCounter2++;
						
					pstm2.setInt(1,mappedCampsid);	
					pstm2.setString(2, "signal_peptide");
					pstm2.setString(3, "phobius");							
					pstm2.setInt(4,begin_position);
					pstm2.setInt(5,end_position);
					pstm2.setInt(6,length);
						
					pstm2.addBatch();
						
					if(batchCounter2 % BATCH_SIZE == 0) {								
						pstm2.executeBatch();
						pstm2.clearBatch();
					}							
				}
			}
			
			System.out.print("\n Number of TM >1 and < 3 are " +count_number_tm_lesserThree+ "\n");
			rs.close();
			
			a = Calendar.getInstance().getTime();
			System.out.println("\n The end time for parsing interpro hits table to populate table 'tms' and 'elements': " + a);
			
			
			if(pstm1 != null) {
				pstm1.executeBatch();
				pstm1.clearBatch();	
				pstm1.close();
			}
			if(pstm2 != null) {
				pstm2.executeBatch();
				pstm2.clearBatch();	
				pstm2.close();
			}		
			
			
		} catch(Exception e) {
			System.err.println("Exception in ExtractTMSData.run(): " +e.getMessage());
			e.printStackTrace();
		} finally {
			if (SIMAP_CONNECTION != null) {
				try {
					SIMAP_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			if (SIMAPF_CONNECTION != null) {
				try {
					SIMAPF_CONNECTION.close();					
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
			/*if (CAMPS_CONNECTION_EXPORT != null) {
				try {
					CAMPS_CONNECTION_EXPORT.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}*/
		}	
	}
	
	
	/*
	 * Returns the SIMAPFeatures feature id for the PHOBIUS transmembrane
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
			System.err.println("Exception in ExtractTMSData.getPhobiusTMHFeatureID(): " +e.getMessage());
			e.printStackTrace();
		}
		
		if(id == null) {
			System.err.println("\t\tERROR: Could not determine SIMAPFeatures internal feature id for PHOBIUS TMH predictions" +
					"\n\tFollowing MySQL Statement failed: '"+sql+"'");
		}else {
			System.out.println("\t\t[INFO]: Featureid "+id.toString()+" will be used to extract PHOBIUS TMH predictions");
		}
		return id;
	}
	
	
	/*
	 * Returns the SIMAPFeatures feature id for the PHOBIUS signal
	 * peptide prediction.
	 */
	private static Integer getPhobiusSPFeatureID() {
		
		Integer id = null;
		String sql = "SELECT featureid FROM interpro_features " +
		"WHERE name like \"%Phobius%\" " +
		"AND description=\"Signal peptide\"";
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
			System.err.println("Exception in ExtractTMSData.getPhobiusSPFeatureID(): " +e.getMessage());
			e.printStackTrace();
		}
		
		if(id == null) {
			System.err.println("\t\tERROR: Could not determine SIMAPFeatures internal feature id for PHOBIUS signal peptide predictions" +
					"\n\tFollowing MySQL Statement failed: '"+sql+"'");			
		}else {
			System.out.println("\t\t[INFO]: Featureid "+id.toString()+" will be used to extract PHOBIUS signal peptide predictions");
		}
		return id;
	}
	
	
}
