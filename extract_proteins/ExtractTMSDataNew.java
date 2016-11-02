
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
public class ExtractTMSDataNew {
	
	private static final Connection SIMAP_CONNECTION = DBAdaptor.getConnection("simap");
	
	private static final Connection SIMAPF_CONNECTION = DBAdaptor.getConnection("simapfeatures");
	
	private static final Connection CAMPS_CONNECTION_IMPORT = DBAdaptor.getConnection("camps4"); // changed from test_camps3
	
	//private static final Connection CAMPS_CONNECTION_EXPORT = DBAdaptor.getConnection("camps4");
	
	
	//number of records that should be inserted simultaneously
	private static final int BATCH_SIZE = 100;
	
	public static void make_mapping(){
		
		try{
		Date a = Calendar.getInstance().getTime();

		PreparedStatement pstm_map = CAMPS_CONNECTION_IMPORT.prepareStatement(
				"INSERT INTO mapping_camps" +
				"(camps_id, simap_id, status) VALUES " +
				"(?,?,?)");	//added md5 and a ? in values
		
		System.out.println("\n\t\t[INFO]: Mapping skipped ;) ");
		
		a = Calendar.getInstance().getTime();
		System.out.println("\n The start time for Mapping between SIMAP and CAMPS sequence ids: " + a);
		
		IDMapping idm = new IDMapping();
		
		int[] mapping = idm.getMapping();  //index: SIMAP id, value: CAMPS id

		//countTMS: index == SIMAP sequence id, value == number of already found TMS
		
		a = Calendar.getInstance().getTime();
		System.out.println("\n The end time for Mapping between SIMAP and CAMPS sequence ids: " + a);
		
		a = Calendar.getInstance().getTime();
		System.out.println("\n Now populating temp table: " + a);
		
		int batchCounter1 =0;
		for (int i =0; i<=mapping.length;i++){	//length of mapping
			
			if (mapping[i] != 0){
				//index simap id------i
				//value camps id------mapping[i]
				
				pstm_map.setInt(1,mapping[i]);	//camps_id						
				pstm_map.setInt(2,i);	//simap_id
				pstm_map.setInt(3,0);	//status
				
				pstm_map.addBatch();
				batchCounter1++;
			}
			
			if(batchCounter1 % BATCH_SIZE == 0) {								
				pstm_map.executeBatch();
				pstm_map.clearBatch();
			}
			
		}
		a = Calendar.getInstance().getTime();
		System.out.println("\n Populating table complete: " + a);

	}
		catch(Exception e) {
			System.err.println("Exception in ExtractTMSData.make_mapping(): " +e.getMessage());
			e.printStackTrace();
		}
	}
	
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
					"INSERT INTO tms2" +
					"(sequenceid, tms_id, begin, end, length, md5) VALUES " +
					"(?,?,?,?,?,?)");	//added md5 and a ? in values
			
			PreparedStatement pstm2 = CAMPS_CONNECTION_IMPORT.prepareStatement(
					"INSERT INTO elements2 " +
					"(sequenceid, type, method, begin, end, length, md5) VALUES " +
					"(?,?,?,?,?,?,?)");  //added md5 and a ? in values
			
			PreparedStatement pstm_read = CAMPS_CONNECTION_IMPORT.prepareStatement("SELECT md5 FROM sequences2 WHERE sequenceid=?");
			
			PreparedStatement pstm_map = CAMPS_CONNECTION_IMPORT.prepareStatement(
					"INSERT INTO mapping_camps2" +
					"(camps_id, simap_id, status) VALUES " +
					"(?,?,?)");	//added md5 and a ? in values
			
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

			
			a = Calendar.getInstance().getTime();
			System.out.println("\n The end time for Mapping between SIMAP and CAMPS sequence ids: " + a);

		
			for (int i =0; i<=mapping.length;i++){	//length of mapping
				
				if (mapping[i] != 0){
					//index simap id------i
					//value camps id------mapping[i]
					
					pstm_map.setInt(1,mapping[i]);	//camps_id						
					pstm_map.setInt(2,i);	//simap_id
					pstm_map.setInt(3,0);	//status
					
					pstm_map.addBatch();
					batchCounter1++;
				}
				
				if(batchCounter1 % BATCH_SIZE == 0) {								
					pstm_map.executeBatch();
					pstm_map.clearBatch();
				}
				
			}
			a = Calendar.getInstance().getTime();
			System.out.println("\n Populating table complete: " + a);

			
			//System.out.println("\t\t[INFO]: Parse complete interpro_hits table.");
			
			
			a = Calendar.getInstance().getTime();
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
					pstm_read.setInt(1, mappedCampsid);
					ResultSet rs_read = pstm_read.executeQuery();
					String md5 = "";
					if(rs_read.next()){
						md5 = rs_read.getString("md5");
					}
					
					pstm1.setInt(1,mappedCampsid);						
					pstm1.setInt(2,currentCountTMS);
					pstm1.setInt(3,begin_position);
					pstm1.setInt(4,end_position);
					pstm1.setInt(5,length);
					pstm1.setString(6, md5);
										
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
					
					pstm_read.setInt(1, mappedCampsid);
					ResultSet rs_read = pstm_read.executeQuery();
					String md5 = "";
					if(rs_read.next()){
						md5 = rs_read.getString("md5");
					}
					
					pstm2.setInt(1,mappedCampsid);	
					pstm2.setString(2, "signal_peptide");
					pstm2.setString(3, "phobius");							
					pstm2.setInt(4,begin_position);
					pstm2.setInt(5,end_position);
					pstm2.setInt(6,length);
					pstm2.setString(7, md5);
						
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








/*
public class ExtractTMSDataNew {
	

	
	private static final Connection CAMPS_CONNECTION_IMPORT = DBAdaptor.getConnection("camps4"); // changed from test_camps3
	
	/**
	 * Runs the whole program.
	 
	public static void run() {
		
		try {			
			
			Date a = Calendar.getInstance().getTime();
			System.out.println("\n The start time for geting Phobius TMHFeatureID and PhobiusSPFeature ID: " + a);
			a = Calendar.getInstance().getTime();
			System.out.println("\n The end time for geting Phobius TMHFeatureID and PhobiusSPFeature ID: " + a);

			
			PreparedStatement pstm1 = CAMPS_CONNECTION_IMPORT.prepareStatement(
					"INSERT INTO tms" +
					"(sequenceid, tms_id, begin, end, length, md5) VALUES " +
					"(?,?,?,?,?,?)");	//added md5 and a ? in values
			
			PreparedStatement pstm_read = CAMPS_CONNECTION_IMPORT.prepareStatement(
					"SELECT sequenceid, md5 FROM sequences");  
			
			System.out.println("\n\t\t[INFO]: Mapping skipped ;) ");
			
			a = Calendar.getInstance().getTime();
	
			System.out.println("\n The start time for parsing interpro hits table to populate table 'tms' and 'elements': " + a);

			//***************STARTING MY OWN EXTRACTION WORK**************
			// Get md5 from sequences and one by one get the parsing done and extract begin end and write in camps tms
			
			
			ResultSet rs_read = pstm_read.executeQuery();
			Check_hit_tm obj ;
			System.out.println("\n");
			int count = 0;
			while (rs_read.next()){
				int id = rs_read.getInt("sequenceid");
				String md = rs_read.getString("md5");
				obj = new Check_hit_tm();
				
				if(obj.run(md)){
					if (obj.tmsCount>0){
						for (int i = 0; i<obj.tmsCount-1; i++){
							
							pstm1.setInt(1,id);	//sequence_sequenceid					
							pstm1.setInt(2,i+1); //TMS no
							pstm1.setInt(3,obj.start_[i]); 
							pstm1.setInt(4,obj.end_[i]);
							pstm1.setInt(5,obj.end_[i] - obj.start_[i]); //length of this tms
							pstm1.setString(6,md);
							
							pstm1.addBatch();
						}
						pstm1.executeBatch();
						//System.out.println(".");
						pstm1.clearBatch();
					}
				}
				else{
					pstm1.setInt(1,id);	//sequence_sequenceid					
					pstm1.setInt(2,0); //TMS no
					pstm1.setInt(3,0); 
					pstm1.setInt(4,0);
					pstm1.setInt(5,0); //length of this tms
					pstm1.setString(6,md);
					System.out.println("\n"+md+"\n");
				}
				count ++;
				if (count % 1000 == 0){
					System.out.print(count + "\n");
				}
				
			}
			
			rs_read.close();
			
			a = Calendar.getInstance().getTime();
			System.out.println("\n The end time tms table filling': " + a);
			
			
			if(pstm1 != null) {
				pstm1.executeBatch();
				pstm1.clearBatch();	
				pstm1.close();
			}
			
		} catch(Exception e) {
			System.err.println("Exception in ExtractTMSData.run(): " +e.getMessage());
			e.printStackTrace();
		} finally {
	
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
	 * Returns the SIMAPFeatures feature id for the PHOBIUS transmembrane
	 * helix prediction.
	 
	
}
*/