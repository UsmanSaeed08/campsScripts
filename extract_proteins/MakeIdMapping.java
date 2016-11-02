package extract_proteins;

//import java.io.File;
//import java.io.FileWriter;
//import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;
//import java.util.Hashtable;

//import datastructures.TMS;

import utils.DBAdaptor;
import utils.IDMapping;

public class MakeIdMapping {

	/**
	 * @param args
	 */
	
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("test_camps3");
	
	
	public static void run() throws SQLException {
		// TODO Auto-generated method stub
		//from
		
		
		PreparedStatement pstm_insert = CAMPS_CONNECTION.prepareStatement(
				"INSERT INTO mapped_tms " +
				"(idx," +
				"simapId," +
				"campsId," +
				"tmsNo," +
				"begin," +
				"end," +
				"length)" +
				"VALUES " +
				"(?,?,?,?,?,?,?)");
		
		PreparedStatement pstm_get_tms = CAMPS_CONNECTION.prepareStatement("SELECT begin,end,length FROM tms WHERE sequences_sequenceid=?");
		
		Date a = Calendar.getInstance().getTime();
		System.out.println("\n Making Id mapping table starts at: " + a + "\n");
					
		System.out.println("\t\t[INFO]: Perform mapping between SIMAP and CAMPS sequence ids.");
			
		IDMapping idm = new IDMapping();			
		//BitSet selected_sequences = idm.getSimapSequenceIDs();
		int[] mapping = idm.getMapping();  //index: SIMAP id, value: CAMPS id
		
		// run a loop for mapping array to populate the simapid and campsid
		
		a = Calendar.getInstance().getTime();
		System.out.println("\n ID Mapping complete now Populate mapped_tms: " + a + "\n");
		
		int count =0; //to count number of un mapped queries
		//int iterator =0;
		//int index =0;
		int i =0;
		for (int index =0;index<=mapping.length-1; index++){
			
			if(mapping[index] == 0) {  //no valid mapping between SIMAP and CAMPS
				count = count + 1;
				continue;
			}
				
			//ArrayList<TMS> tms_arr_query = new ArrayList<TMS>();		//TMS is a self created data structure
				
			pstm_get_tms.setInt(1,mapping[index]); //pick begin and end from tms table for this sequenceid/which is campsid 
			ResultSet rs = pstm_get_tms.executeQuery();
			int tmsNo =0;
			while(rs.next()) {
				tmsNo++;
				
				int begin = rs.getInt("begin");
				int end = rs.getInt("end");
				int length = rs.getInt("length");
				
				pstm_insert.setInt(1, i); //idx the index 1---n
				pstm_insert.setInt(2, index);//simapid
				pstm_insert.setInt(3, mapping[index]);//campsid
				pstm_insert.setInt(4, tmsNo);//which number of tms is this of the membrane protein
				pstm_insert.setInt(5, begin); //begin
				pstm_insert.setInt(6, end); //end
				pstm_insert.setInt(7, length); // length
				
				pstm_insert.executeUpdate();
				pstm_insert.clearParameters();
				i++;
			}
			
			rs.close();
			rs = null;
		}
		
		System.out.print("\n The number of un mapped queries are " + count);	
		a = Calendar.getInstance().getTime();
		System.out.println("\n Population Complete: " + a + "\n");

	}

}
