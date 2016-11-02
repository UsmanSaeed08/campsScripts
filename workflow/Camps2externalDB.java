package workflow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import properties.ConfigFile;
import utils.DBAdaptor;
import utils.IDMapping;

public class Camps2externalDB {

	private static final Connection SIMAP_CONNECTION = DBAdaptor.getConnection("simap");
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	
	//private static final int SWISSPROT_ID = 313;
	//private static final int TREMBL_ID = 314;
	private static final int PDB_ID = 474;
	private static final int GENBANK_ID = 703;
		
	
	/**
	 * Runs the whole program.
	 */
	public static void run() {
		try {
			/*
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO camps2uniprot " +
					"(sequenceid," +
					"uniprot_name," +
					"subset," +
					"linkouturl) " +
					"VALUES " +
					"(?,?,?,?)");
			*/
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO camps2pdb " +
					"(sequenceid," +
					"pdb_name," +
					"linkouturl) " +
					"VALUES " +
					"(?,?,?)");
			
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO camps2genbank " +
					"(sequenceid," +
					"genbank_name," +
					"linkouturl) " +
					"VALUES " +
					"(?,?,?)");			
			
			
			System.out.println("\t\t[INFO]: Perform mapping between SIMAP and CAMPS sequence ids.");
			IDMapping idm = new IDMapping();			
			int[] mapping = idm.getMapping();  //index: SIMAP id, value: CAMPS id
			
			
			ConfigFile cf = new ConfigFile("/home/users/saeed/workspace/CAMPS3/config/config.xml");
			//ConfigFile cf = new ConfigFile("F:/eclipse_workspace/CAMPS3/config/config.xml");
			//String uniprotLinkout = cf.getProperty("linkout:uniprot");
			String pdbLinkout = cf.getProperty("linkout:pdb");
			String genbankLinkout = cf.getProperty("linkout:genbank");
			cf = null;
			
			
			Statement stm = SIMAP_CONNECTION.createStatement();
			stm.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = stm.executeQuery("SELECT sequenceid,name,databaseid FROM protein");
			while(rs.next()) {
				
				int simapID = rs.getInt("sequenceid");
				String name = rs.getString("name");
				int databaseID = rs.getInt("databaseid");
				
				int campsID = mapping[simapID];
				
				if(campsID == 0) { //no valid mapping between SIMAP and CAMPS
					continue;
				}
				/*
				if(databaseID == SWISSPROT_ID) {
					
					String url = uniprotLinkout+name;
					
					pstm1.setInt(1, campsID);
					pstm1.setString(2, name);
					pstm1.setString(3, "swissprot");
					pstm1.setString(4, url);
					
					pstm1.executeUpdate();
				}
				else if(databaseID == TREMBL_ID) {
					
					String url = uniprotLinkout+name;
					
					pstm1.setInt(1, campsID);
					pstm1.setString(2, name);
					pstm1.setString(3, "trembl");
					pstm1.setString(4, url);
					
					pstm1.executeUpdate();
				}
				*/
				//else if(databaseID == PDB_ID) {
				if(databaseID == PDB_ID) {
					
					String code = name.split("_")[0].toUpperCase();
					String url = pdbLinkout+code;
					
					pstm2.setInt(1, campsID);
					pstm2.setString(2, name);
					pstm2.setString(3, url);
					
					pstm2.executeUpdate();
				}
				else if(databaseID == GENBANK_ID) {
					
					String genbankID = name.split("\\|")[1];
					String url = genbankLinkout+genbankID;
					
					pstm3.setInt(1, campsID);
					pstm3.setString(2, name);
					pstm3.setString(3, url);
					
					pstm3.executeUpdate();
				}
			}
			rs.close();
			stm.close();
			
			//pstm1.close();
			pstm2.close();
			pstm3.close();
			
		} catch(Exception e) {
			System.err.println("Exception in Camps2externalDBs.run(): " +e.getMessage());
			e.printStackTrace();
	
		} finally {
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
}
