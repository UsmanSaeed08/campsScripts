package wokflow2;

import general.CreateDatabaseTables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import properties.ConfigFile;
import utils.DBAdaptor;
import utils.IDMapping;

public class Camps2pdb30Identity {

	/**
	 * @param args
	 * the class is used to populate the data in camps2pdb30Iden from source file
	 * the data contains hits with at least 30% identity. 
	 * 
	 */
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	private static final String pathtoCampsPdbMapFile = "F:/Scratch/mappingCampsToCATHandPDB/MappedPdbtoCampsSC";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Run...");
		CreateDatabaseTables.create_table_camps2pdb30Iden();
		run();
	}

	private static void run() {
		try {
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO camps2pdb30Iden " +
							"(sequenceid," +
							"pdb_name," +
							"linkouturl) " +
							"VALUES " +
					"(?,?,?)");

			//ConfigFile cf = new ConfigFile("/home/users/saeed/workspace/CAMPS3/config/config.xml");
			ConfigFile cf = new ConfigFile("F:/eclipse_workspace/CAMPS3/config/config.xml");
			//String uniprotLinkout = cf.getProperty("linkout:uniprot");
			String pdbLinkout = cf.getProperty("linkout:pdb");
			cf = null;

			// Read the mapping file
			BufferedReader br = new BufferedReader(new FileReader(new File(pathtoCampsPdbMapFile)));
			String line = "";
			while((line = br.readLine())!=null){
				line = line.trim();
				if(!line.isEmpty()){
					String parts[] = line.split("\t");
					String campsId = parts[0].trim();
					int campsID = Integer.parseInt(campsId);
					String name  = parts[1].trim();
					//String description = parts[7].trim();
					Float Identity = Float.parseFloat(parts[2].trim());
					if(Identity > 29){
						String code = name.split("_")[0].toUpperCase();
						String url = pdbLinkout+code;

						pstm2.setInt(1, campsID);
						pstm2.setString(2, name);
						pstm2.setString(3, url);
						
						System.out.println(campsID+"\t"+name+"\t"+url);

						pstm2.executeUpdate();
					}
				}
			}
			br.close();
			pstm2.close();
		} catch(Exception e) {
			System.err.println("Exception in Camps2pdb30Identity.run(): " +e.getMessage());
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
