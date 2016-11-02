/*
 * Pdbtm2opm
 * 
 * Version 1.0
 * 
 * 2010-08-05
 * 
 */

package workflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import utils.DBAdaptor;

public class Pdbtm2opm {
	
private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	
	/**
	 * 
	 */
	public static void getClassifications(String fname) {
		try {
			
			Hashtable<String,ArrayList<String>> id2classifications = new Hashtable<String,ArrayList<String>>();
			
			BufferedReader br = new BufferedReader(new FileReader(new File(fname)));
			String line;
			while((line = br.readLine()) != null) {
				String[] tmp = line.split("\t");
				
				String classification = tmp[0].trim();
				String[] pdbIDs = tmp[2].trim().split(",");
				
				for(String pdbID: pdbIDs) {
					
					pdbID = pdbID.trim();
					
					ArrayList<String> classifications = new ArrayList<String>();
					if(id2classifications.containsKey(pdbID)) {
						classifications = id2classifications.get(pdbID);
					}
					
					if(!classifications.contains(classification)) {
						classifications.add(classification);
					}
					
					id2classifications.put(pdbID, classifications);
				}
			}
			br.close();
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO pdbtm2opm " +
					"(pdb_id, classification) " +
					"VALUES " +
					"(?,?)");
			
			
			
			Enumeration<String> pdbIDs = id2classifications.keys();
			while(pdbIDs.hasMoreElements()) {
				
				String pdbID = pdbIDs.nextElement();
				
				ArrayList<String> classifications = id2classifications.get(pdbID);
				
				if(classifications.size()>1) {
					System.out.println("\tINFO "+pdbID+" has multiple classifications: "+classifications);
				}							
				
				
				for(String classification: classifications) {
					
					pstm.setString(1, pdbID);
					pstm.setString(2, classification);
					System.out.print(pdbID + "\t" + classification+"\n");
					pstm.executeUpdate();
				}
				
			}				
			
			pstm.close();
			pstm = null;
			
		} catch(Exception e) {
			System.err.println("Exception in Pdbtm2opm.run(): " +e.getMessage());
			e.printStackTrace();
		
		} 
	}
	
	public static void disconnect() {
		if (CAMPS_CONNECTION != null) {
			try {
				CAMPS_CONNECTION.close();					
			} catch (SQLException e) {					
				e.printStackTrace();
			}
		}
	}		

}
