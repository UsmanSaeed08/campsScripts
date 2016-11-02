/*
 * Pdbtm2scop_cath (old name: Pdbtm2sc)
 * 
 * Version 2.0
 * 
 * 2010-01-29
 * 
 */

package workflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import utils.DBAdaptor;

public class Pdbtm2scop_cath {
	
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	
	private static final String CATH_REFERENCE = "/home/proj/check/otherdatabase/CathDomainList.v4.0.0";
			
	private static final String SCOP_REFERENCE = "/home/proj/check/otherdatabase/dir.cla.scope.2.05-stable.txt";
	
	
	/**
	 * 
	 */
	public static void getStructuralClassifications() {
		try {
			
			ArrayList<String> pdbtmIDs = new ArrayList<String>();
			
			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT name FROM sequences_other_database WHERE db=\"pdbtm\"");
			while(rs.next()) {
				String name = rs.getString("name");
				String id = name.replaceFirst("_", "");
				pdbtmIDs.add(id);
			}
			rs.close();
			rs = null;
			stm.close();
			stm = null;
			
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO pdbtm2scop_cath " +
					"(pdb_id, domain, classification, db) " +
					"VALUES " +
					"(?,?,?,?)");
			
						
			BufferedReader br1 = new BufferedReader(new FileReader(new File(CATH_REFERENCE)));
			String line1;
			while((line1 = br1.readLine()) != null) {
				
				if(line1.startsWith("#")) {
					continue;
				}
				
				String[] content = line1.split("\\s+");
				String domain = content[0];
				String class_ = content[1];
				String architecture = content[2];
				String topology = content[3];
				String superfamily = content[4];
				
				String classification = class_+"."+architecture+"."+topology+"."+superfamily;
				
				String id = domain.substring(0,domain.length()-2);
				
				if(pdbtmIDs.contains(id)) {
					
					String code = id.substring(0,id.length()-1)+"_"+id.substring(id.length()-1);
															
					pstm.setString(1, code);
					pstm.setString(2, domain);					
					pstm.setString(3, classification);
					pstm.setString(4, "cath");
					
					pstm.executeUpdate();
					
				}
				
			}
			br1.close();
			
						
			BufferedReader br2 = new BufferedReader(new FileReader(new File(SCOP_REFERENCE)));
			String line2;
			while((line2 = br2.readLine()) != null) {
				
				if(line2.startsWith("#")) {
					continue;
				}
				String[] content = line2.split("\t");
				String domain = content[0];						
				String classification = content[3];
				
				String structure = domain.substring(1,domain.length()-2);
	            String chain = domain.substring(domain.length()-2,domain.length()-1);
	            					
				
				//
				//within SCOP domain names chains are written in lower case;
				//in PDBTM chains are usually indicated by upper case letters,
				//=> change case to match between SCOP and PDBTM codes 
				//		
				String id = structure+chain.toUpperCase();
				if(pdbtmIDs.contains(id)) {
					
					String code = id.substring(0,id.length()-1)+"_"+id.substring(id.length()-1);
					
					pstm.setString(1, code);
					pstm.setString(2, domain);					
					pstm.setString(3, classification);
					pstm.setString(4, "scop");
					
					pstm.executeUpdate();
					
				}
				
			}
			br2.close();
			
			pstm.close();
			pstm = null;
			
		} catch(Exception e) {
			System.err.println("Exception in Pdbtm2scop_cath.run(): " +e.getMessage());
			e.printStackTrace();
		
		} 
	}
	
	
	/**
	 * 
	 * @param namesFile - 	Cath Names File (CNF) that contains description of each node in the 
	 * 						CATH hierarchy for class, architecture, topology and homologous 
	 * 						superfamily levels.
	 */ 
	public static void addCathHierarchy(String namesFile) {
		try {
			
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO other_database_hierarchies " +
					"(key_,description,hierarchy,comment,db) " +
					"VALUES " +
					"(?,?,?,?,\"cath\")");
			
			Pattern pClass = Pattern.compile("\\d+");
			Pattern pArchitecture = Pattern.compile("\\d+\\.\\d+");
			Pattern pTopology = Pattern.compile("\\d+\\.\\d+\\.\\d+");
			Pattern pSuperfamily = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");
			
						
			BufferedReader br = new BufferedReader(new FileReader(new File(namesFile)));
			String line;
			while((line = br.readLine()) != null) {				
				
				if(line.startsWith("#")) {
					continue;
				}
				
				String[] content = line.split("\\s+");
				String nodeNumber = content[0];
				String nodeDescription = content[2].substring(1);		//ignore ":" at the beginning
				if(content.length > 3) {
					for(int i=3; i<content.length; i++) {
						nodeDescription+= " " +content[i];
					}
				}
				
				String hierarchy = null;
				
				Matcher mClass = pClass.matcher(nodeNumber);
				if(mClass.matches()) {					
					hierarchy = "Class";
					
				}
				
				Matcher mArchitecture = pArchitecture.matcher(nodeNumber);
				if(mArchitecture.matches()) {
					hierarchy = "Architecture";
					
				}
				
				Matcher mTopology = pTopology.matcher(nodeNumber);
				if(mTopology.matches()) {
					hierarchy = "Topology";
					
				}
				
				Matcher mSuperfamily = pSuperfamily.matcher(nodeNumber);
				if(mSuperfamily.matches()) {
					hierarchy = "Superfamily";
					
				}	
				
				pstm.setString(1, nodeNumber);
				pstm.setString(2, nodeDescription);
				pstm.setString(3, hierarchy);
				pstm.setString(4, null);
				
				pstm.executeUpdate();
				
			}
			br.close();
						
			
			pstm.close();
			
		} catch(Exception e) {
			e.printStackTrace();
			
		} 
	}
	
	
	/**
	 * 
	 * @param dirDesFile - 	dir.des.scop.txt file that contains description of each node in the 
	 * 						SCOP hierarchy for class, fold, superfamily and family levels.
	 */
	public static void addScopHierarchy(String dirDesFile) {
		try {
			
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO other_database_hierarchies " +
					"(key_,description,hierarchy,comment,db) " +
					"VALUES " +
					"(?,?,?,?,\"scop\")");
						
			BufferedReader br = new BufferedReader(new FileReader(new File(dirDesFile)));
			String line;
			while((line = br.readLine()) != null) {
				
				if(line.startsWith("#")) {
					continue;
				}
				
				String[] content = line.split("\t");
				String hierarchyShort = content[1];
				String nodeNumber = content[2];
				String nodeDescription = content[4];
				
				String hierarchy = null;
				
				if(hierarchyShort.equals("cl")) {					
					hierarchy = "Class";
				}
				
				else if(hierarchyShort.equals("cf")) {
					hierarchy = "Fold";
				}
				
				else if(hierarchyShort.equals("sf")) {
					hierarchy = "Superfamily";
				}
				
				else if(hierarchyShort.equals("fa")) {
					hierarchy = "Family";
				}
				
				if(hierarchy == null) {
					continue;
				}
				
				pstm.setString(1, nodeNumber);
				pstm.setString(2, nodeDescription);
				pstm.setString(3, hierarchy);
				pstm.setString(4, null);
				
				pstm.executeUpdate();
				
			}
			br.close();			
			
			pstm.close();
			
		} catch(Exception e) {
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
