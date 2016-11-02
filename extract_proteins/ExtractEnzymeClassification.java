

package extract_proteins;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import utils.DBAdaptor;


public class ExtractEnzymeClassification {
	
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	
	public static void run(String fname1, String fname2) {
		
		
		try {
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid " +
					"FROM camps2uniprot " +
					"WHERE entry_name=?");
			
			
			
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO ec_classification " +
					"(sequenceid, classification, uniprot_name) " +
					"VALUES " +
					"(?,?,?)");
			
			Pattern p1 = Pattern.compile("DR\\s+([A-Za-z0-9]+\\s*,\\s*[A-Za-z0-9]+_[A-Za-z0-9]+\\s*;\\s*){1,}");
			Pattern p2 = Pattern.compile("([A-Za-z0-9]+_[A-Za-z0-9]+)\\s*;");
			
			//parse Expasy dat file		
	        System.out.println("In progress: Read Expasy dat file");
	        BufferedReader br = new BufferedReader(new FileReader(new File(fname1)));    
			 
			String line;
			String classification = "";
			ArrayList<String> accessions = new ArrayList<String>();
			int count = 0; 
			while ((line = br.readLine()) != null) {
				count++;
				if (count%10000 == 0){
					System.out.print("Processed Lines " + count + " of 141852\n");
				}
				Matcher m1 = p1.matcher(line);
								
				if (line.startsWith("ID")) {
					classification = line.substring(2).trim();									
				}
				
				else if (m1.matches()) {
					
					Matcher m2 = p2.matcher(line);
					int start = 0;
					while(m2.find(start)) {
						String uniprotAccession = m2.group(1).trim();
						start = m2.end();
						
						accessions.add(uniprotAccession);
					}
						
				}
				else if (line.startsWith("//")) {
										
					for(String accession: accessions) {
						
						boolean isindb = false;
						int sequenceid = -1;
						pstm1.setString(1, accession);
						ResultSet rs1 = pstm1.executeQuery();
						while(rs1.next()) {
							isindb = true;
							sequenceid = rs1.getInt("sequenceid");
						}
						rs1.close();
						
						if(isindb) {
							
							pstm2.setInt(1, sequenceid);
							pstm2.setString(2, classification);
							pstm2.setString(3, accession);
							
							pstm2.executeUpdate();
						}
					}
					
					//reset
					classification = "";
					accessions = new ArrayList<String>();					
				}
			}
			br.close();		
			
			pstm1.close();
			pstm2.close();
			
			
			//add hierarchy to db
			addHierarchy(fname2);
			
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}			
		}
		
	}
	
	
	
	private static void addHierarchy(String fname) {
		try {
			
			Pattern p = Pattern.compile("(\\d+\\.\\s*[\\d|-]+\\.\\s*[\\d|-]+\\.\\s*[\\d|-]+)\\s*(.+)\\.");
			
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO other_database_hierarchies " +
					"(key_, description, db) " +
					"VALUES " +
					"(?,?,?)");
			
			BufferedReader br = new BufferedReader(new FileReader(new File(fname)));
			String line;
			while((line = br.readLine()) != null) {
				
				Matcher m = p.matcher(line);
				
				if(m.matches()) {
					String key_ = m.group(1);
					String description = m.group(2);
					
					while(key_.endsWith("-") || key_.endsWith(".")) {
						key_ = key_.substring(0, key_.length()-1).trim();				
					}
					
					key_ = key_.replaceAll("\\s", "");
					
					pstm.setString(1, key_);
					pstm.setString(2, description);
					pstm.setString(3, "enzyme");
					pstm.executeUpdate();
				}
				
			}
			br.close();
			
			pstm.close();
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
}
