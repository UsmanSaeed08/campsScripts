package extract_proteins;
/*
 * ExtractGOAnnotation_v2
 * 
 * Version 1.0
 * 
 * 2014-08-12
 * 
 * ExtractGOAnnotation:
 * - Annotations coming from ebi are for UniProt dataset and Ontology file Gene Ontology Consortium obo
 * 
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;

import utils.DBAdaptor;

/**
 * 
 * @author usman
 * 
 * Extracts GO annotations for UniProt dataset and write results to db if
 * corresponding UniProt protein is a CAMPS protein.
 * The predictions are stored in the MySQL database 'CAMPS' in table 'go_annotations'.
 * 
 *
 */
public class ExtractGOAnnotation {
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
		
	
	/**
	 * Runs the whole program.
	 * 
	 * 
	 * annotationFile	- GO annotation file for UniProtKB
	 */
	public static void run(String annotationFile, String ontologyFile) {
		try {			

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid FROM camps2uniprot WHERE accession=?");			
			
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO go_annotations " +
					"(sequenceid, accession, term_type, evidence_code,uniprot_accession) " +
					"VALUES "+
					"(?,?,?,?,?)");
			
			
						
			System.out.println("\tRead annotation file...");
			int statusCounter = 0;
			BufferedReader br1 = new BufferedReader(new FileReader(new File(annotationFile)));
			String line1;
			while((line1 = br1.readLine()) != null) {
				
				statusCounter ++;
				if (statusCounter % 10000 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter % 1000000 == 0) {
					System.err.write(statusCounter);
					System.out.write('\n');
					System.out.flush();
				}	
				
				if(line1.startsWith("UniProtKB")) {
					
					String[] content = line1.split("\t");
					
					String protein = content[1].trim();
					String goAccession = content[4].trim();
					String evidenceCode = content[6].trim();
					String termType = content[8].trim();
					
					if(termType.equals("P")) {
						termType = "biological_process";
					}
					else if(termType.equals("F")) {
						termType = "molecular_function";
					}
					else if(termType.equals("C")) {
						termType = "cellular_component";
					}
					
					int sequenceid = -1;
					pstm1.setString(1, protein);
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {
						sequenceid = rs1.getInt("sequenceid");
					}
					rs1.close();
					
					if(sequenceid != -1) {
						
						pstm2.setInt(1, sequenceid);
						pstm2.setString(2, goAccession);
						pstm2.setString(3, termType);
						pstm2.setString(4, evidenceCode);
						pstm2.setString(5, protein);
							
						pstm2.executeUpdate();												
					}
				}
			}
			br1.close();
			
			pstm1.close();
			pstm2.close();
			
			
			//
			//create indizes
			//
			System.out.println("\tCreate indizes...");
			Statement stm = CAMPS_CONNECTION.createStatement();
			stm.executeUpdate("ALTER TABLE go_annotations ADD INDEX sequences_sequenceid (sequenceid)");
			stm.executeUpdate("ALTER TABLE go_annotations ADD INDEX accession (accession)");
			stm.executeUpdate("ALTER TABLE go_annotations ADD INDEX uniprot_accession (uniprot_accession)");
			stm.close();
			
			
			
			//
			//remove redundant rows (GO annotation file can contain one GO accession for one protein several times)
			//
			System.out.println("\tRemove redundant rows...");
						
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"SELECT id,accession FROM go_annotations WHERE uniprot_accession=?");
			
			BitSet rowsToDelete = new BitSet();
			
			stm = CAMPS_CONNECTION.createStatement();
			
			ResultSet rs = stm.executeQuery("SELECT distinct(uniprot_accession) FROM go_annotations");
			
			while(rs.next()) {
				
				String uniprotAccession = rs.getString("uniprot_accession");
				
				ArrayList<String> goAccessions = new ArrayList<String>();
				
				pstm3.setString(1, uniprotAccession);
				ResultSet rs3 = pstm3.executeQuery();
				while(rs3.next()) {
					
					int rowID = rs3.getInt("id");
					String goAccession = rs3.getString("accession");
					
					if(goAccessions.contains(goAccession)) {
						rowsToDelete.set(rowID);
					}
					else {
						goAccessions.add(goAccession);
					}
				}
				rs3.close();
			}	
			rs.close();
			pstm3.close();
			
			stm.close();
			
			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("" +
					"DELETE FROM go_annotations WHERE id=?");
			
			for(int rowID = rowsToDelete.nextSetBit(0); rowID>=0; rowID = rowsToDelete.nextSetBit(rowID+1)) {
				
				pstm4.setInt(1, rowID);
				pstm4.executeUpdate();
			}		
			pstm4.close();

			// *******************************************************************************************			
			//
			//add names for GO accession terms
			//
			System.out.println("\tRead ontology file...");
			PreparedStatement pstm5 = CAMPS_CONNECTION.prepareStatement(
					"UPDATE go_annotations SET name=? WHERE accession=?");
			
		
			BufferedReader br2 = new BufferedReader(new FileReader(new File(ontologyFile)));
			String line2;
			
			String previousAccession = null;
			String previousName = null;
			while((line2 = br2.readLine()) != null) {
				
				line2 = line2.trim();
				
				if(line2.startsWith("[Term]")) {
					
					if(previousAccession != null && previousName != null) {
						
						pstm5.setString(1, previousName);
						pstm5.setString(2, previousAccession);
						
						pstm5.executeUpdate();
					}
					
					//reset
					previousAccession = null;
					previousName = null;
				}
				else if(line2.startsWith("id:")) {
					
					previousAccession = line2.substring(3).trim();					
				}
				else if(line2.startsWith("name:")) {
					
					previousName = line2.substring(5).trim();
				}
			}
			br2.close();
			
			if(previousAccession != null && previousName != null) {
				
				pstm5.setString(1, previousName);
				pstm5.setString(2, previousAccession);
				
				pstm5.executeUpdate();
			}
			
			
			pstm5.close();
		}
		catch(Exception e) {
			System.err.println("Exception in ExtractGOAnnotation_v2.run(): " +e.getMessage());
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
	
	
}
