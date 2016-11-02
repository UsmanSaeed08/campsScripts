package workflow;

import general.CreateDatabaseTables;

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

public class PfamData {
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");
	
	
	public static void run(String pfamAFile, String clan2familiesFile, String clanFile) {
try {
			
			Pattern p1 = Pattern.compile("ACC\\s+(PF\\d+)\\.\\d+");
			Pattern p2 = Pattern.compile("DESC\\s+(.*)");
			Pattern p3 = Pattern.compile("NAME\\s+(.*)");
			
			ArrayList<String> accessions = new ArrayList<String>();
			
			Statement stm = CAMPS_CONNECTION.createStatement();
			
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO pfam " +
					"(accession, name, description, clan_id, clan_description) " +
					"VALUES " +
					"(?,?,?,?,?)");
			
			ResultSet rs = stm.executeQuery("SELECT distinct(accession) FROM domains_pfam");
			while(rs.next()) {
				
				String accession = rs.getString("accession");
				accessions.add(accession);
			}
			rs.close();
			BufferedReader br = new BufferedReader(new FileReader(new File(pfamAFile)));
			String line;

			String accession = "";
			String description = "";
			String name = "";
			
			System.out.print("Size of set to be computed "+accessions.size() +"\n");
			int i =0; 
			while((line = br.readLine()) != null) {

				line = line.trim();

				Matcher m1 = p1.matcher(line);
				Matcher m2 = p2.matcher(line);
				Matcher m3 = p3.matcher(line);

				if (m1.matches()) {
					accession = m1.group(1);						
				}

				else if (m2.matches()) {
					description = m2.group(1);
				}
				else if (m3.matches()) {
					name = m3.group(1);
				}
				else if (line.startsWith("//")) {	// end of one record
					if(accessions.contains(accession)) {
						String clanID = getClan(accession, clan2familiesFile);
						String clanDescription = "";
						if(clanID == null ) {
							clanDescription = null;
						}	
						else{
							clanDescription = getClanDescription(clanID, clanFile);	//added Dec 02, 2014
							//
						}
						pstm.setString(1, accession);
						pstm.setString(2, name);
						pstm.setString(3, description);
						
						pstm.setString(4, clanID);
						pstm.setString(5, clanDescription);

						pstm.executeUpdate();
						i++;
						if (i%1000 == 0){
							System.out.print("Sequences processed "+ i + "\n");
						}
						//System.out.print("****Acc "+ accession + "****Desc " + description + "****Name " + name + "*****ClanId "+ clanID + "****ClanDesc "+clanDescription+"\n");
					}
					//reset
					accession = "";
					description = "";
					name = "";				
				}
			}


			br.close();
			System.out.print("Total Sequences processed "+ i + "\n");
		}
		catch (Exception e){
			e.printStackTrace();

		}
	}
	
private static String getClan(String accession, String clan2familiesFile) {
		
		String result = null;
		
		try {		
						
			BufferedReader br = new BufferedReader(new FileReader(new File(clan2familiesFile)));
			String line;
			while((line = br.readLine())!= null) {
				
				String[] tmp = line.split("\t");
				String clan = tmp[1].split("\\.")[0];
				
				String[] accessions = tmp[0].split(",");
				for(String s: accessions) {
					if(s.equals(accession)) {
						if (!clan.contains("\\N"))
							result = clan;
					}
				}
			}
			br.close();		
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	
	
	private static String getClanDescription(String clan, String clanFile) {
		
		String description = null;
		
		try {
			
			Pattern p1 = Pattern.compile("#=GF\\s+AC\\s+(CL\\d+)\\.\\d+");
			Pattern p2 = Pattern.compile("#=GF\\s+DE\\s+(.*)");
			
			description = "";
			
			BufferedReader br = new BufferedReader(new FileReader(new File(clanFile)));
			String line;
			boolean startParsing = false;
			boolean stopParsing = false;
			while((line=br.readLine()) != null) {
			
				Matcher m1 = p1.matcher(line);
				Matcher m2 = p2.matcher(line);
				
				
				if(m1.matches()) {
					
					String currentClan = m1.group(1).trim();
					
					if(currentClan.equals(clan)) {
						startParsing = true;
					}					
				}
				
				if(startParsing && line.startsWith("//")) {
					stopParsing = true;
				}
				
				
				if(startParsing && !stopParsing) {
					
					if(m2.matches()) {
						description = m2.group(1).trim();
					}
				}
			}
			br.close();
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return description;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			
			//CreateDatabaseTables.create_table_pfam();
			
			//String fileA = "F:/SC_Clust_postHmm/Pfam-A.hmm/Pfam-A.hmm";						// pfam A file HMM
			//String filefam2clan = "F:/SC_Clust_postHmm/Pfam-A.clans.tsv/Pfam-A.clans.tsv"; 	// Pfam cland id file
			//String clandescFile = "F:/SC_Clust_postHmm/Pfam-C/Pfam-C";						// Pfam C  The file contains the information about clans and the Pfam-A membership
			
			String fileA = "/home/users/saeed/pfamData/Pfam-A.hmm";						// pfam A file HMM
			String filefam2clan = "/home/users/saeed/pfamData/Pfam-A.clans.tsv"; 	// Pfam cland id file
			String clandescFile = "/home/users/saeed/pfamData/Pfam-C";						// Pfam C  The file contains the information about clans and the Pfam-A membership
			
			run(fileA, filefam2clan, clandescFile);
			
			DBAdaptor.createIndex("camps4","pfam",new String[]{"accession"},"accession");
			
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

}
