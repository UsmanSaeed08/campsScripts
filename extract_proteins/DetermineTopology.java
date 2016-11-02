package extract_proteins;

/*
 * DetermineTopology
 * 
 * Version 2.0
 * 
 * 2014-08-12
 * 
 */


import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.BitSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import properties.ConfigFile;

import utils.DBAdaptor;


/**
 *  
 * @author usman
 * 
 * Determines the topology of the whole membrane protein dataset of CAMPS 
 * based on PHOBIUS predictions.
 * The predictions are stored in the MySQL database 'CAMPS' in table 'topology'.
 *
 */
public class DetermineTopology {
	
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	
	/**
	 * Runs the whole program.
	 */
	public static void run() {
		try {
			
			int batchSize = 50;
			
			//write temporary input file for PHOBIUS
			File phobiusInfileTMP = File.createTempFile("camps4", ".fasta");
			PrintWriter pw = new PrintWriter(new FileWriter(phobiusInfileTMP));
			
			BitSet processedIDs = new BitSet();
			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT sequenceid, sequence FROM sequences2");
			while(rs.next()) {
				int sequenceid = rs.getInt("sequenceid");
				String sequence = rs.getString("sequence").toUpperCase();
				
				//avoid duplicate FASTA entries
				if(!processedIDs.get(sequenceid)) {
					pw.println(">"+sequenceid+"\n"+sequence);
					
					processedIDs.set(sequenceid);
				}
			}	
			rs.close();
			rs = null;
			stm.close();
			stm = null;
			
			pw.close();
			
			//run PHOBIUS and store results in database		
			int statusCounter = 0;
			int batchCounter = 0;
			
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO topology " +
					"(sequenceid,method,n_term,c_term) " +
					"VALUES " +
					"(?,\"phobius\",?,?)");
			
			Pattern p1 = Pattern.compile("(.*)\\s+(\\d+)\\s+([0Y])\\s+(.*)");		//complete row			
			Pattern p2 = Pattern.compile("(([io]\\d+-\\d+)*[io])");		//TMH prediction(s)			
			
			ConfigFile cf = new ConfigFile("/home/users/saeed/workspace/CAMPS3/config/config.xml");
			String phobiusDir = cf.getProperty("apps:phobius:install-dir");
		    String phobiusCmd = phobiusDir + " -short " + phobiusInfileTMP.getAbsolutePath();
		    Process p = Runtime.getRuntime().exec(phobiusCmd);
		    BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
		    String line;
		    while((line=br.readLine()) != null) {
		    	
		    	Matcher m1 = p1.matcher(line);
				if(m1.matches()) {
					statusCounter++;
					if (statusCounter % 100 == 0) {
						System.out.write('.');
						System.out.flush();
					}
					if (statusCounter % 10000 == 0) {
						System.out.write('\n');
						System.out.flush();
					}
					
					String query = m1.group(1).trim();
					int sequenceid = Integer.parseInt(query);
					int num_tms = Integer.parseInt(m1.group(2));
					String prediction = m1.group(4);
										
					if(num_tms > 0) {
						Matcher m2 = p2.matcher(prediction);
						if(m2.find()) {
							String tms_prediction = m2.group(1);
							String n_term = null;
							String c_term = null;
							if(tms_prediction.startsWith("i")) {
								n_term = "in";
							}
							else if(tms_prediction.startsWith("o")) {
								n_term = "out";
							}
							if(tms_prediction.endsWith("i")) {
								c_term = "in";
							}
							else if(tms_prediction.endsWith("o")) {
								c_term = "out";
							}
							
							if(n_term != null && c_term != null) {							
								batchCounter++;								
								
								pstm.setInt(1, sequenceid);
								pstm.setString(2, n_term);
								pstm.setString(3, c_term);
								
								pstm.addBatch();
								
								if(batchCounter % batchSize == 0) {								
									pstm.executeBatch();
									pstm.clearBatch();
								}
							}
						}					
					}
				}
		    	
		    }		    
			p.waitFor();
			p.destroy();
			
			br.close();
			
			pstm.executeBatch();	//insert remaining entries
			pstm.clearBatch();
			pstm.close();
			pstm = null;
			
			
			phobiusInfileTMP.delete();
						
		} catch (Exception e) {					
			System.err.println("Exception in DetermineTopology.run(): " +e.getMessage());
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
