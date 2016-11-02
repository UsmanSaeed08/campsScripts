/*
 * IDMapping
 * 
 * Version 1.0
 * 
 * 2009-08-25
 * 
 */

package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.Date;
import java.util.Hashtable;

/**
 * 
 * @author sneumann
 * 
 * Performs the mapping of sequences ids used in CAMPS and SIMAP sequence ids.
 * This is necessary as SIMAP sequence ids are used as foreign keys in almost
 * all SIMAP database tables.
 *  
 *
 */
public class IDMapping {
	
	
	private static final Connection SIMAP_CONNECTION = DBAdaptor.getConnection("simap");
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	//number of values for 'SELECT ... IN(...)' QUERIES
	private static final int SQL_BLOCK_SIZE = 10000;
	
	
	private int maxSimapSequenceid;
	
	//set of SIMAP sequence ids that represents CAMPS dataset
	private BitSet simapSequenceids;
	
	//mapping of CAMPS to SIMAP sequence ids (index: SIMAP id, value: CAMPS id)
	private int[] mapping;
	
	
	private String mappingFile = null;
	
	
	/*
	 * Performs mapping from scratch.
	 */
	public IDMapping() {
		run();
	}
	
	
	/*
	 * Reads already performed mapping from file.
	 */
	public IDMapping(String file) {
		this.mappingFile = file;
		run();
	}
	
	
	public int getMaxSimapSequenceid() {
		return this.maxSimapSequenceid;
	}
	
	public BitSet getSimapSequenceIDs() {
		return this.simapSequenceids;
	}
	
	public int[] getMapping() {
		return this.mapping;
	}
	
	
	private void run() {
			
		try {
			
			if(this.mappingFile != null) {
				
				this.simapSequenceids = new BitSet();				
				
				BufferedReader br = new BufferedReader(new FileReader(new File(this.mappingFile)));
				String line;
				while((line = br.readLine()) != null) {
					if(line.startsWith("#")) {
						continue;
					}
					
					if(line.startsWith("MAX:")) {
						this.maxSimapSequenceid = Integer.valueOf(line.substring(4).trim());
						this.mapping = new int[this.maxSimapSequenceid+1];
						continue;
					}
					
					int simapid = Integer.valueOf(line.split("\t")[0]);
					int campsid = Integer.valueOf(line.split("\t")[1]);
					
					this.simapSequenceids.set(simapid);
					this.mapping[simapid] = campsid;
					
				}
				br.close();
			}
			
			else {
				
				Statement stm1 = SIMAP_CONNECTION.createStatement();
				
				//get maximal SIMAP sequence id
				ResultSet rs1 = stm1.executeQuery("SELECT sequenceid FROM sequence " +
						"ORDER BY sequenceid DESC LIMIT 1"); 	// the count on 11.08.13 was 62564238
				while(rs1.next()) {
					this.maxSimapSequenceid = rs1.getInt("sequenceid");
				}
				// let this be like that and set a limit on sequences from camps.. automaticaly the size would reduce!!! - test case
				rs1.close();
				rs1 = null;
				stm1.close();
				stm1 = null;
				
				String sql = "SELECT sequenceid, md5 FROM sequence WHERE md5 IN (";
				
				this.simapSequenceids = new BitSet();
				this.mapping = new int[this.maxSimapSequenceid+1];

				Statement stm2 = CAMPS_CONNECTION.createStatement();	
				stm2.setFetchSize(Integer.MIN_VALUE);
				ResultSet rs2 = stm2.executeQuery("SELECT sequenceid,md5 FROM sequences2");
				//ResultSet rs2 = stm2.executeQuery("SELECT sequenceid,md5 FROM sequences limit 100");
				
				Hashtable<String,Integer> md52campsid = new Hashtable<String,Integer>();
				int currentBlockSize = 0;
				String md5Collector = "";
				
				while(rs2.next()) {
					
					currentBlockSize++;
					
					int campsid = rs2.getInt("sequenceid");
					String md5 = rs2.getString("md5");
					md52campsid.put(md5, Integer.valueOf(campsid));
					
					md5Collector += ",\""+md5+"\"";
					
					if(currentBlockSize % SQL_BLOCK_SIZE == 0) {
						md5Collector = md5Collector.substring(1);	//remove first comma
						
						Statement stm3 = SIMAP_CONNECTION.createStatement();
						ResultSet rs3 = stm3.executeQuery(sql+md5Collector+")");
						while(rs3.next()) {
							int simapid = rs3.getInt("sequenceid");
							String simapMd5 = rs3.getString("md5"); 
							
							int mappedCampsid = md52campsid.get(simapMd5);
							
							this.simapSequenceids.set(simapid);
							this.mapping[simapid] = mappedCampsid;
						}
						rs3.close();
						stm3.close();
						
											
						//reset all data
						md52campsid = new Hashtable<String,Integer>();					
						md5Collector = "";							
					}				
					
				}
				rs2.close();
				rs2 = null;
				stm2.close();
				stm2 = null;
				
				//process remaining entries
				if(!md5Collector.equals("")) {
					md5Collector = md5Collector.substring(1);	//remove first comma
					
					Statement stm3 = SIMAP_CONNECTION.createStatement();
					ResultSet rs3 = stm3.executeQuery(sql+md5Collector+")");
					while(rs3.next()) {
						int simapid = rs3.getInt("sequenceid");
						String simapMd5 = rs3.getString("md5"); 
						
						int mappedCampsid = md52campsid.get(simapMd5);
						
						this.simapSequenceids.set(simapid);
						this.mapping[simapid] = mappedCampsid;
					}
					rs3.close();
					stm3.close();
				}
				
			}			
			
		} catch(Exception e) {
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
	
	
	public void writeToFile(String outfile) throws IOException {
		PrintWriter pw = new PrintWriter(new FileWriter(new File(outfile)));
		
		pw.println("MAX: " +this.maxSimapSequenceid);
		
		int size = this.mapping.length;
		
		for(int i=0; i<size; i++) {
			int value = this.mapping[i];
			
			if(value != 0) {
				pw.println(i+"\t"+value);
			}
		}
		
		
		pw.close();
	}
	
//	public static void main(String[] args) throws IOException {
//		
//		SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
//		
//		Date startDate = new Date();
//		System.out.println("\n\n\t...["+df.format(startDate));
//		
//		IDMapping im = new IDMapping();
//		im.writeToFile("/home/proj/Camps3/simapCamps_idMapping.txt");
//			
//		Date endDate = new Date();
//		System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
//	}

}
