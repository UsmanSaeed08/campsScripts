package workflow;

/*
 * OtherDatabase
 * 
 * Version 2.0
 * 
 * 2014-04-Dec
 * 
 */


import general.CreateDatabaseTables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import utils.DBAdaptor;
import utils.FastaReader;
import utils.FastaReader.FastaEntry;
import utils.OtherDbLen;
import utils.TMS_map;

import java.util.*;


import properties.ConfigFile;

import datastructures.TMS;


public class OtherDatabase {

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");



	////////////////////////////////////////////////////////////////////////////
	//																		  //
	//				filter parameters										  //
	//      																  //
	////////////////////////////////////////////////////////////////////////////


	//evalue threshold to filter out insecure BLAST hits
	//private static final double EVALUE_THRESHOLD = 1E-1;
	private static final double EVALUE_THRESHOLD = 1E-3;

	//minimal total number of covered TMHs to filter out insecure BLAST hits
	private static final int MIN_TMH_COVERAGE_TOTAL = 2;

	//minimal percentage of covered TMHs to filter out insecure BLAST hits
	private static final double MIN_TMH_COVERAGE_PERC = 40;

	//minimal identity to filter out insecure BLAST hits
	private static final double MIN_IDENTITY = 40;



	private static final int DRUGBANK = 1;
	private static final int GPCRDB = 2;
	private static final int MPTOPO = 3;
	private static final int OMIM = 4;
	private static final int OPM = 5;
	private static final int PDBTM = 6;
	private static final int TARGETDB = 7;
	private static final int TCDB = 8;
	private static final int TOPDB = 9;
	private static final int VFDB = 10;


	/**
	 * 
	 * Generates a modified version of the original FASTA file. In the new file, 
	 * the header has an additional counter at the beginning and the sequence is 
	 * given in a single (!) line.
	 * 
	 * The additional counter in the header can be useful if the new FASTA file
	 * is used in a BLAST search. Because long headers are truncated in the result 
	 * file and thus identification of the right query/hit may be impossible 
	 * (if truncated headers are identical). 
	 * 
	 * @param originalFile	- original FASTA file 
	 * @param formattedFile - FASTA formatted file
	 */
	public static void generalFastaFormatting(File originalFile, File formattedFile) {
		try {

			PrintWriter pw = new PrintWriter(new FileWriter(formattedFile));

			FastaReader fr = new FastaReader(originalFile);
			ArrayList<FastaEntry> entries = fr.getEntries();
			int countEntry = 0;
			for(FastaEntry entry: entries) {
				countEntry++;

				String header = entry.getHeader().trim();
				header = header.replaceAll("\\s\\$\\s?", "_");

				String sequence = entry.getSequence().replaceAll("\\s+", "");

				pw.println(">"+countEntry+"|"+header+"\n"+sequence);

			}

			pw.close();

		} catch(Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * 
	 * Generates a FASTA file from the specified MPtopo text file.
	 * 
	 * @param mptopoFile	- original MPtopo file downloaded from Website
	 * @param formattedFile - FASTA formatted file
	 */
	public static void mptopoFastaFormatting(File mptopoFile, File formattedFile) {
		try {

			Pattern entryPattern = Pattern.compile(">(.*?)\\nprotein_name:(.*?)\\nfile_name:(.*?)\\n.*?Swiss_Prot_entry:(.*?)\\n.*?sequence:(\\w+)",Pattern.DOTALL);

			PrintWriter pw = new PrintWriter(new FileWriter(formattedFile));
			int countEntry = 0;

			StringBuffer sb = new StringBuffer();
			BufferedReader br = new BufferedReader(new FileReader(mptopoFile));
			String line;
			while((line = br.readLine()) != null) {
				if(line.startsWith(">")) {
					if(sb.length() > 0) {
						String entry = sb.toString().trim();
						Matcher m =  entryPattern.matcher(entry);
						if(m.find()) {
							String subset = m.group(1);
							String pname = m.group(2);
							String fname = m.group(3);
							String sprot = m.group(4);
							if(sprot.trim().equals("")) {
								sprot = "NA";
							}
							String sequence = m.group(5);

							if(subset != null && pname != null && sprot != null && sequence != null) {
								//ignore beta-barrel and monotopic membrane proteins (== 3D_other)
								if(!subset.startsWith("3D_other")) {
									countEntry++;
									pw.println(">"+countEntry+"|"+sprot+"|"+pname+"|"+fname+"\n"+sequence);
								}								
							}							
						}	
						else {
							System.out.println("WARNING: The following lines are not matching:\n" +entry);
						}
					}
					sb = new StringBuffer(line+"\n");
				}
				else {
					sb.append(line+"\n");
				}
			}
			br.close();

			//write last entry
			if(sb.length() > 0) {
				String entry = sb.toString().trim();
				Matcher m =  entryPattern.matcher(entry);
				if(m.find()) {
					String subset = m.group(1);
					String pname = m.group(2);
					String sprot = m.group(3);
					String sequence = m.group(4);

					if(subset != null && pname != null && sprot != null && sequence != null) {
						//ignore beta-barrel and monotopic membrane proteins (== 3D_other)
						if(!subset.startsWith("3D_other")) {
							countEntry++;
							pw.println(">"+countEntry+"|"+sprot+"|"+pname+"\n"+sequence);
						}								
					}					
				}	
				else {
					System.out.println("WARNING: The following lines are not matching:\n" +entry);
				}
			}

			pw.close();

		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * Generates a FASTA file for the OPM proteins given in the specified
	 * file. The sequences are extracted from SIMAP using the PDB ids, 
	 * as the OPM file doesn't contain that information. 
	 * 
	 * @param opmFile		- OPM data file with information separated by tab
	 * 						  (column 0 - OPM family
	 * 						   column 1 - protein name of member
	 *   					   column 2 - PDB id of member)
	 * @param formattedFile	- FASTA formatted file
	 */
	public static void opmFastaFormatting(File opmFile, File formattedFile) {
		try {

			Connection conn = DBAdaptor.getConnection("simap");
			PreparedStatement pstm1 = conn.prepareStatement("SELECT sequenceid, name, description FROM protein WHERE name like ? AND databaseid=474");
			PreparedStatement pstm2 = conn.prepareStatement("SELECT sequence FROM sequence WHERE sequenceid=?");

			PrintWriter pw = new PrintWriter(new FileWriter(formattedFile));
			int countEntry = 0;

			BufferedReader br = new BufferedReader(new FileReader(opmFile));
			String line;
			while((line = br.readLine()) != null) {
				if(line.startsWith("#")) {
					continue;
				}

				String[] content = line.split("\\t");
				if(content.length > 3) {

					String family = content[0].trim();
					family = family.substring(0,family.length()-1); //remove '.' at end (e.g. 1.1.01.01. becomes 1.1.01.01)
					String pdbID = content[2].trim();

					pstm1.setString(1, pdbID+"%");
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {
						int sequenceid = rs1.getInt("sequenceid");
						String pname = rs1.getString("name");
						String description = rs1.getString("description");

						pstm2.setInt(1, sequenceid);
						ResultSet rs2 = pstm2.executeQuery();
						while(rs2.next()) {
							countEntry++;

							String sequence = rs2.getString("sequence");

							pw.println(">"+countEntry+"|"+pname+"|"+description+"|"+family+"\n"+sequence);
						}
						rs2.close();
						rs2 = null;
					}
					rs1.close();
					rs1 = null;
				}
			}
			br.close();

			pstm1.close();
			pstm2.close();
			conn.close();

			pw.close();

		} catch(Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * 
	 * Generates a modified version of the original PDB FASTA file. All sequences
	 * in the original file that are composed exclusively of 'U's' are omitted.  
	 * 
	 * @param pdbFile		- original PDB FASTA file	  						 
	 * @param formattedFile	- FASTA formatted file
	 */
	public static void pdbFastaFormatting(File pdbFile, File formattedFile) {
		try {

			PrintWriter pw = new PrintWriter(new FileWriter(formattedFile));

			FastaReader fr = new FastaReader(pdbFile);
			ArrayList<FastaEntry> entries = fr.getEntries();
			int countEntry = 0;
			for(FastaEntry entry: entries) {				

				String header = entry.getHeader();
				String sequence = entry.getSequence().replaceAll("\\s+", "");

				String tmp = sequence.replaceAll("[Uu]", "");
				//ignore sequences that are composed excl. of U's
				if(tmp.length() != 0) {
					countEntry++;
					pw.println(">"+countEntry+"|"+header+"\n"+sequence);
				}			

			}

			pw.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * 
	 * @param originalFile
	 * @param formattedFile
	 * @param familyFile - file that contains family assignment for each protein (format: 5ht1f_pantr     001_001_005_001_006)
	 */
	public static void gpcrdbFastaFormatting(File originalFile, File formattedFile, File familyFile) {
		try {

			Hashtable<String,String> protein2familyAssignment = new Hashtable<String,String>();
			BufferedReader br = new BufferedReader(new FileReader(familyFile));
			String line;
			while((line = br.readLine()) != null) {
				if(line.trim().equals("")) {
					continue;
				}

				String[] content = line.split("\\s+");
				String protein = content[0].trim();
				String family = content[1].trim();
				protein2familyAssignment.put(protein,family);
			}
			br.close();


			PrintWriter pw = new PrintWriter(new FileWriter(formattedFile));

			FastaReader fr = new FastaReader(originalFile);
			ArrayList<FastaEntry> entries = fr.getEntries();
			int countEntry = 0;
			for(FastaEntry entry: entries) {
				countEntry++;

				String header = entry.getHeader().trim();
				header = header.replaceAll("\\s\\$\\s?", "_");

				String family = protein2familyAssignment.get(header.trim());

				String sequence = entry.getSequence().replaceAll("\\s+", "");

				pw.println(">"+countEntry+"|"+header+"|"+family+"\n"+sequence);

			}

			pw.close();

		} catch(Exception e) {
			e.printStackTrace();
		}
	}


	public static void extopodbFastaFormatting(File originalFile, File formattedFile) {
		try {

			Pattern p = Pattern.compile("ExTopoDB:(\\d+)\\[Uniprot:.*\\]");

			PrintWriter pw = new PrintWriter(new FileWriter(formattedFile));

			FastaReader fr = new FastaReader(originalFile);
			ArrayList<FastaEntry> entries = fr.getEntries();
			int countEntry = 0;
			for(FastaEntry entry: entries) {
				countEntry++;

				String header = entry.getHeader().trim();
				String sequence = entry.getSequence().replaceAll("\\s+", "");

				Matcher m = p.matcher(header);

				if(m.matches()) {

					String sequenceid = m.group(1);
					pw.println(">"+sequenceid+"\n"+sequence);
				}
				else{
					System.out.println("WARNING: Header is not matching: " +header);
				}				
			}

			pw.close();

		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 
	 * @param fastaFile
	 * @param dbID
	 * Since the function is not working for drugbank... to add sequence in camps from drugbank
	 * using this function
	 */
	public static void addSeqsFromExternalDBs2CampsDrugBank(String fastaFile, int dbID) {
		try{
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("INSERT INTO sequences_other_database " +
					"(name, additional_information, sequenceid, md5, length, db, linkouturl) " +
					"VALUES " +
					"(?,?,?,?,?,?,?)");	

			ConfigFile cf = new ConfigFile("/home/users/saeed/workspace/CAMPS3/config/config.xml");
			//ConfigFile cf = new ConfigFile("F:/eclipse_workspace/CAMPS3/config/config.xml");

			String linkout = "";
			if(dbID == OtherDatabase.DRUGBANK) {
				linkout = cf.getProperty("linkout:drugbank");
			}
			FastaReader fr = new FastaReader(new File(fastaFile));
			ArrayList<FastaEntry> entries = fr.getEntries();
			int i =0; 
			for(FastaEntry entry: entries) {
				i++;
				if (i%100==0){
					System.out.print(".");
				}
				if (i%1000==0){
					System.out.print("\n");

				}
				String header = entry.getHeader();
				String sequence = entry.getSequence();

				int id = Integer.parseInt(header.split("\\|")[0]);
				String md5 = computeMD5Hash(sequence);
				int length = sequence.length();

				if(dbID == OtherDatabase.DRUGBANK) {
					//>2|drugbank_target|P19113 Histidine decarboxylase (DB00114; DB00117)
					
					String[] splitOne = header.split("\\|");
					//>2|
					//drugbank_target
					//|P19113 Histidine decarboxylase (DB00114; DB00117)
					
					String[] splitTwo = splitOne[2].split("\\(DB");
					
						String name = splitTwo[0].trim();
						String links = "DB"+splitTwo[1].trim();
						int l = links.length();
						links = links.substring(0, l-1);
						
						String[] linkList = links.split(";");

						for(String link: linkList) {
							String linkouturl = linkout+link.trim();

							pstm.setString(1, name);
							pstm.setString(2, link.trim()); // additional info
							pstm.setInt(3, id);				// sequenceid
							pstm.setString(4, md5);			// md5	
							pstm.setInt(5, length);
							pstm.setString(6, "drugbank");
							pstm.setString(7, linkouturl);

							pstm.executeUpdate();
						}
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	/**
	 * Reads specified FASTA file with sequences from external database and writes
	 * them to CAMPS database.
	 * 
	 * @param fastaFile - multiple FASTA file containing sequences from external database
	 * @param dbID    - id of external database
	 * @param p			- pattern to extract sequence name and sequence description
	 * 					  (must contain two groupings !!!; first for name, second for description)
	 */
	public static void addSeqsFromExternalDBs2Camps(String fastaFile, int dbID) {
		try {			

			Pattern drugbankP = Pattern.compile("\\d+\\|drugbank_target\\|\\d+\\s+(.*)\\((.*)\\)");
			Pattern gpcrdbP = Pattern.compile("\\d+\\|(.*)\\|(.*)");
			Pattern mptopoP = Pattern.compile("\\d+\\|.*\\|(.*)\\|(.*)");
			Pattern omimP = Pattern.compile("\\d+\\|(.*)\\|.*\\|.*\\|(.*)");
			Pattern omim2P = Pattern.compile("(\\d+)\\s*;\\s*(.*?)\\.");
			Pattern opmP = Pattern.compile("\\d+\\|((\\w+)_\\w)\\|.*\\|(.*)");
			Pattern pdbtmP = Pattern.compile("\\d+\\|((\\w+)_\\w)");
			Pattern targetdbP = Pattern.compile("\\d+\\|(.*?)_.*_.*_.*_");
			Pattern tcdbP = Pattern.compile("\\d+\\|gnl\\|TC-DB\\|.*?(\\d+\\.\\w\\.\\d+\\.\\d+\\.\\d+).*");
			Pattern topdbP = Pattern.compile("\\d+\\|(.*)");
			Pattern vfdbP = Pattern.compile("\\d+\\|(.*?)\\s+\\(.*");

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("INSERT INTO sequences_other_database " +
					"(name, additional_information, sequenceid, md5, length, db, linkouturl) " +
					"VALUES " +
					"(?,?,?,?,?,?,?)");	


			//ConfigFile cf = new ConfigFile("/home/users/saeed/workspace/CAMPS3/config/config.xml");
			ConfigFile cf = new ConfigFile("F:/eclipse_workspace/CAMPS3/config/config.xml");

			String linkout;
			if(dbID == OtherDatabase.DRUGBANK) {
				linkout = cf.getProperty("linkout:drugbank");
			}
			else if(dbID == OtherDatabase.GPCRDB) {
				linkout = cf.getProperty("linkout:gpcrdb");
			}
			else if(dbID == OtherDatabase.MPTOPO) {
				linkout = cf.getProperty("linkout:mptopo");
			}
			else if(dbID == OtherDatabase.OMIM) {
				linkout = cf.getProperty("linkout:omim");
			}
			else if(dbID == OtherDatabase.OPM) {
				linkout = cf.getProperty("linkout:opm");
			}
			else if(dbID == OtherDatabase.PDBTM) {
				linkout = cf.getProperty("linkout:pdbtm");
			}
			else if(dbID == OtherDatabase.TARGETDB) {
				linkout = cf.getProperty("linkout:targetdb");
			}
			else if(dbID == OtherDatabase.TCDB) {
				linkout = cf.getProperty("linkout:tcdb");
			}
			else if(dbID == OtherDatabase.TOPDB) {
				linkout = cf.getProperty("linkout:topdb");
			}
			else if(dbID == OtherDatabase.VFDB) {
				linkout = cf.getProperty("linkout:vfdb");
			}
			else {
				System.err.println("Unknown database id: " +dbID);
				return;
			}

			FastaReader fr = new FastaReader(new File(fastaFile));
			ArrayList<FastaEntry> entries = fr.getEntries();
			int i =0; 
			for(FastaEntry entry: entries) {
				i++;
				if (i%100==0){
					System.out.print(".");
				}
				if (i%1000==0){
					System.out.print("\n");

				}
				String header = entry.getHeader();
				String sequence = entry.getSequence();

				int id = Integer.parseInt(header.split("\\|")[0]);
				String md5 = computeMD5Hash(sequence);
				int length = sequence.length();

				//System.out.println(header);

				if(dbID == OtherDatabase.DRUGBANK) {
					Matcher m = drugbankP.matcher(header);

					if(m.matches()) {
						String name = m.group(1).trim();
						String links = m.group(2).trim();
						String[] linkList = links.split(";");

						for(String link: linkList) {
							String linkouturl = linkout+link.trim();

							pstm.setString(1, name);
							pstm.setString(2, link.trim());
							pstm.setInt(3, id);
							pstm.setString(4, md5);				
							pstm.setInt(5, length);
							pstm.setString(6, "drugbank");
							pstm.setString(7, linkouturl);

							pstm.executeUpdate();
						}
					}
				}
				else if(dbID == OtherDatabase.GPCRDB) {
					Matcher m = gpcrdbP.matcher(header);

					if(m.matches()) {
						String name = m.group(1).trim();
						String link = m.group(1).trim();
						String linkouturl = linkout+link;
						String additionalInfo = m.group(2).trim();

						pstm.setString(1, name);
						pstm.setString(2, additionalInfo);
						pstm.setInt(3, id);
						pstm.setString(4, md5);				
						pstm.setInt(5, length);
						pstm.setString(6, "gpcrdb");
						pstm.setString(7, linkouturl);

						pstm.executeUpdate();
					}
				}
				else if(dbID == OtherDatabase.MPTOPO) {
					Matcher m = mptopoP.matcher(header);

					if(m.matches()) {
						String name = m.group(1).trim();
						String link = m.group(2).trim();
						String linkouturl = linkout+link;
						String additionalInfo = null;

						pstm.setString(1, name);
						pstm.setString(2, additionalInfo);
						pstm.setInt(3, id);
						pstm.setString(4, md5);				
						pstm.setInt(5, length);
						pstm.setString(6, "mptopo");
						pstm.setString(7, linkouturl);

						pstm.executeUpdate();
					}
				}
				else if(dbID == OtherDatabase.OMIM) {
					Matcher m = omimP.matcher(header);

					if(m.matches()) {
						String name = m.group(1).trim();
						String links = m.group(2).trim();						


						Matcher m2 = omim2P.matcher(links);
						int start = 0;						
						while(start <= links.length() && m2.find(start)) {

							String link = m2.group(1).trim();
							String linkouturl = linkout+link;
							String additionalInfo = link+":"+m2.group(2).trim();

							pstm.setString(1, name);
							pstm.setString(2, additionalInfo);
							pstm.setInt(3, id);
							pstm.setString(4, md5);				
							pstm.setInt(5, length);
							pstm.setString(6, "omim");
							pstm.setString(7, linkouturl);

							pstm.executeUpdate();

							start = m2.end();			
						}						
					}					
				}
				else if(dbID == OtherDatabase.OPM) {
					Matcher m = opmP.matcher(header);

					if(m.matches()) {
						String name = m.group(1).trim();
						String link = m.group(2).trim();
						String linkouturl = linkout+link;
						String additionalInfo = m.group(3).trim();

						pstm.setString(1, name);
						pstm.setString(2, additionalInfo);
						pstm.setInt(3, id);
						pstm.setString(4, md5);				
						pstm.setInt(5, length);
						pstm.setString(6, "opm");
						pstm.setString(7, linkouturl);

						pstm.executeUpdate();
					}
				}
				else if(dbID == OtherDatabase.PDBTM) {
					Matcher m = pdbtmP.matcher(header);

					if(m.matches()) {
						String name = m.group(1).trim();
						String link = m.group(2).trim();
						String linkouturl = linkout+link;
						String additionalInfo = null;

						pstm.setString(1, name);
						pstm.setString(2, additionalInfo);
						pstm.setInt(3, id);
						pstm.setString(4, md5);				
						pstm.setInt(5, length);
						pstm.setString(6, "pdbtm");
						pstm.setString(7, linkouturl);

						pstm.executeUpdate();
					}
				}
				else if(dbID == OtherDatabase.TARGETDB) {
					Matcher m = targetdbP.matcher(header);

					if(m.matches()) {
						String name = m.group(1).trim();
						String link = m.group(1).trim();
						String linkouturl = linkout+link;
						String additionalInfo = null;

						pstm.setString(1, name);
						pstm.setString(2, additionalInfo);
						pstm.setInt(3, id);
						pstm.setString(4, md5);				
						pstm.setInt(5, length);
						pstm.setString(6, "targetdb");
						pstm.setString(7, linkouturl);

						pstm.executeUpdate();
					}
				}
				else if(dbID == OtherDatabase.TCDB) {
					Matcher m = tcdbP.matcher(header);

					if(m.matches()) {
						String name = m.group(1).trim();
						String link = m.group(1).trim();
						String linkouturl = linkout+link;
						String additionalInfo = m.group(1).trim();

						pstm.setString(1, name);
						pstm.setString(2, additionalInfo);
						pstm.setInt(3, id);
						pstm.setString(4, md5);				
						pstm.setInt(5, length);
						pstm.setString(6, "tcdb");
						pstm.setString(7, linkouturl);

						pstm.executeUpdate();
					}
				}
				else if(dbID == OtherDatabase.TOPDB) {
					Matcher m = topdbP.matcher(header);

					if(m.matches()) {
						String name = m.group(1).trim();
						String link = m.group(1).trim();
						String linkouturl = linkout+link;
						String additionalInfo = null;

						pstm.setString(1, name);
						pstm.setString(2, additionalInfo);
						pstm.setInt(3, id);
						pstm.setString(4, md5);				
						pstm.setInt(5, length);
						pstm.setString(6, "topdb");
						pstm.setString(7, linkouturl);

						pstm.executeUpdate();
					}
				}
				else if(dbID == OtherDatabase.VFDB) {
					Matcher m = vfdbP.matcher(header);

					if(m.matches()) {
						String name = m.group(1).trim();
						String link = m.group(1).trim();
						String linkouturl = linkout+link;
						String additionalInfo = null;

						pstm.setString(1, name);
						pstm.setString(2, additionalInfo);
						pstm.setInt(3, id);
						pstm.setString(4, md5);				
						pstm.setInt(5, length);
						pstm.setString(6, "vfdb");
						pstm.setString(7, linkouturl);

						pstm.executeUpdate();
					}
				}				
			}


			pstm.close();

		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	private static String computeMD5Hash(String sequence) {
		// Get md5 digest of the sequence
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			System.out.println("MD5 digest algorithm unavailable.");
			System.exit(1);
		}
		md5.update(sequence.toUpperCase().getBytes());
		BigInteger md5hash = new BigInteger(1, md5.digest());
		String sequence_md5 = md5hash.toString(16);
		// leading zeros are added to make a 32 digit String
		while (sequence_md5.length() < 32) {
			sequence_md5 = "0" + sequence_md5;
		}
		return sequence_md5.toLowerCase();
	}



	/**
	 * 
	 * Writes CAMPS dataset in blocks of splitSize sequences to specified 
	 * output directory in FASTA format.
	 */
	public static void camps2fasta(String outdir, int splitSize) {
		try {

			int currentBlockSize = 0;
			int fileCount = 1;

			PrintWriter pw = new PrintWriter(new FileWriter(new File(outdir+"camps4_part"+fileCount+".fasta")));

			Statement stm = CAMPS_CONNECTION.createStatement();
			stm.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = stm.executeQuery("SELECT sequenceid,sequence FROM sequences2");
			while(rs.next()) {
				currentBlockSize++;

				int sequenceid = rs.getInt("sequenceid");
				String sequence = rs.getString("sequence");

				pw.println(">"+sequenceid+"\n"+sequence);

				if(currentBlockSize % splitSize == 0) {

					pw.close();					

					fileCount++;

					pw = new PrintWriter(new FileWriter(new File(outdir+"camps4_part"+fileCount+".fasta")));

				}
			}
			rs.close();

			pw.close();

		} catch(Exception e) {
			e.printStackTrace();
		}
	}


	public static void createBLASTSHFiles(String otherDBDir, String shOutdir, String blastOutdir) {
		try {

			String campsDBDir = "/home/proj/check/otherdatabase/camps_blastFormatted/";
			int campsBlocks = 6;

			int fileCount = 0;

			File[] files = new File(otherDBDir).listFiles();
			for(File file: files) {

				String queryName = file.getName().split("\\.")[0];

				for(int i=1; i<=campsBlocks; i++) {

					String dbName = "camps4_part"+i+".fasta";

					fileCount++;
					PrintWriter pw = new PrintWriter(new FileWriter(new File(shOutdir+"RunBLAST"+fileCount+".sh")));

					pw.println(". /etc/profile");
					pw.println();
					pw.println("mkdir -p /tmp/usman/");
					pw.println("lockfile -10 -r -1 -l 3600 /tmp/usman/rsync.lock");
					pw.println("rsync -a "+campsDBDir+dbName+"* /tmp/usman/");
					pw.println("rm -f /tmp/usman/rsync.lock");
					pw.println();
					pw.println("/home/software/bin/blastall -p blastp -a 2 -e 0.001 -d /tmp/usman/"+dbName+" -i "+file.getAbsolutePath()+" -m 9 -o "+blastOutdir+"camps4_part"+i+"_"+queryName+".blast");

					pw.close();
				}

			}

			File[] shFiles = new File(shOutdir).listFiles();

			PrintWriter pw = new PrintWriter(new FileWriter(new File(shOutdir+"submitAllBLASTs.sh")));
			for(File shFile: shFiles) {

				if(shFile.getName().startsWith("RunBLAST")) {
					// - p priority
					// -pe parallel env
					pw.println("qsub -l vf=900m -p -10 "+shFile.getAbsolutePath());
				}	

			}
			pw.close();

		} catch(Exception e) {
			e.printStackTrace();
		}
	}



	/**
	 * 
	 * Extracts all hits from the specified BLAST result file (tabular format!) 
	 * that satisfy the following criteria:
	 * 
	 * - minimal sequence identity: 40 %
	 * - coverage of predicted TMHs (in query sequence == CAMPS sequence): >= 2 TMH, >= 40% of all TMHs
	 * - evalue <= 1E-3 
	 * 
	 * @param blastFile	- tabular BLAST output file
	 * @param dbName	- name of database used in BLAST run
	 */
	/*
	public static void extractHitsFromBlastResults(String blastFile, String dbName) {
		try {

			//number of records for insertion of multiple rows at once
			int MULTIROW_INSERT_SIZE = 1000;


			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT begin,end " +
					"FROM tms " +
					"WHERE sequenceid=?");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT length FROM sequences2 " +
					"WHERE sequenceid=?");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"SELECT length FROM sequences_other_database " +
					"WHERE sequenceid=? AND db=?");


			//statement template for multirow insert		
			String sqlInsert = "INSERT INTO other_database" +
			" (sequenceid, sequences_other_database_sequenceid, alignment_length, query_start, query_end, query_coverage, hit_start, hit_end, hit_coverage, bitscore, evalue, ident, db)" +
			" VALUES ";

			Statement stmImport = CAMPS_CONNECTION.createStatement();
			StringBuffer multirowInsert = new StringBuffer(); 
			int currentmultirowInsertSize = 0;

			BufferedReader br = new BufferedReader(new FileReader(new File(blastFile)));
			String line;
			while((line = br.readLine()) != null) {
				if(line.startsWith("#")) {		//comment line in tabular BLAST format
					continue;
				}

				String[] content = line.split("\t");

				String query = content[0];
				int sequenceidQuery = Integer.parseInt(query.split("\\|")[0]); //id of external sequence
				int sequenceidHit = Integer.parseInt(content[1]);  //id of CAMPS sequence

				float identity = Float.parseFloat(content[2]);
				int alignmentLength = Integer.parseInt(content[3]);
				int queryStart = Integer.parseInt(content[6]);  //external seq
				int queryEnd = Integer.parseInt(content[7]);	//external seq	
				int hitStart = Integer.parseInt(content[8]);  //CAMPS seq
				int hitEnd = Integer.parseInt(content[9]);	//CAMPS seq	
				double evalue = Double.parseDouble(content[10]); 
				float bitScore = Float.parseFloat(content[11]);



				if(identity < MIN_IDENTITY || evalue > EVALUE_THRESHOLD) {
					continue;
				}

				ArrayList<TMS> tmsArrHit = new ArrayList<TMS>();
        		pstm1.setInt(1,sequenceidHit);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					int start = rs1.getInt("begin");
					int end = rs1.getInt("end");
					TMS tms = new TMS(start,end);
					tmsArrHit.add(tms);
				}
				rs1.close();
				rs1 = null;

				int coveredTmsHit = 0;
				for(TMS tms:tmsArrHit) {
					int tms_start = tms.get_start();
					int tms_end = tms.get_end();
					if((tms_start >= (queryStart - 5)) && (tms_end <= (queryEnd + 5))) {
						coveredTmsHit++;
					}
				}
				double coveredTmsPercHit = (coveredTmsHit*100)/((double) tmsArrHit.size());


				if(coveredTmsHit >= MIN_TMH_COVERAGE_TOTAL && coveredTmsPercHit >= MIN_TMH_COVERAGE_PERC) {


					//compute coverage
					int hitLength = -1;
					pstm2.setInt(1, sequenceidHit);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						hitLength = rs2.getInt("length");
					}
					rs2.close();

					int matchLengthHit = (hitEnd - hitStart) + 1;
					float coverageHit = 100 * (matchLengthHit/(float) hitLength);
					BigDecimal bd1 = new BigDecimal(coverageHit);
					bd1 = bd1.setScale(2, BigDecimal.ROUND_HALF_UP);
					float coverageHitRounded = bd1.floatValue();

					int queryLength = -1;
					pstm3.setInt(1, sequenceidQuery);
					pstm3.setString(2, dbName);
					ResultSet rs3 = pstm3.executeQuery();
					while(rs3.next()) {
						queryLength = rs3.getInt("length");
					}
					rs3.close();

					int matchLengthQuery = (queryEnd - queryStart) + 1;
					float coverageQuery = 100 * (matchLengthQuery/(float) queryLength);
					BigDecimal bd2 = new BigDecimal(coverageQuery);
					bd2 = bd2.setScale(2, BigDecimal.ROUND_HALF_UP);
					float coverageQueryRounded = bd2.floatValue();


					currentmultirowInsertSize++;

					//
					//please note: BLAST runs were prepared such that 
					//CAMPS sequences make up the BLAST database
					//
					// => query in BLAST result file is sequence from external db
					// => hit in BLAST result is CAMPS sequence
					//
					String insert = "("+sequenceidHit+","+
					sequenceidQuery+","+
					alignmentLength+","+
					queryStart+","+
					queryEnd+","+
					coverageQueryRounded+","+
					hitStart+","+
					hitEnd+","+
					coverageHitRounded+","+
					bitScore+","+
					evalue+","+
					identity+","+
					"\""+dbName+"\""+")";

					multirowInsert.append(","+insert);

					if(currentmultirowInsertSize % MULTIROW_INSERT_SIZE == 0) {
						String fullInsertStatement = sqlInsert+multirowInsert.toString().substring(1);
						stmImport.executeUpdate(fullInsertStatement);

						//reset all data
						multirowInsert = new StringBuffer(); 
					}										
				}
			}

			br.close();		


			pstm1.close();
			pstm2.close();
			pstm3.close();	

			//insert remaining records
			if(multirowInsert.length() != 0) {
				String fullInsertStatement = sqlInsert+multirowInsert.toString().substring(1);
				stmImport.executeUpdate(fullInsertStatement);
				multirowInsert = new StringBuffer(); 
			}

			stmImport.close();


		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	 */
	public static void extractHitsFromBlastResults(String blastFile, String dbName) {
		try {

			//number of records for insertion of multiple rows at once
			int MULTIROW_INSERT_SIZE = 1000;


			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
			//		"SELECT sequenceid,tms_id,begin,end " +
			//		"FROM tms " );
			HashMap <Integer, TMS_map> pstm1nd2_map = new HashMap<Integer,TMS_map>(); // key sequenceId - value Begin&End

			//PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
			//		"SELECT sequenceid,length FROM sequences2 ");


			//PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
			//		"SELECT length FROM sequences_other_database " +
			//		"WHERE sequenceid=? AND db=?");
			HashMap <String, OtherDbLen> pstm3_map = new HashMap<String,OtherDbLen>(); // sequenceId - length			

			//statement template for multirow insert		
			String sqlInsert = "INSERT INTO other_database" +
					" (sequenceid, sequences_other_database_sequenceid, alignment_length, query_start, query_end, query_coverage, hit_start, hit_end, hit_coverage, bitscore, evalue, ident, db)" +
					" VALUES ";

			Statement stmImport = CAMPS_CONNECTION.createStatement();
			StringBuffer multirowInsert = new StringBuffer(); 
			int currentmultirowInsertSize = 0;
			// populate pstm1nd2
			System.out.print("Populating map1\n");
			pstm1nd2_map = populatepstm1(pstm1nd2_map);
			System.out.print("Populating map2\n");
			// populate pstm3
			pstm3_map = populatepstm3(pstm3_map);
			System.out.print("Populating map2 Complete Now Reading file \n");
			BufferedReader br = new BufferedReader(new FileReader(new File(blastFile)));
			String line;
			while((line = br.readLine()) != null) {
				if(line.startsWith("#")) {		//comment line in tabular BLAST format
					continue;
				}

				String[] content = line.split("\t");

				String query = content[0];
				int sequenceidQuery = Integer.parseInt(query.split("\\|")[0]); //id of external sequence
				int sequenceidHit = Integer.parseInt(content[1]);  //id of CAMPS sequence

				float identity = Float.parseFloat(content[2]);
				int alignmentLength = Integer.parseInt(content[3]);
				int queryStart = Integer.parseInt(content[6]);  //external seq
				int queryEnd = Integer.parseInt(content[7]);	//external seq	
				int hitStart = Integer.parseInt(content[8]);  //CAMPS seq
				int hitEnd = Integer.parseInt(content[9]);	//CAMPS seq	
				double evalue = Double.parseDouble(content[10]); 
				float bitScore = Float.parseFloat(content[11]);



				if(identity < MIN_IDENTITY || evalue > EVALUE_THRESHOLD) {
					continue;
				}

				ArrayList<TMS> tmsArrHit = new ArrayList<TMS>();
				TMS_map temp = pstm1nd2_map.get(sequenceidHit);

				for (int i =0;i<=temp.tms_id.size()-1;i++){
					int start = temp.start.get(i);
					int end = temp.end.get(i);
					TMS tms = new TMS(start,end);
					tmsArrHit.add(tms);
				}

				int coveredTmsHit = 0;
				for(TMS tms:tmsArrHit) {
					int tms_start = tms.get_start();
					int tms_end = tms.get_end();
					if((tms_start >= (queryStart - 5)) && (tms_end <= (queryEnd + 5))) {
						coveredTmsHit++;
					}
				}
				double coveredTmsPercHit = (coveredTmsHit*100)/((double) tmsArrHit.size());


				if(coveredTmsHit >= MIN_TMH_COVERAGE_TOTAL && coveredTmsPercHit >= MIN_TMH_COVERAGE_PERC) {


					//compute coverage
					int hitLength = -1;
					TMS_map temp2 = pstm1nd2_map.get(sequenceidHit);
					hitLength = temp2.length;


					int matchLengthHit = (hitEnd - hitStart) + 1;
					float coverageHit = 100 * (matchLengthHit/(float) hitLength);
					BigDecimal bd1 = new BigDecimal(coverageHit);
					bd1 = bd1.setScale(2, BigDecimal.ROUND_HALF_UP);
					float coverageHitRounded = bd1.floatValue();

					int queryLength = -1;
					queryLength = pstm3_map.get(dbName).db.get(sequenceidQuery);


					int matchLengthQuery = (queryEnd - queryStart) + 1;
					float coverageQuery = 100 * (matchLengthQuery/(float) queryLength);
					BigDecimal bd2 = new BigDecimal(coverageQuery);
					bd2 = bd2.setScale(2, BigDecimal.ROUND_HALF_UP);
					float coverageQueryRounded = bd2.floatValue();


					currentmultirowInsertSize++;

					//
					//please note: BLAST runs were prepared such that 
					//CAMPS sequences make up the BLAST database
					//
					// => query in BLAST result file is sequence from external db
					// => hit in BLAST result is CAMPS sequence
					//
					String insert = "("+sequenceidHit+","+
							sequenceidQuery+","+
							alignmentLength+","+
							queryStart+","+
							queryEnd+","+
							coverageQueryRounded+","+
							hitStart+","+
							hitEnd+","+
							coverageHitRounded+","+
							bitScore+","+
							evalue+","+
							identity+","+
							"\""+dbName+"\""+")";

					multirowInsert.append(","+insert);

					if(currentmultirowInsertSize % MULTIROW_INSERT_SIZE == 0) {
						String fullInsertStatement = sqlInsert+multirowInsert.toString().substring(1);
						stmImport.executeUpdate(fullInsertStatement);

						//reset all data
						multirowInsert = new StringBuffer(); 
					}										
				}
			}

			br.close();		


			//insert remaining records
			if(multirowInsert.length() != 0) {
				String fullInsertStatement = sqlInsert+multirowInsert.toString().substring(1);
				stmImport.executeUpdate(fullInsertStatement);
				multirowInsert = new StringBuffer(); 
			}

			stmImport.close();


		} catch(Exception e) {
			e.printStackTrace();
		}
	}



	private static HashMap<String, OtherDbLen> populatepstm3(
			HashMap<String, OtherDbLen> pstm3_map) {
		// TODO Auto-generated method stub
		try {
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid,db,length FROM sequences_other_database ");
			ResultSet rs = pstm3.executeQuery();
			int x =0;
			while(rs.next()){
				x++;
				int id = rs.getInt("sequenceid");
				String db = rs.getString("db");
				int len = rs.getInt("length");
				if (pstm3_map.containsKey(db)){
					OtherDbLen temp = pstm3_map.get(db);

					temp.db.put(id, len);
					pstm3_map.put(db, temp);

				}
				else{
					OtherDbLen temp = new OtherDbLen();
					temp.db.put(id, len);
					pstm3_map.put(db, temp);
				}
				if (x%100000 ==0){
					System.out.print("Complete "+x+"\n");
				}
			}
			rs.close();
			pstm3.close();

			return pstm3_map;
		}
		catch(Exception e){
			e.printStackTrace();
		}


		return null;
	}


	private static HashMap<Integer, TMS_map> populatepstm1(HashMap<Integer, TMS_map> pstm1nd2_map) {
		// TODO Auto-generated method stub


		pstm1nd2_map = new HashMap<Integer,TMS_map>(); // key sequenceId - value Begin&End
		try{
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid,tms_id,begin,end " +"FROM tms "+
					"ORDER BY tms_id " );


			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid,length FROM sequences2 ");
			int x =0;
			ResultSet rs = pstm1.executeQuery();
			while (rs.next()){
				x++;
				int id = rs.getInt("sequenceid");
				int tms_id = rs.getInt("tms_id");
				int st = rs.getInt("begin");
				int end = rs.getInt("end");

				if (pstm1nd2_map.containsKey(id)){
					TMS_map temp = pstm1nd2_map.get(id);
					temp.start.add(st);
					temp.end.add(end);
					temp.tms_id.add(tms_id);
					pstm1nd2_map.put(id, temp);
				}
				else{
					TMS_map temp = new TMS_map();
					temp.start.add(st);
					temp.end.add(end);
					temp.tms_id.add(tms_id);
					pstm1nd2_map.put(id, temp);
				}
				if (x%100000 ==0){
					System.out.print("Complete "+x+"\n");
				}
			}
			rs.close();
			ResultSet rs2 = pstm2.executeQuery(); // sequenceid length
			System.out.print("Getting lengths \n");
			while (rs2.next()){
				int id = rs2.getInt("sequenceid");
				int length = rs2.getInt("length");

				TMS_map temp = pstm1nd2_map.get(id);
				temp.length = length;


				pstm1nd2_map.put(id,temp);
			}
			rs2.close();
			pstm1.close();
			pstm2.close();

			return pstm1nd2_map;
		}
		catch(Exception e){
			e.printStackTrace();
		}


		return null;
	}


	public static void fillTableOtherDatabaseHierarchies(String hierarchyFile, String db) {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO other_database_hierarchies " +
							"(key_, description, hierarchy, comment, db) " +
							"VALUES " +
					"(?,?,?,?,?)");

			BufferedReader br = new BufferedReader(new FileReader(new File(hierarchyFile)));
			String line;
			int i =0;
			while((line = br.readLine()) != null) {
				if(line.startsWith("#")) {
					continue;
				}

				String[] content = line.split("\t");

				String hierarchy = content[0];
				String key = content[1];
				String description = content[2];

				String comment = null;
				if(content.length > 3) {
					comment = content[3];
				}		


				pstm.setString(1, key);
				pstm.setString(2, description);
				pstm.setString(3, hierarchy);
				pstm.setString(4, comment);
				pstm.setString(5, db);

				pstm.executeUpdate();
				i++;
				if (i%100==0){
					//System.out.print(i+"\n");
				}
			}
			br.close();

			pstm.close();

		}catch(Exception e) {
			e.printStackTrace();

		}
	}


	public static void disconnect() {
		try {
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	/*
	 * due to parsing errors this method was added on Apr,4 2011
	 * 
	 * example: 223604|my_001000402.1_RSGI_SQ81687_PDBT223600_
	 *          using pattern sequence name was set to 'my' instead of 'my_001000402.1'
	 */
	public static void addSeqsFromTargetdb2Camps(String fastaFile) {
		try {			

			//
			//please note: this list must be updated if another FASTA targetdb file
			//is used!!!
			//
			ArrayList<String> contributors = new ArrayList<String>();
			contributors.add("_BSGC_");
			contributors.add("_SECSG_");
			contributors.add("_JCSG_");
			contributors.add("_MCSG_");
			contributors.add("_NESGC_");
			contributors.add("_NYSGXRC_");
			contributors.add("_TB_");
			contributors.add("_CESG_");
			contributors.add("_SGPP_");
			contributors.add("_YSG_");
			contributors.add("_RSGI_");
			contributors.add("_BSGI_");
			contributors.add("_S2F_");
			contributors.add("_MSGP_");
			contributors.add("_BIGS_");
			contributors.add("_OPPF_");
			contributors.add("_ISPC_");
			contributors.add("_ISFI_");
			contributors.add("_XMTB_");
			contributors.add("_SPINE_");
			contributors.add("_SGC_");
			contributors.add("_CHTSB_");
			contributors.add("_CSMP_");
			contributors.add("_NYCOMPS_");
			contributors.add("_SSGCID_");
			contributors.add("_ATCG3D_");
			contributors.add("_CSGID_");
			contributors.add("_OCSP_");
			contributors.add("_SGX_");


			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("INSERT INTO sequences_other_database " +
					"(name, additional_information, sequenceid, md5, length, db, linkouturl) " +
					"VALUES " +
					"(?,?,?,?,?,?,?)");	


			ConfigFile cf = new ConfigFile("/home/users/saeed/workspace/CAMPS3/config/config.xml");
			String linkout = cf.getProperty("linkout:targetdb");


			FastaReader fr = new FastaReader(new File(fastaFile));
			ArrayList<FastaEntry> entries = fr.getEntries();
			for(FastaEntry entry: entries) {
				String header = entry.getHeader();
				String sequence = entry.getSequence();

				int id = Integer.parseInt(header.split("\\|")[0]);
				String md5 = computeMD5Hash(sequence);
				int length = sequence.length();

				String fullName = header.split("\\|")[1];
				String shortName = null;

				for(String contributor: contributors) {

					if(fullName.contains(contributor)) {

						String[] tmp = fullName.split(contributor);
						shortName = tmp[0];
					}

				}

				String linkouturl = linkout+shortName;

				pstm.setString(1, shortName);
				pstm.setString(2, fullName);
				pstm.setInt(3, id);
				pstm.setString(4, md5);				
				pstm.setInt(5, length);
				pstm.setString(6, "targetdb");
				pstm.setString(7, linkouturl);

				pstm.executeUpdate();

			}


			pstm.close();

		} catch(Exception e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) {
		/*
		System.out.print("Formating all databases \n");
		//System.exit(0);

		generalFastaFormatting(new File("/home/proj/check/otherdatabase/drugbank/all_target.fasta"), new File("/home/proj/check/otherdatabase/FASTAformatted/drugbank.fasta"));
//		
		gpcrdbFastaFormatting(new File("/home/proj/check/otherdatabase/gpcrdb/all_gpcr.fasta"),new File("/home/proj/check/otherdatabase/FASTAformatted/gpcrdb.fasta"),new File("/home/proj/check/otherdatabase/gpcrdb/familyFile.txt"));
//		
//		mptopoFastaFormatting(new File("/home/proj/Camps3/other_database/download/mptopodownload.txt"), new File("/home/proj/Camps3/other_database/fasta/mptopo.fasta"));
//		
		generalFastaFormatting(new File("/home/proj/check/otherdatabase/omim/omim.fasta"), new File("/home/proj/check/otherdatabase/FASTAformatted/omim.fasta"));
//		
		opmFastaFormatting(new File("/home/proj/check/otherdatabase/opm/opm_tm.txt"), new File("/home/proj/check/otherdatabase/FASTAformatted/opm.fasta"));
//		**
		pdbFastaFormatting(new File("/home/proj/check/otherdatabase/pdbtm/pdbtm_alpha.seq"), new File("/home/proj/check/otherdatabase/FASTAformatted/pdbtm.fasta"));
//		**
		generalFastaFormatting(new File("/home/proj/check/otherdatabase/targetdb/targets.fa"), new File("/home/proj/check/otherdatabase/FASTAformatted/targetdb.fasta"));
//		**
		generalFastaFormatting(new File("/home/proj/check/otherdatabase/tcdb/tcdb.fasta"), new File("/home/proj/check/otherdatabase/FASTAformatted/tcdb.fasta"));
//		**
		generalFastaFormatting(new File("/home/proj/check/otherdatabase/vfdb/VFs.faa"), new File("/home/proj/check/otherdatabase/FASTAformatted/vfdb.fasta"));
//		**
		generalFastaFormatting(new File("/home/proj/check/otherdatabase/topdb/topdb_all.txt"), new File("/home/proj/check/otherdatabase/FASTAformatted/topdb.fasta"));

		System.out.print("Formating all databases Complete \n");

		//CreateDatabaseTables.create_table_sequences_other_database();
		//sequences_other_database

		System.out.print("Adding Sequences to CAMPS \n");

		addSeqsFromExternalDBs2Camps("/home/proj/check/otherdatabase/FASTAformatted/drugbank.fasta", OtherDatabase.DRUGBANK);
//		
		addSeqsFromExternalDBs2Camps("/home/proj/check/otherdatabase/FASTAformatted/gpcrdb.fasta", OtherDatabase.GPCRDB);
//
//		addSeqsFromExternalDBs2Camps("/home/proj/Camps3/other_database/fasta/mptopo.fasta", OtherDatabase.MPTOPO);
//
		addSeqsFromExternalDBs2Camps("/home/proj/check/otherdatabase/FASTAformatted/omim.fasta", OtherDatabase.OMIM);
//
		addSeqsFromExternalDBs2Camps("/home/proj/check/otherdatabase/FASTAformatted/opm.fasta", OtherDatabase.OPM);
//
		addSeqsFromExternalDBs2Camps("/home/proj/check/otherdatabase/FASTAformatted/pdbtm.fasta", OtherDatabase.PDBTM);
//
		//addSeqsFromExternalDBs2Camps("/home/proj/check/otherdatabase/FASTAformatted/targetdb.fasta", OtherDatabase.TARGETDB);
//
		addSeqsFromExternalDBs2Camps("/home/proj/check/otherdatabase/FASTAformatted/tcdb.fasta", OtherDatabase.TCDB);
//
		addSeqsFromExternalDBs2Camps("/home/proj/check/otherdatabase/FASTAformatted/topdb.fasta", OtherDatabase.TOPDB);
//
		addSeqsFromExternalDBs2Camps("/home/proj/check/otherdatabase/FASTAformatted/vfdb.fasta", OtherDatabase.VFDB);

		addSeqsFromTargetdb2Camps("/home/proj/check/otherdatabase/FASTAformatted/targetdb.fasta");

		System.out.print("Adding Sequences to CAMPS Complete \n");

		System.out.print("Spliting CAMPS \n");

		camps2fasta("/home/proj/check/otherdatabase/fasta_camps/", 175000);//1013102

		System.out.print("Spliting CAMPS Complete \n");

		createBLASTSHFiles("/home/proj/check/otherdatabase/FASTAformatted/", "/home/proj/check/otherdatabase/sh/", "/scratch/usman/Camps4/blastOutput/");

		System.out.print("Blast Files Creation Complete \n");
		 */
		//addSeqsFromExternalDBs2CampsDrugBank("/home/proj/check/otherdatabase/FASTAformatted/drugbank.fasta", OtherDatabase.DRUGBANK);
		//addSeqsFromExternalDBs2CampsDrugBank("F:/drugBank.fasta", OtherDatabase.DRUGBANK);
				//System.out.print("\nPopulating opm\n");
				//fillTableOtherDatabaseHierarchies("F:/Scratch/opm_hiearchy.txt", "opm");
				//System.out.print("\nPopulating tcdb\n");
				//fillTableOtherDatabaseHierarchies("F:/Scratch/tcdb_hierarchies.txt", "tcdb");
				//System.out.print("\nPopulating gpcrdb\n");
				//fillTableOtherDatabaseHierarchies("F:/Scratch/gpcr_hierarchy_formatted.txt", "gpcrdb");
				//System.out.print("\nComplete\n");

		//		disconnect();





		//		pdbFastaFormatting(new File("/home/proj/Camps3/other_database/download/pdbtm_alpha.seq"), new File("/home/proj/Camps3/other_database/fasta/pdbtm.fasta"));
		//		generalFastaFormatting(new File("/home/proj/Camps3/other_database/download/tcdb"), new File("/home/proj/Camps3/other_database/fasta/tcdb.fasta"));
		//		generalFastaFormatting(new File("/home/proj/Camps3/other_database/download/targets.fa"), new File("/home/proj/Camps3/other_database/fasta/targetdb.fasta"));


		//		addSeqsFromExternalDBs2Camps("/home/proj/Camps3/other_database/fasta/pdbtm.fasta", OtherDatabase.PDBTM);
		//		disconnect();

		//		addSeqsFromExternalDBs2Camps("/home/proj/Camps3/other_database/fasta/tcdb.fasta", OtherDatabase.TCDB);
		//		disconnect();

		//		fillTableOtherDatabaseHierarchies("/home/proj/Camps3/other_database/tcdb_hierarchies.txt", "tcdb");

		//		generalFastaFormatting(new File("/home/proj/Camps3/other_database/download/omim/omim_diseaseprot.txt"), new File("/home/proj/Camps3/other_database/fasta/omim.fasta"));

		//		fillTableOtherDatabaseHierarchies("/home/proj/Camps3/other_database/opm_hierarchies.txt", "opm");

		//		addSeqsFromExternalDBs2Camps("/home/proj/Camps3/other_database/fasta/omim.fasta", OtherDatabase.OMIM);
		//		disconnect();

		//		addSeqsFromExternalDBs2Camps("/home/proj/Camps3/other_database/fasta/targetdb.fasta", OtherDatabase.TARGETDB);
		//		disconnect();

		//		fillTableOtherDatabaseHierarchies("/home/proj/Camps3/other_database/gpcrdb_hierarchies.txt", "gpcrdb");


		//
		//Postprocessing on Apr, 4 2011
		//

		//		addSeqsFromTargetdb2Camps("/home/proj/Camps3/other_database/fasta/targetdb.fasta");


		//
		//Adding of ExtopoDB (Apr, 15 2011)
		//	
		//		extopodbFastaFormatting(new File("/home/proj/Camps3/other_database/download/extopodb/ExTopoDB.fasta"), new File("/home/proj/Camps3/other_database/fasta/extopodb.fasta"));
	}

}
