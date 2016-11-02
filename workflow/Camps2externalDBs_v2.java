package workflow;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

//import org.apache.axis.utils.StringUtils;
import org.biojava3.core.sequence.AccessionID;
import org.biojava3.core.sequence.ProteinSequence;
import org.biojava3.core.sequence.io.FastaReaderHelper;
import org.biojava3.core.sequence.template.SequenceMixin.SequenceIterator;

import utils.DBAdaptor;
import de.gsf.mips.simap.specialclients.FASTAReader;
import de.gsf.mips.simap.specialclients.SingleEntry;
import org.biojava.bio.*;
import org.biojava.bio.seq.*;
import org.biojava.bio.seq.io.*;

public class Camps2externalDBs_v2 {

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	private static final Connection SIMAP_CONNECTION = DBAdaptor.getConnection("simap");
	static Hashtable<String, Protein> camps4 = new Hashtable<String, Protein>();

	/*
	 * sprotFastaFile 	- FASTA file for SwissProt sequences
	 * tremblFastaFile	- FASTA file for TrEMBL sequences
	 * sprotDatFile     - Text file for all SwissProt entries (in gz format!)
	 * tremblDatFile    - Text file for all TrEMBL entries (in gz format!)
	 */
	
// the function puts the data from mapped uniprot files into camps2uniprot
	public static void mapFileToCamps(String f) {

		// this part handles witht the inconsistency and the problem in mapping file. Which had /t instead of \t
		try{
			BufferedReader br = new BufferedReader(new FileReader(f));
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO camps2uniprot " +
							"(sequenceid," +
							"entry_name," +
							"accession, " +
							"description, " +					
							"subset," +
							"linkouturl) " +
							"VALUES " +
					"(?,?,?,?,?,?)");

			String line;
			String[] parts = new String[5];

			int lineNumber=0;
			int batchCounter = 0;

			while ((line = br.readLine()) != null){
				//System.out.print(line + "\n");
				lineNumber++;
				for (int i =0; i<=3; i++){
					if (i>=3){
						int index = line.indexOf("/thttp");
						parts[i] = line.substring(0, index);
						line = line.substring(index+2);
						parts[i+1] = line;
					}
					else{
						int index = line.indexOf("/t");
						parts[i] = line.substring(0, index);
						line = line.substring(index+2);
					}
				}
				Integer seqId = Integer.parseInt(parts[0]);
				pstm2.setInt(1, seqId);
				pstm2.setString(2, parts[1]); //entry name
				pstm2.setString(3, parts[2]); // accession
				pstm2.setString(4, parts[3]); //description
				pstm2.setString(5, "trembl"); //ssubset
				pstm2.setString(6, parts[4]); //url
				batchCounter++;
				pstm2.addBatch();
				//String linein = sequenceID+"\t"+entryName+"\t"+accession+"\t"+description+"\t"+url;

				if (batchCounter % 1000 == 0){
					pstm2.executeBatch();
					pstm2.clearBatch();
					System.out.print(".");
					if (batchCounter % 100000 == 0){
						System.out.print("\n");
					}
				}
			}
			pstm2.executeBatch();
			pstm2.clearBatch();
			br.close();
			pstm2.close();
			System.out.print("\n Processsed line " + lineNumber);
		}
		catch(Exception e){
			e.printStackTrace();
		}

		// the commented out code below is for normal mapping files to put mapped data into CAMPS
		/*
		try{

			BufferedReader br = new BufferedReader(new FileReader(f));

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO camps2uniprot " +
							"(sequenceid," +
							"entry_name," +
							"accession, " +
							"description, " +					
							"subset," +
							"linkouturl) " +
							"VALUES " +
					"(?,?,?,?,?,?)");
			String line = "";
			int lineNumber=0;
			int batchCounter = 0;
			while ((line = br.readLine()) != null){
				lineNumber++;
				String parts [] = line.split("\t");
				if (parts.length < 5){
					System.out.print("REPORT "+ lineNumber + "\n");
				}
				else{
					Integer seqId = Integer.parseInt(parts[0]);
					pstm2.setInt(1, seqId);
					pstm2.setString(2, parts[1]); //entry name
					pstm2.setString(3, parts[2]); // accession
					pstm2.setString(4, parts[3]); //description
					pstm2.setString(5, "trembl"); //ssubset
					pstm2.setString(6, parts[4]); //url
					batchCounter++;
					pstm2.addBatch();
					//String linein = sequenceID+"\t"+entryName+"\t"+accession+"\t"+description+"\t"+url;
				}
				if (batchCounter % 1000 == 0){
					pstm2.executeBatch();
					pstm2.clearBatch();
					System.out.print(".");
					if (batchCounter % 100000 == 0){
						System.out.print("\n");
					}
				}
				//pstm2.execute();
			}
			pstm2.executeBatch();
			pstm2.clearBatch();
			br.close();
			pstm2.close();

		}
		catch (Exception e){
			e.printStackTrace();
		}
		finally {
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}	
			if (SIMAP_CONNECTION != null) {
				try {
					SIMAP_CONNECTION.close();
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}*/

	}
	

	// maps the camps proteins to uniprot using hash tables and puts them in MapFile
	// previously - made 26 parts of uniprot and tried to map
	//this however works better million times
	public static void camps2uniprot2(String tremblFastaFile, int n) {


		Date a = Calendar.getInstance().getTime();
		System.out.println("\n The start time for camps2uniprot: " + a);

		try {			
			File file = new File("/scratch/usman/mappingCamps/MappedTrembl"+n);
			//File file = new File("F:/MappedTrembl");
			BufferedWriter br = new BufferedWriter(new FileWriter(file));

			// pstm1 was initialised here

			/*
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO camps2uniprot " +
					"(sequenceid," +
					"entry_name," +
					"accession, " +
					"description, " +					
					"subset," +
					"linkouturl) " +
					"VALUES " +
					"(?,?,?,?,?,?)");

			 */
			//ConfigFile cf = new ConfigFile("/home/users/saeed/workspace/CAMPS3/config/config.xml");
			//String uniprotLinkout = cf.getProperty("linkout:uniprot");
			String uniprotLinkout = "http://www.uniprot.org/uniprot/";

			a = Calendar.getInstance().getTime();
			//System.out.println("\n Got the link out to uniprot: " + a);

			//
			//read TrEMBL file
			//
			System.out.println("\nReading TrEMBL FASTA file...");

			int counter = 0;

			//FASTAReader fr2 = new FASTAReader(tremblFastaFile);
			//System.out.println("\nCompleted Reading TrEMBL FASTA file...");

			LinkedHashMap<String, ProteinSequence> aa = FastaReaderHelper.readFastaProteinSequence(new File(tremblFastaFile));
			//FastaReaderHelper.readFastaDNASequence for DNA sequences
			PreparedStatement pstm_getAll = CAMPS_CONNECTION.prepareStatement(
					"SELECT md5,sequenceid FROM sequences2 ");

			ResultSet rs_getAll = pstm_getAll.executeQuery();
			while(rs_getAll.next()) {

				int id = rs_getAll.getInt("sequenceid");
				String md = rs_getAll.getString("md5");
				Protein temp = new Protein(id);
				camps4.put(md, temp);
			}
			rs_getAll.close();
			pstm_getAll.close();
			// putting all camps4 md5s done

			for (  Entry<String, ProteinSequence> entry : aa.entrySet() ) {
				//System.out.println( entry.getValue().getOriginalHeader() + "=" + entry.getValue().getSequenceAsString() );
				//}



				//while(fr2.hasNextEntry()) {

				counter ++;
				if (counter % 10000 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (counter % 1000000 == 0) {					
					System.out.write('\n');
					System.out.flush();
				}

				//	SingleEntry entry = fr2.getNextEntry();
				//	String name = entry.getName().trim();	
				//String description = entry.getDescription();
				String sequence = entry.getValue().getSequenceAsString();


				//String accession = name.split("\\|")[1];
				//String entryName = name.split("\\|")[2];


				//System.out.println("\nComputing md5...");
				String md5 = computeMD5Hash(sequence);
				//System.out.println("\nmd5 Computed..." + counter);

				//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
				//		"SELECT sequenceid FROM sequences2 WHERE md5=?");

				//changing code here to use hashmaps and make it fast



				int sequenceID = -1;

				Protein temp = camps4.get(md5);
				if (temp!=null){

					sequenceID = temp.seq_id;

					String description = entry.getValue().getDescription();
					AccessionID accession = entry.getValue().getAccession();
					String Header =entry.getValue().getOriginalHeader();
					int x = Header.indexOf(" ");
					Header = Header.substring(0,x);
					Header = Header.trim();
					x = Header.indexOf("|");
					Header = Header.substring(x+1);
					x = Header.indexOf("|");
					String entryName = Header.substring(x+1);

					String url = uniprotLinkout+accession;
					// getting the error here, "prepared statement needs to be re prepared"
					// therefore, I would now try to write all this to a file and then later
					// just read the file and insert in the table

					//seqID /t entryName /t acc /t description /t let_subset_empty /t url 

					String linein = sequenceID+"\t"+entryName+"\t"+accession+"\t"+description+"\t"+url;
					br.write(linein);
					br.newLine();
					System.out.println("\n Last processed " + accession);
					System.out.flush();
				}
				//pstm1.setString(1, md5);
				//ResultSet rs1 = pstm1.executeQuery();
				//while(rs1.next()) {
				//	sequenceID = rs1.getInt("sequenceid");
				//}
				//rs1.close();
				//pstm1.close();
				/*
				if(sequenceID != -1) {

					String url = uniprotLinkout+accession;

					// getting the error here, "prepared statement needs to be re prepared"
					// therefore, I would now try to write all this to a file and then later
					// just read the file and insert in the table

					//seqID /t entryName /t acc /t description /t let_subset_empty /t url 

					String linein = sequenceID+"\t"+entryName+"\t"+accession+"\t"+description+"\t"+url;
					br.write(linein);
					br.newLine();

				}
				 */
				//System.out.println("\n Last processed " + accession);
				//System.out.flush();
			}

			// pstm1 was closed here
			br.close();
			//pstm2.close();
			a = Calendar.getInstance().getTime();
			System.out.println("\n Insert to camps2uniprot complete from Trembl fasta file: " + a);
			/*
			System.out.println("\nCreate indizes...\n");
			DBAdaptor.createIndex("camps4","camps2uniprot",new String[]{"entry_name"},"entry_name");
			DBAdaptor.createIndex("camps4","camps2uniprot",new String[]{"accession"},"accession");
			DBAdaptor.createIndex("camps4","camps2uniprot",new String[]{"sequenceid"},"sequenceid");

			System.out.println("Add taxonomy information...");
			addTaxonomyid(sprotDatFile,tremblDatFile);

			a = Calendar.getInstance().getTime();
			System.out.println("\n Completion of addTaxonomyid: " + a);			
			 */
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
			if (SIMAP_CONNECTION != null) {
				try {
					SIMAP_CONNECTION.close();
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
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


	public static void runAddTaxonomuid(String sprotDatFile, String tremblDatFile){
		System.out.println("\nCreate indizes...\n");
		//DBAdaptor.createIndex("camps4","camps2uniprot",new String[]{"entry_name"},"entry_name");
		//DBAdaptor.createIndex("camps4","camps2uniprot",new String[]{"accession"},"accession");
		//DBAdaptor.createIndex("camps4","camps2uniprot",new String[]{"sequenceid"},"sequenceid");

		System.out.println("Add taxonomy information...\n");
		addTaxonomyid(sprotDatFile,tremblDatFile);

			
	}
	private static void addTaxonomyid(String sprotDatFile, String tremblDatFile) {

		try {
			Date a = Calendar.getInstance().getTime();
			System.out.println("\n In the add Taxonomyid: " + a);

			Pattern p = Pattern.compile("OX\\s+NCBI_TaxID=(\\d+);");

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"UPDATE camps2uniprot SET taxonomyid=? WHERE entry_name=?");

			PreparedStatement pstm2 = SIMAP_CONNECTION.prepareStatement(
					"SELECT n1.node_id,n1.parent_node,n1.rank,n2.name " +
							"FROM ncbi_tax_node n1,ncbi_tax_name n2 " +
					"WHERE n1.node_id=? AND n2.class=\"scientific name\" AND n1.node_id=n2.node_id");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO camps2uniprot_taxonomies " +
							"(taxonomyid, taxonomy, species, genus, family, order_, class, phylum, kingdom, superkingdom) VALUES " +
					"(?,?,?,?,?,?,?,?,?,?)");

			BitSet alreadyProcessed = new BitSet();	// *********** ATTENTION NOT PROCESSED **********
			
			// get all info of camps proteins usniprot ids. so to read the .dat file selectively
			PreparedStatement pstm_Campsids = CAMPS_CONNECTION.prepareStatement(
					"SELECT distinct entry_name FROM camps2uniprot");
			ResultSet campsids = pstm_Campsids.executeQuery();
			String ac =""; 
			while(campsids.next()){
				ac = campsids.getString("entry_name");
				Protein temp = new Protein();
				camps4.put(ac, temp);
			}

			//
			//parse SwissProt dat file	
			//
			System.out.println("\nReading SwissProt dat file...");
			FileInputStream fin1 = new FileInputStream(sprotDatFile);
			GZIPInputStream gzis1 = new GZIPInputStream(fin1);
			InputStreamReader xover1 = new InputStreamReader(gzis1);
			BufferedReader br1 = new BufferedReader(xover1);    

			String line;
			String entryName = "";
			int taxonomyid = -1;
			while ((line = br1.readLine()) != null) {

				Matcher m = p.matcher(line);

				if (line.startsWith("ID")) {
					entryName = line.split("\\s+")[1];									
				}

				else if (m.matches()) {
					taxonomyid = Integer.parseInt(m.group(1));
				}
				else if (line.startsWith("//")) {

					//pstm1.setInt(1, taxonomyid);
					//pstm1.setString(2, entryName);
					//int updatedRows = pstm1.executeUpdate();
					if (camps4.containsKey(entryName)){
						//Protein temp = new Protein(taxonomyid);
						pstm1.setInt(1, taxonomyid);
						pstm1.setString(2, entryName);
						int updatedRows = pstm1.executeUpdate();
						if(updatedRows >0 && !alreadyProcessed.get(taxonomyid)) {

							fillTableCamps2uniprot_taxonomies(pstm2, pstm3, taxonomyid);
							alreadyProcessed.set(taxonomyid);
						}
					}

					

					//reset
					entryName = "";
					taxonomyid = -1;				
				}
			}
			br1.close();			
			gzis1.close();
			a = Calendar.getInstance().getTime();
			System.out.println("\n Reading SwissProt dat file complete: " + a);
			//
			//parse TrEMBL dat file	
			//
			System.out.println("\nReading TrEMBL dat file...");
			FileInputStream fin2 = new FileInputStream(tremblDatFile);
			GZIPInputStream gzis2 = new GZIPInputStream(fin2);
			InputStreamReader xover2 = new InputStreamReader(gzis2);
			BufferedReader br2 = new BufferedReader(xover2);    

			int counter = 0;

			String line2;
			entryName = "";
			taxonomyid = -1;
			while ((line2 = br2.readLine()) != null) {

				Matcher m = p.matcher(line2);

				if (line2.startsWith("ID")) {

					counter ++;
					if (counter % 10000 == 0) {
						System.out.write('.');
						System.out.flush();
					}
					if (counter % 1000000 == 0) {					
						System.out.write('\n');
						System.out.flush();
					}

					entryName = line2.split("\\s+")[1];									
				}

				else if (m.matches()) {
					taxonomyid = Integer.parseInt(m.group(1));
				}
				else if (line2.startsWith("//")) {

					if (camps4.containsKey(entryName)){
						//Protein temp = new Protein(taxonomyid);
						pstm1.setInt(1, taxonomyid);
						pstm1.setString(2, entryName);
						int updatedRows = pstm1.executeUpdate();
						if(updatedRows >0 && !alreadyProcessed.get(taxonomyid)) {

							fillTableCamps2uniprot_taxonomies(pstm2, pstm3, taxonomyid);
							alreadyProcessed.set(taxonomyid);
						}
					}

					

					//reset
					entryName = "";
					taxonomyid = -1;				
				}
			}
			br2.close();			
			gzis2.close();
			a = Calendar.getInstance().getTime();
			System.out.println("\n Reading Trembl dat file complete: " + a);

			pstm1.close();
			pstm2.close();
			pstm3.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	private static void fillTableCamps2uniprot_taxonomies(PreparedStatement pstm1, PreparedStatement pstm2, int taxonomyid) {
		try {

			String taxonomy = null;
			String species = null;
			String genus = null;
			String family = null;
			String order = null;
			String class_ = null;
			String phylum = null;
			String kingdom = null;
			String superkingdom = null;

			int nodeID = taxonomyid;
			int parentNodeID = 0;
			while(parentNodeID != 1) {
				pstm1.setInt(1, nodeID);

				int countEntries = 0;
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					countEntries++;
					nodeID = rs1.getInt("node_id");
					parentNodeID = rs1.getInt("parent_node");
					String rank = rs1.getString("rank");
					String name = rs1.getString("name");
					if(nodeID == taxonomyid) {
						taxonomy = name;
					}
					if(rank.equals("species") && species == null) {
						species = name;
					}
					else if(rank.equals("genus") && genus == null) {
						genus = name;
					}
					else if(rank.equals("family") && family == null) {
						family = name;
					}
					else if(rank.equals("order") && order == null) {
						order = name;
					}
					else if(rank.equals("class") && class_ == null) {
						class_ = name;
					}
					else if(rank.equals("phylum") && phylum == null) {
						phylum = name;
					}
					else if(rank.equals("kingdom") && kingdom == null) {
						kingdom = name;
					}
					else if(rank.equals("superkingdom") && superkingdom == null) {
						superkingdom = name;
					}
				}
				rs1.close();

				if(countEntries == 0) {
					System.out.println("\tWARNING: Could not found taxonomyid " +nodeID+ " in SIMAP!");
					break;
				}

				nodeID = parentNodeID;
			}						

			pstm2.setInt(1,taxonomyid);	
			pstm2.setString(2, taxonomy);
			pstm2.setString(3, species);
			pstm2.setString(4, genus);
			pstm2.setString(5, family);
			pstm2.setString(6, order);
			pstm2.setString(7, class_);
			pstm2.setString(8, phylum);
			pstm2.setString(9, kingdom);
			pstm2.setString(10, superkingdom);

			pstm2.executeUpdate();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}

}
