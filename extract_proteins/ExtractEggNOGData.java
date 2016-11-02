package extract_proteins;

/*
 * ExtractEggNOGData
 * 
 * Version 2.0
 * 
 * 2015-09-09
 * 
 */


import general.CreateDatabaseTables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import utils.DBAdaptor;


public class ExtractEggNOGData {

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	private static HashMap <String, Integer> SeqMap = new HashMap<String, Integer>(); // key md5 and value campsSeqId


	public static void createMapping(String eggnogProteinsFile, String outfile) {
		try {
			PrintWriter pw = new PrintWriter(new FileWriter(new File(outfile)));

			//PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM sequences WHERE md5=?");
			BufferedReader br = new BufferedReader(new FileReader(eggnogProteinsFile));
			String line;
			String header = "";
			StringBuffer sequence = new StringBuffer();
			int i =0;
			int j = 0;
			while ((line=br.readLine()) != null) {
				line = line.trim();
				if (!line.equals("")) {
					if (line.startsWith(">")) {
						//write previous entry
						if (!header.equals("") && sequence.length()>0) {
							//entries.add(new FastaEntry(header.substring(1),sequence.toString()));
							//tax_id.protein_id
							String thisHeader = header.substring(1);
							String thisSequence = sequence.toString();
							i++;
							if (i%100==0){
								System.out.print(".");
							}
							if (i%1000==0){
								System.out.print("\n");
							}
							String md5 = computeMD5Hash(thisSequence);
							int campsID = -1;
							if (SeqMap.containsKey(md5)){
								campsID = SeqMap.get(md5);
							}
							if(campsID != -1) {
								pw.println(thisHeader+"\t"+campsID);
								j++;
							}

							header = "";
							sequence = new StringBuffer();
						}
						header = line;
					}
					else {
						sequence.append(line);
					}
				}
			}

			br.close();

			System.out.println("Number of entries in FASTA file: " +i);
			System.out.println("Number of entries in Mapped file: " +j);

			pw.close();

		} catch(Exception e) {
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


	public static void getData4CAMPS(ArrayList<String> allmembers, String mappingFile, HashMap <String, String> mapDescription) {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO camps2eggnog " +
							"(sequenceid, eggnog_protein, start, end, group_, description) " +
							"VALUES " +
					"(?,?,?,?,?,?)");


			Hashtable<String,Integer> mapping = new Hashtable<String,Integer>();

			BufferedReader br1 = new BufferedReader(new FileReader(new File(mappingFile)));
			String line1;
			while((line1 = br1.readLine()) != null) {
				String[] content = line1.split("\t");

				String eggnogProtein = content[0];
				int campsProtein = Integer.parseInt(content[1]);

				if(mapping.containsKey(eggnogProtein)) {
					System.out.println("WARNING: Multiple mappings for "+eggnogProtein);
				}
				else {
					mapping.put(eggnogProtein, Integer.valueOf(campsProtein));
				}

			}
			br1.close();

			for(int x=0;x<=allmembers.size()-1;x++){
				String eggNOGFile = allmembers.get(x);

				BufferedReader br2 = new BufferedReader(new FileReader(new File(eggNOGFile)));
				String line2;
				while((line2 = br2.readLine()) != null) {
					if(line2.startsWith("#")) {
						continue;
					}
					// nog - protein - start - end
					String[] content = line2.split("\t");

					String group = content[0].trim();

					String eggnogProtein = content[1].trim();
					int start = Integer.parseInt(content[2]);
					int end = Integer.parseInt(content[3]);
					String groupDescr = "NA";
					if (mapDescription.containsKey(group)){
						groupDescr = mapDescription.get(group);
					}
					if(mapping.containsKey(eggnogProtein)) {

						int campsID = mapping.get(eggnogProtein).intValue();

						pstm.setInt(1, campsID);
						pstm.setString(2, eggnogProtein);
						pstm.setInt(3, start);
						pstm.setInt(4, end);
						pstm.setString(5, group);
						pstm.setString(6, groupDescr);

						pstm.executeUpdate();
						System.out.print(campsID+"\t"+eggnogProtein+"\t"+start+"\t"+end+"\t"+group+"\t"+groupDescr+"\n");
					}
				}
				br2.close();
			}	
			pstm.close();


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


	public static void main(String[] args) {

		System.out.print("EggNog Data\n");
		populateCamps4SequenceHash();
		System.out.print("Map of CAMPS Done. Size "+SeqMap.size()+"\n");
		//file 1 ->  Sequence File of EggNog			file 2 -> outfile
		createMapping("F:/orthoProjRattei/orthologProj/eggNOG/sequences/eggnogv4.proteins.all.fa","F:/orthoProjRattei/EggNogForCAMPS/eggNOG2camps_0915.txt");
		//createMapping("F:/orthoProjRattei/orthologProj/eggNOG/sequences/temp.fa","F:/orthoProjRattei/EggNogForCAMPS/eggNOG2camps_0915.txt");

		CreateDatabaseTables.create_table_camps2eggnog();
		//parameter 1 - members .........Parameter 2 - Description .................. Parameter 3 - Mapping of camps and EggNog
		makeSingleFile("F:/orthoProjRattei/EggNogForCAMPS/all.members/","F:/orthoProjRattei/EggNogForCAMPS/all.description/","F:/orthoProjRattei/EggNogForCAMPS/eggNOG2camps_0915.txt");



	}


	private static void makeSingleFile(String string, String string2,String camps2egg) {
		// TODO Auto-generated method stub
		try{
			File description = new File(string2);
			ArrayList<String> descrpts = listFilesForFolder(description);
			HashMap <String, String> DescMap = makeFileDescription(descrpts,"F:/orthoProjRattei/EggNogForCAMPS/all_desccriptions.txt");

			File members = new File(string);
			ArrayList<String> mems = listFilesForFolder(members);
			getData4CAMPS(mems,camps2egg,DescMap);

		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

	private static HashMap <String, String> makeFileDescription(ArrayList<String> files, String outfile){
		try{
			HashMap <String, String> map = new HashMap <String, String>();
			PrintWriter pwriter = new PrintWriter(new FileWriter(new File(outfile)));
			for(int i =0;i<=files.size()-1;i++){
				String f = files.get(i);
				BufferedReader br = new BufferedReader(new FileReader(f));
				String line ="";
				while((line = br.readLine())!=null){
					if(line.startsWith("#")) {
						continue;
					}
					String [] parts = line.split("\t");
					if(parts.length>1){
					String OG = parts[0].trim();
					String descrip = parts[1];
					if (!map.containsKey(OG) && descrip!=null){
						map.put(OG, descrip);
					}}
					pwriter.println(line);
				}
				br.close();
			}
			pwriter.close();
			return map;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	private static ArrayList<String> listFilesForFolder(final File folder) {
		int count = 0; 
		ArrayList<String> files_results = new ArrayList<String>();
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				listFilesForFolder(fileEntry);
			} else {
				count++;
				files_results.add(fileEntry.getAbsolutePath());
			}
		}
		System.out.println("\n total file " + count + " \n");
		// all file names received
		return files_results;
	}
	private static HashMap<String, Integer> populateCamps4SequenceHash() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("SELECT md5,sequenceid FROM sequences2");
			ResultSet rs = pstm.executeQuery();
			String m ="";
			int id = 0;
			while(rs.next()){
				m = rs.getString(1);
				id = rs.getInt(2);
				if (!SeqMap.containsKey(m)){
					SeqMap.put(m, id);
				}
			}
			rs.close();
			pstm.close();
		}

		catch(Exception e){
			e.printStackTrace();
		}
		return SeqMap;
	}

}
