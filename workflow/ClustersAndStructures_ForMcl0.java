package workflow;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import utils.DBAdaptor;

public class ClustersAndStructures_ForMcl0 {

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	/*
	private static final float QUERY_COVERAGE_THRESHOLD = 90;
	private static final float HIT_COVERAGE_THRESHOLD = 90;
	private static final float IDENTITY_THRESHOLD = 90;
	 */
	// recalculating with 30 threshold
	// however, have not yet recomputed everything based on the 30 identity threshold - as asked while reading mansuscript
	// basic calculations were done but not detailed....
	private static final float QUERY_COVERAGE_THRESHOLD = 30;
	private static final float HIT_COVERAGE_THRESHOLD = 30;
	private static final float IDENTITY_THRESHOLD = 30;
	private static final String BASE_Address = "/home/users/saeed/pdbtest/pdb_mcl/";
	//private static final String BASE_URL = "http://www.pdb.org/pdb/files/";

	private static final Pattern RESOLUTION_PATTERN = Pattern.compile("REMARK\\s+\\d+\\s+RESOLUTION\\.\\s+(\\d+\\.\\d+)\\s+ANGSTROMS\\.");


	public static void run() {
		//go();
		//System.exit(0);

		try {
			String sequences ="";
			ArrayList <String> done  = new ArrayList<String>();

			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
			//		"SELECT sequences_other_database_sequenceid," +
			//		"query_coverage,hit_coverage,bitscore,evalue,ident " +
			//		"FROM other_database " +
			//		"WHERE sequenceid=? AND db=\"pdbtm\"");
			PreparedStatement ptm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid, sequences_other_database_sequenceid," +
							"query_coverage,hit_coverage,bitscore,evalue,ident " +
							"FROM other_database " +
					"WHERE db=\"pdbtm\"");
			ResultSet rst1 = ptm1.executeQuery();
			HashMap <Integer, ArrayList <OtherDatabase_Strucutre>> ptbl1 = new HashMap<Integer, ArrayList<OtherDatabase_Strucutre>>();
			//key -> sequenceid and value is the rest
			while(rst1.next()){
				int sequenceid = rst1.getInt("sequenceid");
				int sequenceidpdb = rst1.getInt("sequences_other_database_sequenceid");
				float queryCoverage = rst1.getFloat("query_coverage");
				float hitCoverage = rst1.getFloat("hit_coverage");
				float bitscore = rst1.getFloat("bitscore");
				double evalue = rst1.getDouble("evalue");
				float identity = rst1.getFloat("ident");

				if (ptbl1.containsKey(sequenceid)){
					ArrayList <OtherDatabase_Strucutre> temp = ptbl1.get(sequenceid);
					OtherDatabase_Strucutre entry = new OtherDatabase_Strucutre();
					entry.set(sequenceidpdb, queryCoverage, hitCoverage, bitscore, evalue, identity);
					temp.add(entry);
					ptbl1.put(sequenceid, temp);
				}
				else{
					ArrayList <OtherDatabase_Strucutre> temp = new ArrayList<OtherDatabase_Strucutre>();
					OtherDatabase_Strucutre entry = new OtherDatabase_Strucutre();
					entry.set(sequenceidpdb, queryCoverage, hitCoverage, bitscore, evalue, identity);
					temp.add(entry);
					ptbl1.put(sequenceid, temp);
				}
			}
			rst1.close();
			ptm1.close();
			System.out.print("Populated Other Db\n");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT name " +
							"FROM sequences_other_database " +
					"WHERE sequenceid=? AND db=\"pdbtm\"");	// NOTE: sequenceid is NOT CAMPS seqID
			
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_mcl_structures2 " +
							"(cluster_id, cluster_threshold, pdbid, method, resolution, query_coverage, hit_coverage, bitscore, evalue, ident) " +
							"VALUES " +
					"(?,?,?,?,?,?,?,?,?,?)");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_mcl_structures2 " +
							"(cluster_id, cluster_threshold, pdbid, method, query_coverage, hit_coverage, bitscore, evalue, ident) " +
							"VALUES " +
					"(?,?,?,?,?,?,?,?,?)");
			
			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id,cluster_threshold FROM clusters_mcl_nr_info2");
			int count =0;
			while(rs.next()) {
				count++;

				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				System.out.print("Processing: "+clusterID+"_"+clusterThreshold+" ("+count+")\n");

				BitSet members = getMembers(clusterID, clusterThreshold);

				ArrayList<String> alreadyFoundStructures = new ArrayList<String>();

				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					if (ptbl1.containsKey(sequenceid)){
						ArrayList<OtherDatabase_Strucutre> returns= ptbl1.get(sequenceid);
						// sort the above array highest to lowest based on identity
						// so the best hits are reported first
						if(returns.size()>1){
							System.out.println("Original");
							print(returns);
							returns = sort(returns);
							System.out.println("Sorted");
							print(returns);
						}
						for(int x =0;x<=returns.size()-1;x++){
							OtherDatabase_Strucutre yo = returns.get(x);

							int sequenceidpdb = yo.sequences_other_database_sequenceid;
							float queryCoverage = yo.query_coverage;
							float hitCoverage = yo.hit_coverage;
							float bitscore = yo.bitscore;
							double evalue = yo.evalue;
							float identity = yo.ident;
//							if(queryCoverage >= QUERY_COVERAGE_THRESHOLD && hitCoverage >= HIT_COVERAGE_THRESHOLD && identity >= IDENTITY_THRESHOLD) {

								String name = "";
								pstm2.setInt(1, sequenceidpdb);
								ResultSet rs2 = pstm2.executeQuery();
								while(rs2.next()) {
									name = rs2.getString("name");
								}
								rs2.close();

								String pdbid = name.split("_")[0];
								if(!done.contains(pdbid)){
									sequences = sequences+","+pdbid.trim();
									done.add(pdbid);

								}

								if(alreadyFoundStructures.contains(name)) {
									continue;
								}
								//get method and resolution from pdb website
								//URL url = new URL (BASE_URL+pdbid+".pdb");
								//URLConnection urlConn = url.openConnection();
								//DataInputStream input = new DataInputStream (urlConn.getInputStream ());
								//InputStreamReader isr = new InputStreamReader(input);

								String method = "";
								float resolution = -1;

								String file_address = BASE_Address+pdbid+".pdb";

								File f = new File(file_address);
								if(f.exists() && !f.isDirectory()) { 

									BufferedReader br = new BufferedReader(new FileReader(f));
									String line;
									while((line = br.readLine()) != null) {
										line = line.trim();
										if(line.startsWith("EXPDTA")) {						
											method = line.replace("EXPDTA", "").trim();
											if(method.startsWith("NMR")) {
												method = method.split(",")[0].trim();
											}	
											if(!(method.equals("ELECTRON DIFFRACTION") || method.equals("X-RAY DIFFRACTION"))) {
												break;
											}
										}
										else if(line.startsWith("REMARK")) {
											Matcher m = RESOLUTION_PATTERN.matcher(line);
											if(m.matches()) {
												resolution = Float.parseFloat(m.group(1));
												break;
											}						
										}
									}
									//input.close();
									br.close();

									//System.out.println(id+"\t"+method+"\t"+resolution);


									if(resolution != -1) {
										
										pstm3.setInt(1, clusterID);
										pstm3.setFloat(2, clusterThreshold);
										pstm3.setString(3, name);
										pstm3.setString(4, method);
										pstm3.setFloat(5, resolution);
										pstm3.setFloat(6, queryCoverage);
										pstm3.setFloat(7, hitCoverage);
										pstm3.setFloat(8, bitscore);
										pstm3.setDouble(9, evalue);
										pstm3.setFloat(10, identity);

										pstm3.executeUpdate();
									}
									else {
										
										pstm4.setInt(1, clusterID);
										pstm4.setFloat(2, clusterThreshold);
										pstm4.setString(3, name);
										pstm4.setString(4, method);
										pstm4.setFloat(5, queryCoverage);
										pstm4.setFloat(6, hitCoverage);
										pstm4.setFloat(7, bitscore);
										pstm4.setDouble(8, evalue);
										pstm4.setFloat(9, identity);

										pstm4.executeUpdate();
										 
									}


									alreadyFoundStructures.add(name);
								}

						}
					}
				}
			}


			rs.close();
			stm.close();

			pstm2.close();
			pstm3.close();
			pstm4.close();

			//System.out.print("\n\n"+sequences+"\n\n");


		}catch(Exception e) {
			e.printStackTrace();
			System.exit(0);
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


	public static void print(ArrayList<OtherDatabase_Strucutre> returns) {
		// TODO Auto-generated method stub
		for(int i=0;i<=returns.size()-1;i++){
			System.out.println(returns.get(i).ident + " ---- "+i );
		}
	}


	public static ArrayList<OtherDatabase_Strucutre> sort(
			ArrayList<OtherDatabase_Strucutre> returns) {
		// TODO Auto-generated method stub
		ArrayList<OtherDatabase_Strucutre> sortedArray = new ArrayList<OtherDatabase_Strucutre>();
		try{
			// check conditions for input in table and reduce size
			for(int x =0;x<=returns.size()-1;x++){
				OtherDatabase_Strucutre yo = returns.get(x);
				float queryCoverage = yo.query_coverage;
				float hitCoverage = yo.hit_coverage;
				float identity = yo.ident;
				// condition below checks the min conditions to be in table
				// reducing size now as sorting of smaller array would be faster
				if(queryCoverage >= ClustersAndStructures_ForMcl0.QUERY_COVERAGE_THRESHOLD && hitCoverage >= ClustersAndStructures_ForMcl0.HIT_COVERAGE_THRESHOLD && identity >= ClustersAndStructures_ForMcl0.IDENTITY_THRESHOLD) {
					sortedArray.add(yo);
				}
			}
			// NOW sort
			Collections.sort(sortedArray, new CustomComparator());

		}
		catch(Exception e){
			e.printStackTrace();
		}
		return sortedArray;
	}



	private static void go() {
		// TODO Auto-generated method stub
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/Scratch/seqs.txt")));
			ArrayList<String> x = new ArrayList<String>();
			String s = "";
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT name " +
							"FROM sequences_other_database " +
					"WHERE db=\"pdbtm\"");
			ResultSet rs = pstm2.executeQuery();
			while(rs.next()){
				String id = rs.getString(1);
				String[] temp = id.split("_");
				String i = temp[0];
				if(!x.contains(i)){
					x.add(i);
					bw.write(i+",");
				}
			}
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}


	private static BitSet getMembers(int clusterID, float clusterThreshold) {

		BitSet members = null;
		try {
			members = new BitSet();
			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery(
					"SELECT sequenceid " +
							"FROM clusters_mcl2 " +
							"WHERE cluster_id="+clusterID+" AND cluster_threshold="+clusterThreshold);

			while(rs.next()) {
				int sequenceID = rs.getInt("sequenceid");
				members.set(sequenceID);
			}
			rs.close();
			stm.close();

		}catch(Exception e) {
			e.printStackTrace();
		}

		return members;
	}
}
