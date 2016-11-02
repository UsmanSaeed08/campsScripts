package reRUN_CAMPS;

import general.CreateDatabaseTables;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import utils.DBAdaptor;
import workflow.CpClusters;
import workflow.Protein;

public class CpClusters2 {

	/**
	 * @param args
	 */
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");


	public static void run(String metaModelDirectory) {
		try {

			addSCclusters(metaModelDirectory);
			addSCcode();

			//			addFHCode();
			//			addFHDescription();
			//System.out.print("\nFixing self parents of md cluster\n");
			//fixMDSelParents();
			//fixMDSelParents2();

			//System.out.print("\nAdding MD_code\n");
			//addMDCode();
			//addMDCodeForNulls();
			//System.out.print("\nMD_codes added\n");
			//System.out.print("\nAdding MD description\n");
			//addMDDescription();


			//
			//update cluster descriptions
			//
			/*
			System.out.print("\nAdding fh_clusters_architecture\n");
			CreateDatabaseTables.create_table_fh_clusters_architecture();
			fillTableFHClustersArchitecture();

			//addSCDescription2();			
			//addFHDescription2();
			//addMDDescription2();


			System.out.print("\nAdding strucutres Info\n");
			addStructuresInfo();
			 */
			//addStructuresInfo2();
			//addSCDescription2();
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

	private static void fixMDSelParents() {
		// TODO Auto-generated method stub
		try{
			// todo
			// fix the parent cluster ids of md clusters which have null code -
			// having null code means that they are assigned parents of themselves, whereas they should have an sc cluster as parent
			// to have
			// child of every sc cluster up to 100
			// strategy
			// if null code... find the cluster id in childs of sc cluster..assign respective sc clcuster

			// 										1 get those md with null name

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold," +
					"code,super_cluster_id,super_cluster_threshold from cp_clusters where type=\"md_cluster\"");
			ResultSet rs = pstm1.executeQuery();
			ArrayList<String> redoSeq = new ArrayList<String>(); 

			while(rs.next()){
				Integer clusid = rs.getInt(1);
				Float thresh = rs.getFloat(2);
				String code = rs.getString(3);
				//int superClusid = rs.getInt(4);
				//float superThresh = rs.getFloat(5);
				// if code is null put above in hash
				if(code == null){
					redoSeq.add(clusid.toString().trim()+"_"+thresh.toString().trim());
				}
			}
			rs.close();
			pstm1.close();

			//								Get SC clusters and their child clusters  
			// 1 sc cluster may be divided into many childs which could have further divided 
			// into more childs..and an md cluster could exist at any level

			// get all the children for all sc clusters
			ArrayList<String> sc = new ArrayList<String>();
			HashMap<String,String> sc_map = new HashMap<String,String>();
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters where type=\"sc_cluster\"");
			/*			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("select child_cluster_number ,child_cluster_id,child_cluster_threshold from clusters_mcl_track" +
					"where cluster_id=? AND cluster_threshold=? "); // returns all child clusters with child cluster number which is index of number of childs 
			 */			
			ResultSet rs2 = pstm2.executeQuery();
			while(rs2.next()){
				Integer clusid = rs2.getInt(1);
				Float thresh = rs2.getFloat(2);
				String key = clusid.toString().trim()+"_"+thresh.toString().trim();
				sc.add(key);
				sc_map.put(key, null);
			}
			rs2.close();
			pstm2.close();
			// go through all the sc and get ist of childs

			int mapped = 0;
			ArrayList<String> umapped = new ArrayList<String>();
			for(int i =0;i<=redoSeq.size()-1;i++){
				Integer md_clusid = Integer.parseInt(redoSeq.get(i).split("_")[0].trim());
				Float md_thresh = Float.parseFloat(redoSeq.get(i).split("_")[1].trim());
				//System.out.println(clusid+"\t"+thresh);

				if(sc_map.containsKey(redoSeq.get(i))){
					//FOUND
					mapped++;
					System.out.println(redoSeq.get(i)+"\tis SC parent for MD cluster\t"+redoSeq.get(i));
				}
				else{
					boolean notfound = true;
					while(notfound){
						if(md_thresh == 5f){
							notfound = false;
							umapped.add(redoSeq.get(i));
						}
						PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold" +
								" from clusters_mcl_track " +
								"where child_cluster_id=? AND child_cluster_threshold=? ");
						pstm3.setInt(1, md_clusid);
						pstm3.setFloat(2, md_thresh);
						//System.out.println("waiting for query return");
						ResultSet rs_track = pstm3.executeQuery();
						//System.out.println("processing results");
						while(rs_track.next()){
							Integer clus = rs_track.getInt(1);
							Float th = rs_track.getFloat(2);
							String key = clus.toString().trim()+"_"+th.toString().trim();
							if(sc_map.containsKey(key)){
								//FOUND
								notfound = false;
								mapped++;
								System.out.println(key+"\tis SC parent for MD cluster\t"+redoSeq.get(i));
								break;
							}
							else{
								md_clusid = clus;
								md_thresh = th;
							}
						}
						rs_track.close();
						pstm3.close();

					}
				}
			}

			System.out.println("Redo Size = " +redoSeq.size() );
			System.out.println("Mapped Size = " +mapped );
			for(int i = 0; i<= umapped.size()-1;i++){
				System.out.println(umapped.get(i));
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void fixMDSelParents2() {
		// TODO Auto-generated method stub
		try{
			// 										1 get those md with null name
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold," +
					"code,super_cluster_id,super_cluster_threshold from cp_clusters where type=\"md_cluster\"");
			ResultSet rs = pstm1.executeQuery();

			ArrayList<String> redoSeq = new ArrayList<String>(); 
			HashMap<String,ArrayList<Integer>> redoSeqMap = new HashMap<String,ArrayList<Integer>>();

			while(rs.next()){
				Integer clusid = rs.getInt(1);
				Float thresh = rs.getFloat(2);
				String code = rs.getString(3);
				//int superClusid = rs.getInt(4);
				//float superThresh = rs.getFloat(5);
				// if code is null put above in hash
				if(code == null){
					String key = clusid.toString().trim()+"_"+thresh.toString().trim();
					redoSeq.add(key);

					PreparedStatement pstmR = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl " +
							"where cluster_id=? AND cluster_threshold=? ");
					pstmR.setInt(1, clusid);
					pstmR.setFloat(2, thresh);
					//System.out.println("waiting for query return");
					ResultSet rs_R = pstmR.executeQuery();
					ArrayList<Integer> temp = new ArrayList<Integer>();
					while(rs_R.next()){
						temp.add(rs_R.getInt(1));
					}
					rs_R.close();
					pstmR.close();
					redoSeqMap.put(key, temp);
				}
			}
			rs.close();
			pstm1.close();

			//								Get SC clusters and their child clusters  
			// 1 sc cluster may be divided into many childs which could have further divided 
			// into more childs..and an md cluster could exist at any level

			// get all the children for all sc clusters
			ArrayList<String> sc = new ArrayList<String>();
			HashMap<String,HashMap<Integer,Integer>> sc_map = new HashMap<String,HashMap<Integer,Integer>>();
			HashMap<Integer,String> SeqToSc = new HashMap<Integer,String>();
			// key is clusid_thresh and vlaue is seqids
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters where type=\"sc_cluster\"");

			ResultSet rs2 = pstm2.executeQuery();
			while(rs2.next()){
				Integer clusid = rs2.getInt(1);
				Float thresh = rs2.getFloat(2);
				String key = clusid.toString().trim()+"_"+thresh.toString().trim();
				sc.add(key);
				// for this sc cluster.. get all sequences
				PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl " +
						"where cluster_id=? AND cluster_threshold=? ");
				pstm3.setInt(1, clusid);
				pstm3.setFloat(2, thresh);
				//System.out.println("waiting for query return");
				ResultSet rs_tofind = pstm3.executeQuery();
				HashMap<Integer,Integer> temp = new HashMap<Integer,Integer>();
				while(rs_tofind.next()){
					temp.put(rs_tofind.getInt(1),null);
					if(!SeqToSc.containsKey(rs_tofind.getInt(1))){
						SeqToSc.put(rs_tofind.getInt(1), key);
					}
				}
				rs_tofind.close();
				pstm3.close();
				sc_map.put(key, temp);
			}
			rs2.close();
			pstm2.close();
			// go through all the sc and get ist of childs


			// Now have all the members of both md and sc clusters.... so just map

			PreparedStatement pstmUpdate = CAMPS_CONNECTION.prepareStatement("UPDATE cp_clusters SET super_cluster_id=?,super_cluster_threshold=? " +
					"where cluster_id=? and cluster_threshold=? and type=\"md_cluster\"");

			int mapped = 0;
			for(int i =0;i<=redoSeq.size()-1;i++){
				String key_md = redoSeq.get(i);
				//ArrayList<Integer> md_members = redoSeqMap.get(key_md);
				String scClusKey = getSCClusForMDmembers(redoSeqMap.get(key_md),sc,sc_map);

				if(!scClusKey.contains("NotFound")){
					mapped ++;

					Integer superid = Integer.parseInt(scClusKey.trim().split("_")[0]);
					Float superthresh = Float.parseFloat(scClusKey.trim().split("_")[1]);


					Integer id = Integer.parseInt(key_md.trim().split("_")[0]);
					Float thresh = Float.parseFloat(key_md.trim().split("_")[1]);;

					//set
					pstmUpdate.setInt(1, superid);
					pstmUpdate.setFloat(2, superthresh);
					//where
					pstmUpdate.setInt(3, id);
					pstmUpdate.setFloat(4, thresh);

					pstmUpdate.execute();
					System.out.println(superid+"\t"+superthresh+"\t"+id+"\t"+thresh+"\t"+redoSeqMap.get(key_md).size() + "\t" +sc_map.get(scClusKey).size() );
				}
				else{
					//System.out.println("Md Cluster Not Mapped "+ key_md + "\t" + redoSeqMap.get(key_md).size());

					/*for(int x = 0;x<=redoSeqMap.get(key_md).size()-1;x++){
						int seqid = redoSeqMap.get(key_md).get(x);
						if(SeqToSc.containsKey(seqid)){
							System.out.println("Md Cluster Not Mapped "+ key_md + "\t" +SeqToSc.get(seqid)+"\t"+redoSeqMap.get(key_md).size());
						}
					}*/

					Integer id = Integer.parseInt(key_md.trim().split("_")[0]);
					Float thresh = Float.parseFloat(key_md.trim().split("_")[1]);;

					Integer superid = 786;
					Float superthresh = 105f;
					pstmUpdate.setInt(1, superid);
					pstmUpdate.setFloat(2, superthresh);
					pstmUpdate.setInt(3, id);
					pstmUpdate.setFloat(4, thresh);

					pstmUpdate.execute();
					//System.out.println(superid+"\t"+superthresh+"\t"+id+"\t"+thresh+"\t"+redoSeqMap.get(key_md).size() + "\t" +sc_map.get(scClusKey).size() );
				}
			}
			System.out.println("Redo Size = " +redoSeq.size() );
			System.out.println("Mapped Size = " +mapped );


		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static String getSCClusForMDmembers(ArrayList<Integer> mdClusmembers,
			ArrayList<String> sc,
			HashMap<String, HashMap<Integer, Integer>> sc_map) {
		boolean found = false;
		String s = "";
		int x = 0;
		// TODO Auto-generated method stub
		for(int i=0;i<=sc.size()-1;i++){
			s = sc.get(i);
			HashMap<Integer, Integer> smembers = sc_map.get(s);

			x=0;
			for(;x <= mdClusmembers.size()-1;){
				Integer mem = mdClusmembers.get(x);
				if(smembers.containsKey(mem)){
					// has the seqid
					x++;
					found = true;
				}
				else{
					break;
				}
			}
			if(found == true && x == mdClusmembers.size()){
				// the md clus belongs to this sc cluster
				//x == mdClusmembers.size()-1 makes sure that all the md cluster members were found in this sc cluster
				return s;
			}
		}
		if(found == true && x == mdClusmembers.size()-1){
			// the md clus belongs to this sc cluster
			//x == mdClusmembers.size()-1 makes sure that all the md cluster members were found in this sc cluster
			return s;
		}
		else{
			return "NotFound";
		}
	}

	private static void addSCclusters(String metaModelDirectory) {
		try {

			Pattern p1 = Pattern.compile("cluster_(\\d+\\.\\d+)_(\\d+)\\.hmm");
			Pattern p2 = Pattern.compile("(PF\\d+)\\s*-\\s*.*");


			HashMap <String, ArrayList<Protein>> ptbl1 = new HashMap<String, ArrayList<Protein>>(); // key is the clusid+clusthresh and value is sequences_other_database_sequenceid
			// how to order by instances_rel desc
			PreparedStatement ptm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT link_description,instances_rel,cluster_id,cluster_threshold " +
							"FROM clusters_cross_db2 " +
							"WHERE db=\"pfam\" " +
					"ORDER BY instances_rel desc"); 
			// since the descending order is specified in query directly. It would ensure that all the array list are automatically 
			// populated like wise.
			ResultSet rs1 = ptm1.executeQuery();
			while(rs1.next()) {
				String linkDes = rs1.getString("link_description"); 
				double instRel = rs1.getDouble("instances_rel");
				int clusid = rs1.getInt("cluster_id");
				float thresh = rs1.getFloat("cluster_threshold");
				String key = Integer.toString(clusid)+Float.toString(thresh);
				if (ptbl1.containsKey(key)){
					// get array List and add element
					ArrayList<Protein> p = ptbl1.get(key);
					Protein temp = new Protein();
					temp.set(linkDes, instRel);
					p.add(temp);
					ptbl1.put(key, p);
				}
				else{
					// simply add the element
					ArrayList<Protein> p =  new ArrayList<Protein>();
					Protein temp = new Protein();
					temp.set(linkDes, instRel);
					p.add(temp);
					ptbl1.put(key, p);
				}				
			}
			rs1.close();
			HashMap <String, String> ptbl2 = new HashMap<String, String>(); // key is the accession and value is Description
			PreparedStatement ptm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT accession,description FROM pfam");			
			ResultSet rs2 = ptm2.executeQuery();
			String d = "";
			String acc = "";
			while(rs2.next()) {
				d = rs2.getString("description");
				acc = rs2.getString("accession");
				if(!ptbl2.containsKey("accession")){
					ptbl2.put(acc, d);
				}
			}
			rs2.close();

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO cp_clusters2 " +
							"(cluster_id,cluster_threshold,description,type) " +
							"VALUES " +
					"(?,?,?,?)");

			File[] files = new File(metaModelDirectory).listFiles();
			for(File file: files) {

				String fname = file.getName();
				Matcher m1 = p1.matcher(fname);
				if(m1.matches()) {

					float clusterThreshold = Float.parseFloat(m1.group(1));
					int clusterID = Integer.parseInt(m1.group(2));


					String accession = "";
					String description = "NA";

					String k = Integer.toString(clusterID)+Float.toString(clusterThreshold);
					if(ptbl1.containsKey(k)){
						ArrayList<Protein> p = ptbl1.get(k);
						for(int i =0;i<=p.size()-1;i++){
							String descr = p.get(i).link_description;
							double instRel = p.get(i).instances_rel;

							if(instRel >= 50) {

								if(descr.split("#").length > 1) {	//multiple PFAM assignments available
									description = "NA";								
								}
								else {
									Matcher m2 = p2.matcher(descr);
									if(m2.matches()) {
										accession = m2.group(1).trim();

										if (ptbl2.containsKey(accession)){
											description = ptbl2.get(accession);
										}
									}
									else{
										System.err.println("Not matching: " +descr);
									}						

								}							
							}
						}
					}
					rs1.close();

					pstm3.setInt(1, clusterID);
					pstm3.setFloat(2, clusterThreshold);
					pstm3.setString(3, description);
					pstm3.setString(4, "sc_cluster");

					pstm3.executeUpdate();

				}
				else {
					System.err.println("Not matching: " +fname);
				}
				System.out.print("Processed "+fname+"\n");
			}

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	private static void addSCcode() {

		try {
			System.out.print("\n Adding Codes \n");
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("UPDATE cp_clusters2 SET code=? WHERE cluster_id=? AND cluster_threshold=? AND type=\"sc_cluster\"");

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery(
					"SELECT cp_clusters2.cluster_id,cp_clusters2.cluster_threshold,clusters_mcl_nr_info2.sequences " +
							"FROM cp_clusters2,clusters_mcl_nr_info2 " +
							"WHERE cp_clusters2.type=\"sc_cluster\" AND " +
							"cp_clusters2.cluster_id=clusters_mcl_nr_info2.cluster_id AND " +
							"cp_clusters2.cluster_threshold=clusters_mcl_nr_info2.cluster_threshold " +
					"ORDER BY sequences DESC");


			String prefix = "CMSC";

			int count = 0;
			while(rs.next()) {
				count++;

				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				String suffix = String.valueOf(count);
				while(suffix.length()<4) {
					suffix = "0"+suffix;
				}

				String code = prefix+suffix;

				pstm.setString(1, code);
				pstm.setInt(2, clusterID);
				pstm.setFloat(3, clusterThreshold);

				pstm.executeUpdate();

				System.out.print("\n Processed"+count+" \n");

			}
			rs.close();
			stm.close();	
			pstm.close();


		}catch(Exception e) {
			e.printStackTrace();
		}		

	}



	private static void addFHCode() {
		try {

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT c1.cluster_id,c1.cluster_threshold,sequences " +
							"FROM cp_clusters c1, clusters_mcl_info c2 " +
							"WHERE c1.cluster_id=c2.cluster_id AND " +
							"c1.cluster_threshold=c2.cluster_threshold AND " +
							"super_cluster_id=? AND super_cluster_threshold=? AND type=\"fh_cluster\" " +
					"ORDER BY sequences desc");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"UPDATE cp_clusters SET code=? " +
							"WHERE cluster_id=? AND cluster_threshold=? " +
					"AND type=\"fh_cluster\"");

			String prefix = "FH";

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id,cluster_threshold,code FROM cp_clusters WHERE type=\"sc_cluster\"");

			while(rs.next()) {

				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");
				String code = rs.getString("code");

				//get fh-clusters
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);

				int count = 0;

				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {

					count++;

					int subClusterID = rs1.getInt("cluster_id");
					float subClusterThreshold = rs1.getFloat("cluster_threshold");

					String suffix = String.valueOf(count);
					while(suffix.length()<3) {
						suffix = "0"+suffix;
					}

					String subCode = code+"_"+prefix+suffix;

					pstm2.setString(1, subCode);
					pstm2.setInt(2, subClusterID);
					pstm2.setFloat(3, subClusterThreshold);

					pstm2.executeUpdate();
				}
				rs1.close();
			}
			rs.close();

			pstm1.close();
			pstm2.close();


		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	private static void addMDCodeForNulls() {
		try {
			// get the nulls
			PreparedStatement pstmNulls = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold," +
					"super_cluster_threshold,code from cp_clusters where type=\"md_cluster\"");
			ResultSet rsNulls = pstmNulls.executeQuery();
			HashMap<String,Integer> redoSeqMap = new HashMap<String,Integer>();
			ArrayList<String> redoSeqUnMapped = new ArrayList<String>();
			HashMap<String,Integer> existingCodes = new HashMap<String,Integer>();
			while(rsNulls.next()){
				Integer clusid = rsNulls.getInt(1);
				Float thresh = rsNulls.getFloat(2);
				Float Superthresh = rsNulls.getFloat(3);
				String cod = rsNulls.getString(4);
				if(cod == null){
					String key = clusid.toString().trim()+"_"+thresh.toString().trim();
					if(Superthresh == 105f){
						redoSeqUnMapped.add(key);
					}
					else{
						redoSeqMap.put(key, null);
					}
				}
				else{
					existingCodes.put(cod,null);
				}
			}
			rsNulls.close();
			pstmNulls.close();

			// got the nulls moove ahead


			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT c1.cluster_id,c1.cluster_threshold,sequences " +
							"FROM cp_clusters c1, clusters_mcl_nr_info c2 " +
							"WHERE c1.cluster_id=c2.cluster_id AND " +
							"c1.cluster_threshold=c2.cluster_threshold AND " +
							"super_cluster_id=? AND super_cluster_threshold=? AND type=\"md_cluster\" " +
					"ORDER BY sequences desc");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"UPDATE cp_clusters SET code=? " +
							"WHERE cluster_id=? AND cluster_threshold=? " +
					"AND type=\"md_cluster\"");

			String prefix = "MD";

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id,cluster_threshold,code FROM cp_clusters WHERE type=\"sc_cluster\"");

			//int cNumber = 0;

			while(rs.next()) {

				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");
				String code = rs.getString("code");

				//get md-clusters
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				int count = 0;
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					count++;
					Integer subClusterID = rs1.getInt("cluster_id");
					Float subClusterThreshold = rs1.getFloat("cluster_threshold");

					String suffix = String.valueOf(count);
					while(suffix.length()<3) {
						suffix = "0"+suffix;
					}
					String subCode = code+"_"+prefix+suffix;
					String k = subClusterID.toString().trim()+"_"+subClusterThreshold.toString().trim();

					if(redoSeqMap.containsKey(k)){
						if(existingCodes.containsKey(subCode)){
							System.err.println("************** \nDUPLICATE CODE \n************* ");
						}
						pstm2.setString(1, subCode);
						pstm2.setInt(2, subClusterID);
						pstm2.setFloat(3, subClusterThreshold);
						pstm2.executeUpdate();
						System.out.println(subCode + "\t" + subClusterID +"\t"+ subClusterThreshold);
					}
				}
				rs1.close();
			}
			rs.close();
			pstm1.close();

			// Set Up codes for remaining non SC parent clusters
			for(int x = 0;x<= redoSeqUnMapped.size()-1;x++){
				Integer subClusterID = Integer.parseInt(redoSeqUnMapped.get(x).trim().split("_")[0]);
				Float subClusterThreshold = Float.parseFloat(redoSeqUnMapped.get(x).trim().split("_")[1]);

				String suffix = String.valueOf(x);
				String code = "CMSC0000";
				String subCode = code+"_"+prefix+"-SP-"+suffix;
				if(existingCodes.containsKey(subCode)){
					System.err.println("************** \nDUPLICATE CODE \n************* ");
				}
				pstm2.setString(1, subCode);
				pstm2.setInt(2, subClusterID);
				pstm2.setFloat(3, subClusterThreshold);
				pstm2.executeUpdate();
				System.out.println(subCode + "\t" + subClusterID +"\t"+ subClusterThreshold);
			}
			pstm2.close();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	private static void addMDCode() {
		try {

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT c1.cluster_id,c1.cluster_threshold,sequences " +
							"FROM cp_clusters c1, clusters_mcl_nr_info c2 " +
							"WHERE c1.cluster_id=c2.cluster_id AND " +
							"c1.cluster_threshold=c2.cluster_threshold AND " +
							"super_cluster_id=? AND super_cluster_threshold=? AND type=\"md_cluster\" " +
					"ORDER BY sequences desc");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"UPDATE cp_clusters SET code=? " +
							"WHERE cluster_id=? AND cluster_threshold=? " +
					"AND type=\"md_cluster\"");

			String prefix = "MD";

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id,cluster_threshold,code FROM cp_clusters WHERE type=\"sc_cluster\"");

			//int cNumber = 0;

			while(rs.next()) {

				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");
				String code = rs.getString("code");

				//get md-clusters
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);

				int count = 0;

				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {

					count++;

					int subClusterID = rs1.getInt("cluster_id");
					float subClusterThreshold = rs1.getFloat("cluster_threshold");

					String suffix = String.valueOf(count);
					while(suffix.length()<3) {
						suffix = "0"+suffix;
					}

					String subCode = code+"_"+prefix+suffix;

					pstm2.setString(1, subCode);
					pstm2.setInt(2, subClusterID);
					pstm2.setFloat(3, subClusterThreshold);

					pstm2.executeUpdate();
					System.out.println(subCode + "\t" + subClusterID +"\t"+ subClusterThreshold);
				}
				rs1.close();
			}
			rs.close();

			pstm1.close();
			pstm2.close();


		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	private static void addFHDescription() {
		try {

			Pattern p2 = Pattern.compile("(PF\\d+)\\s*-\\s*.*");



			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT link_description,instances_rel " +
							"FROM clusters_cross_db " +
							"WHERE cluster_id=? AND cluster_threshold=? AND db=\"pfam\" " +
					"ORDER BY instances_rel desc");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT description FROM pfam WHERE accession=?");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"UPDATE cp_clusters SET description=? " +
							"WHERE cluster_id=? AND cluster_threshold=? " +
					"AND type=\"fh_cluster\"");

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id,cluster_threshold FROM cp_clusters WHERE type=\"fh_cluster\"");
			while(rs.next()) {

				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				String accession = "";
				String description = "NA";

				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {

					String descr = rs1.getString("link_description");
					double instRel = rs1.getDouble("instances_rel");


					if(instRel >= 50) {

						if(descr.split("#").length > 1) {	//multiple PFAM assignments available
							description = "NA";								
						}
						else {
							Matcher m2 = p2.matcher(descr);
							if(m2.matches()) {
								accession = m2.group(1).trim();

								pstm2.setString(1, accession);
								ResultSet rs2 = pstm2.executeQuery();
								while(rs2.next()) {
									description = rs2.getString("description");
								}
								rs2.close();
							}
							else{
								System.err.println("Not matching: " +descr);
							}						

						}							
					}
				}
				rs1.close();

				pstm3.setString(1, description);
				pstm3.setInt(2, clusterID);
				pstm3.setFloat(3, clusterThreshold);

				pstm3.executeUpdate();

			}

		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	private static void addMDDescription() {
		try {

			Pattern p2 = Pattern.compile("(PF\\d+)\\s*-\\s*.*");



			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT link_description,instances_rel " +
							"FROM clusters_cross_db " +
							"WHERE cluster_id=? AND cluster_threshold=? AND db=\"pfam\" " +
					"ORDER BY instances_rel desc");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT description FROM pfam WHERE accession=?");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"UPDATE cp_clusters SET description=? " +
							"WHERE cluster_id=? AND cluster_threshold=? " +
					"AND type=\"md_cluster\"");

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id,cluster_threshold FROM cp_clusters WHERE type=\"md_cluster\"");
			while(rs.next()) {

				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				String accession = "";
				String description = "NA";

				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {

					String descr = rs1.getString("link_description");
					double instRel = rs1.getDouble("instances_rel");


					if(instRel >= 50) {

						if(descr.split("#").length > 1) {	//multiple PFAM assignments available
							description = "NA";								
						}
						else {
							Matcher m2 = p2.matcher(descr);
							if(m2.matches()) {
								accession = m2.group(1).trim();

								pstm2.setString(1, accession);
								ResultSet rs2 = pstm2.executeQuery();
								while(rs2.next()) {
									description = rs2.getString("description");
								}
								rs2.close();
							}
							else{
								System.err.println("Not matching: " +descr);
							}						

						}							
					}
				}
				rs1.close();

				pstm3.setString(1, description);
				pstm3.setInt(2, clusterID);
				pstm3.setFloat(3, clusterThreshold);

				pstm3.executeUpdate();

			}

		}catch(Exception e) {
			e.printStackTrace();
		}
	}
private static void addMissingDescriptions(){
	try{
		
	}
	catch(Exception e){
		e.printStackTrace();
	}
}

	/**
	 * The function below was used first to add descriptions with threshold 50
	 * Then remaining entries are given a description with no threshold
	 * Further more, only 2 enteries had hypothetical protein description
	 * m pc = manually found description based on pfam clan
	 * CMSC0152 -- 781 - 6 -- Integral membrane proteins belonging to CPA and AT superfamily. [m pc]
	 * CMSC0697 -- 1331 - 5 -- Integral membrane proteins, strongly hydrophobic with strongly basic motif near the C-terminus. [m pf]
	 *  
	 */
	private static void addSCDescription2() {
		try {

			//double threshold = 50;
			double threshold = 0;


			Hashtable<String, ArrayList<String>> description2codes = new Hashtable<String, ArrayList<String>>(); 

			Pattern p1 = Pattern.compile("(PF\\d+)\\s*-\\s*.*");
			Pattern p2 = Pattern.compile("(SSF\\d+)\\s*-\\s*(.*)");


			int countNums = 0;

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT link_description,instances_rel " +
							"FROM clusters_cross_db " +
							"WHERE cluster_id=? AND cluster_threshold=? AND db=\"pfam\" " +
					"ORDER BY instances_rel desc");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT description,clan_description FROM pfam WHERE accession=?");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"SELECT link_description,instances_rel " +
							"FROM clusters_cross_db " +
							"WHERE cluster_id=? AND cluster_threshold=? AND db=\"superfamily\" " +
					"ORDER BY instances_rel desc");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement(
					"UPDATE cp_clusters SET description=? " +
							"WHERE code=? " +
					"AND type=\"sc_cluster\"");

			Statement stm = CAMPS_CONNECTION.createStatement();
			//ResultSet rs = stm.executeQuery("SELECT cluster_id,cluster_threshold,code FROM cp_clusters WHERE type=\"sc_cluster\" ORDER BY code");
			
			ResultSet rs = stm.executeQuery("SELECT cluster_id,cluster_threshold,code FROM cp_clusters WHERE type=\"sc_cluster\" AND" +
					" description like \"%Uncharacterized SC-cluster%\" ORDER BY code");
			
			while(rs.next()) {

				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");
				String clusterCode = rs.getString("code");


				//
				//get pfam family and clan hits
				//
				String bestPfamAccession = "";
				String bestPfamDescription = "";
				double bestPfamAccession_instRel = 0;
				Hashtable<String,Double> clan2instRel = new Hashtable<String,Double>();

				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {

					String descr = rs1.getString("link_description");
					double instRel = rs1.getDouble("instances_rel");


					if(descr.split("#").length > 1) {	//multiple PFAM assignments available
						continue;							
					}
					else {
						Matcher m1 = p1.matcher(descr);
						if(m1.matches()) {

							String pfamAccession = m1.group(1).trim();

							String pfamDescription = null;
							String pfamClanDescription = null;

							pstm2.setString(1, pfamAccession);
							ResultSet rs2 = pstm2.executeQuery();
							while(rs2.next()) {
								pfamDescription = rs2.getString("description");
								pfamClanDescription = rs2.getString("clan_description");
							}
							rs2.close();

							if(instRel > bestPfamAccession_instRel) {
								bestPfamAccession = pfamAccession;
								bestPfamDescription = pfamDescription;
								bestPfamAccession_instRel = instRel;
							}

							if(pfamClanDescription != null) {

								double relCount = 0;
								if(clan2instRel.containsKey(pfamClanDescription)) {
									relCount = clan2instRel.get(pfamClanDescription).doubleValue();
								}

								relCount += instRel;

								clan2instRel.put(pfamClanDescription,Double.valueOf(relCount));
							}
						}
						else{
							System.err.println("Not matching: " +descr);
						}						

					}							

				}
				rs1.close();


				//
				//get superfamily hits
				//
				String bestSuperfamilyAccession = "";
				String bestSuperfamilyDescription = "";
				double bestSuperfamilyAccession_instRel = 0;

				pstm3.setInt(1, clusterID);
				pstm3.setFloat(2, clusterThreshold);
				ResultSet rs3 = pstm3.executeQuery();
				while(rs3.next()) {

					String descr = rs3.getString("link_description");
					double instRel = rs3.getDouble("instances_rel");


					if(descr.split("#").length > 1) {	//multiple PFAM assignments available
						continue;							
					}
					else {
						Matcher m2 = p2.matcher(descr);
						if(m2.matches()) {

							String superfamilyAccession = m2.group(1).trim();
							String superfamilyDescription = m2.group(2).trim();

							String pfamDescription = null;
							String pfamClanDescription = null;

							if(instRel > bestSuperfamilyAccession_instRel) {
								bestSuperfamilyAccession = superfamilyAccession;
								bestSuperfamilyDescription = superfamilyDescription;
								bestSuperfamilyAccession_instRel = instRel;
							}														
						}
						else{
							System.err.println("Not matching: " +descr);
						}						
					}							

				}
				rs1.close();


				//
				//choose best reference
				//
				//priority list:
				// 1. Pfam families
				// 2. Pfam clans
				// 3. Superfamily families
				//
				String bestClanDescription = "";
				double bestClanDescription_instRel = 0;

				Enumeration<String> clans = clan2instRel.keys();
				while(clans.hasMoreElements()) {

					String clan = clans.nextElement();
					double relCount = clan2instRel.get(clan).doubleValue();

					if(relCount > bestClanDescription_instRel) {

						bestClanDescription = clan;
						bestClanDescription_instRel = relCount;
					}
				}


				String clusterDescription = "Uncharacterized SC-cluster "+clusterCode;

				if(bestPfamAccession_instRel >= threshold) {

					if(bestClanDescription_instRel >= threshold) {

						if(bestPfamAccession_instRel >= bestClanDescription_instRel) {
							clusterDescription = bestPfamDescription+" [pf]";
						}
						else {
							clusterDescription = bestClanDescription+" [pc]";
						}
					}
					else {
						clusterDescription = bestPfamDescription+" [pf]";
					}
				}
				else{

					if(bestClanDescription_instRel >= threshold) {
						clusterDescription = bestClanDescription+" [pc]";
					}
					else {

						if(bestSuperfamilyAccession_instRel >= threshold) {
							clusterDescription = bestSuperfamilyDescription+" [sf]";
						}
					}
				}


				ArrayList<String> codes = new ArrayList<String>();
				if(description2codes.containsKey(clusterDescription)) {
					codes = description2codes.get(clusterDescription);
				}

				codes.add(clusterCode);

				description2codes.put(clusterDescription, codes);

			}

			int countClustersWithoutDescription = 0;

			Enumeration<String> descriptions = description2codes.keys();
			while(descriptions.hasMoreElements()) {

				String description = descriptions.nextElement();

				if(description.startsWith("Uncharacterized SC-cluster")) {
					countClustersWithoutDescription++;
				}

				ArrayList<String> codes = description2codes.get(description);

				if(codes.size() == 1) {

					String code = codes.get(0);

					pstm4.setString(1, description);
					pstm4.setString(2, code);

					pstm4.executeUpdate();

					countNums++;
					System.out.println(code+"\t"+description+"\t"+countNums);
				}
				else {	//multiple clusters are associated with same description
					int count = 0;

					for(String code: codes) {

						count++;

						String newDescription = description + " #"+count;

						pstm4.setString(1, newDescription);
						pstm4.setString(2, code);

						pstm4.executeUpdate();
						countNums++;

						System.out.println(code+"\t"+newDescription+"\t"+countNums);
					}
				}
			}

			System.out.println("Number of clusters without description: " +countClustersWithoutDescription);


			stm.close();

			pstm1.close();
			pstm2.close();
			pstm3.close();
			pstm4.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	private static void fillTableFHClustersArchitecture() {

		try{ 

			Statement stm = CAMPS_CONNECTION.createStatement();

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters_architecture " +
							"(code,architecture) " +
							"VALUES " +
					"(?,?)");

			ResultSet rs = stm.executeQuery("SELECT code,description FROM cp_clusters WHERE type=\"fh_cluster\"");
			while(rs.next()) {

				String code = rs.getString("code");
				String description = rs.getString("description");

				pstm.setString(1, code);
				pstm.setString(2, description);

				pstm.executeUpdate();
			}
			rs.close();

			stm.close();

			pstm.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	private static void addFHDescription2() {
		try {

			double threshold = 50;


			Pattern p1 = Pattern.compile("(PF\\d+)\\s*-\\s*.*");
			Pattern p2 = Pattern.compile("(SSF\\d+)\\s*-\\s*(.*)");


			PreparedStatement pstm0 = CAMPS_CONNECTION.prepareStatement(
					"SELECT code FROM cp_clusters " +
							"WHERE super_cluster_id=? AND super_cluster_threshold=? " +
					"AND type=\"fh_cluster\" ORDER BY code");


			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT link_description,instances_rel " +
							"FROM fh_clusters_cross_db " +
							"WHERE code=? AND db=\"pfam\" " +
					"ORDER BY instances_rel desc");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT description,clan_description FROM pfam WHERE accession=?");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"SELECT link_description,instances_rel " +
							"FROM fh_clusters_cross_db " +
							"WHERE code=? AND db=\"superfamily\" " +
					"ORDER BY instances_rel desc");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement(
					"UPDATE cp_clusters SET description=? " +
							"WHERE code=? " +
					"AND type=\"fh_cluster\"");

			Statement stm = CAMPS_CONNECTION.createStatement();

			int countClustersWithoutDescription = 0;

			ResultSet rs = stm.executeQuery("SELECT cluster_id,cluster_threshold FROM cp_clusters WHERE type=\"sc_cluster\" ORDER BY code");
			while(rs.next()) {

				int scClusterID = rs.getInt("cluster_id");
				float scClusterThreshold = rs.getFloat("cluster_threshold");

				Hashtable<String, ArrayList<String>> description2codes = new Hashtable<String, ArrayList<String>>(); 

				pstm0.setInt(1, scClusterID);
				pstm0.setFloat(2, scClusterThreshold);
				ResultSet rs0 = pstm0.executeQuery();
				while(rs0.next()){

					String fhClusterCode = rs0.getString("code");


					//
					//get pfam family and clan hits
					//
					String bestPfamAccession = "";
					String bestPfamDescription = "";
					double bestPfamAccession_instRel = 0;
					Hashtable<String,Double> clan2instRel = new Hashtable<String,Double>();

					pstm1.setString(1, fhClusterCode);					
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {

						String descr = rs1.getString("link_description");
						double instRel = rs1.getDouble("instances_rel");


						if(descr.split("#").length > 1) {	//multiple PFAM assignments available
							continue;							
						}
						else {
							Matcher m1 = p1.matcher(descr);
							if(m1.matches()) {

								String pfamAccession = m1.group(1).trim();

								String pfamDescription = null;
								String pfamClanDescription = null;

								pstm2.setString(1, pfamAccession);
								ResultSet rs2 = pstm2.executeQuery();
								while(rs2.next()) {
									pfamDescription = rs2.getString("description");
									pfamClanDescription = rs2.getString("clan_description");
								}
								rs2.close();

								if(instRel > bestPfamAccession_instRel) {
									bestPfamAccession = pfamAccession;
									bestPfamDescription = pfamDescription;
									bestPfamAccession_instRel = instRel;
								}

								if(pfamClanDescription != null) {

									double relCount = 0;
									if(clan2instRel.containsKey(pfamClanDescription)) {
										relCount = clan2instRel.get(pfamClanDescription).doubleValue();
									}

									relCount += instRel;

									clan2instRel.put(pfamClanDescription,Double.valueOf(relCount));
								}
							}
							else{
								System.err.println("Not matching: " +descr);
							}						

						}							

					}
					rs1.close();


					//
					//get superfamily hits
					//
					String bestSuperfamilyAccession = "";
					String bestSuperfamilyDescription = "";
					double bestSuperfamilyAccession_instRel = 0;

					pstm3.setString(1, fhClusterCode);
					ResultSet rs3 = pstm3.executeQuery();
					while(rs3.next()) {

						String descr = rs3.getString("link_description");
						double instRel = rs3.getDouble("instances_rel");


						if(descr.split("#").length > 1) {	//multiple PFAM assignments available
							continue;							
						}
						else {
							Matcher m2 = p2.matcher(descr);
							if(m2.matches()) {

								String superfamilyAccession = m2.group(1).trim();
								String superfamilyDescription = m2.group(2).trim();

								String pfamDescription = null;
								String pfamClanDescription = null;

								if(instRel > bestSuperfamilyAccession_instRel) {
									bestSuperfamilyAccession = superfamilyAccession;
									bestSuperfamilyDescription = superfamilyDescription;
									bestSuperfamilyAccession_instRel = instRel;
								}														
							}
							else{
								System.err.println("Not matching: " +descr);
							}						
						}							

					}
					rs1.close();


					//
					//choose best reference
					//
					//priority list:
					// 1. Pfam families
					// 2. Pfam clans
					// 3. Superfamily families
					//
					String bestClanDescription = "";
					double bestClanDescription_instRel = 0;

					Enumeration<String> clans = clan2instRel.keys();
					while(clans.hasMoreElements()) {

						String clan = clans.nextElement();
						double relCount = clan2instRel.get(clan).doubleValue();

						if(relCount > bestClanDescription_instRel) {

							bestClanDescription = clan;
							bestClanDescription_instRel = relCount;
						}
					}


					String clusterDescription = "Uncharacterized FH-cluster "+fhClusterCode;

					if(bestPfamAccession_instRel >= threshold) {

						if(bestClanDescription_instRel >= threshold) {

							if(bestPfamAccession_instRel >= bestClanDescription_instRel) {
								clusterDescription = bestPfamDescription+" [pf]";
							}
							else {
								clusterDescription = bestClanDescription+" [pc]";
							}
						}
						else {
							clusterDescription = bestPfamDescription+" [pf]";
						}
					}
					else{

						if(bestClanDescription_instRel >= threshold) {
							clusterDescription = bestClanDescription+" [pc]";
						}
						else {

							if(bestSuperfamilyAccession_instRel >= threshold) {
								clusterDescription = bestSuperfamilyDescription+" [sf]";
							}
						}
					}


					ArrayList<String> codes = new ArrayList<String>();
					if(description2codes.containsKey(clusterDescription)) {
						codes = description2codes.get(clusterDescription);
					}

					codes.add(fhClusterCode);

					description2codes.put(clusterDescription, codes);

				}



				Enumeration<String> descriptions = description2codes.keys();
				while(descriptions.hasMoreElements()) {

					String description = descriptions.nextElement();

					if(description.startsWith("Uncharacterized FH-cluster")) {
						countClustersWithoutDescription++;
					}

					ArrayList<String> codes = description2codes.get(description);

					if(codes.size() == 1) {

						String code = codes.get(0);

						pstm4.setString(1, description);
						pstm4.setString(2, code);

						pstm4.executeUpdate();

						//System.out.println(code+"\t"+description);
					}
					else {	//multiple clusters are associated with same description
						int count = 0;

						for(String code: codes) {

							count++;

							String newDescription = description + " #"+count;

							pstm4.setString(1, newDescription);
							pstm4.setString(2, code);

							pstm4.executeUpdate();

							//System.out.println(code+"\t"+newDescription);
						}
					}
				}
				rs0.close();			

			}

			System.out.println("Number of clusters without description: " +countClustersWithoutDescription);


			stm.close();

			pstm0.close();
			pstm1.close();
			pstm2.close();
			pstm3.close();
			pstm4.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	private static void addMDDescription2() {
		try {

			double threshold = 50;


			Pattern p1 = Pattern.compile("(PF\\d+)\\s*-\\s*.*");
			Pattern p2 = Pattern.compile("(SSF\\d+)\\s*-\\s*(.*)");


			PreparedStatement pstm0 = CAMPS_CONNECTION.prepareStatement(
					"SELECT cluster_id,cluster_threshold,code FROM cp_clusters " +
							"WHERE super_cluster_id=? AND super_cluster_threshold=? " +
					"AND type=\"md_cluster\" ORDER BY code");


			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT link_description,instances_rel " +
							"FROM clusters_cross_db " +
							"WHERE cluster_id=? AND cluster_threshold=? AND db=\"pfam\" " +
					"ORDER BY instances_rel desc");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT description,clan_description FROM pfam WHERE accession=?");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"SELECT link_description,instances_rel " +
							"FROM clusters_cross_db " +
							"WHERE cluster_id=? AND cluster_threshold=? AND db=\"superfamily\" " +
					"ORDER BY instances_rel desc");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement(
					"UPDATE cp_clusters SET description=? " +
							"WHERE code=? " +
					"AND type=\"md_cluster\"");

			Statement stm = CAMPS_CONNECTION.createStatement();

			int countClustersWithoutDescription = 0;

			ResultSet rs = stm.executeQuery("SELECT cluster_id,cluster_threshold FROM cp_clusters WHERE type=\"sc_cluster\" ORDER BY code");
			while(rs.next()) {

				int scClusterID = rs.getInt("cluster_id");
				float scClusterThreshold = rs.getFloat("cluster_threshold");

				Hashtable<String, ArrayList<String>> description2codes = new Hashtable<String, ArrayList<String>>(); 

				pstm0.setInt(1, scClusterID);
				pstm0.setFloat(2, scClusterThreshold);
				ResultSet rs0 = pstm0.executeQuery();
				while(rs0.next()){

					int mdClusterID = rs0.getInt("cluster_id");
					float mdClusterThreshold = rs0.getFloat("cluster_threshold");	
					String mdClusterCode = rs0.getString("code");

					//
					//get pfam family and clan hits
					//
					String bestPfamAccession = "";
					String bestPfamDescription = "";
					double bestPfamAccession_instRel = 0;
					Hashtable<String,Double> clan2instRel = new Hashtable<String,Double>();

					pstm1.setInt(1, mdClusterID);		
					pstm1.setFloat(2, mdClusterThreshold);		
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {

						String descr = rs1.getString("link_description");
						double instRel = rs1.getDouble("instances_rel");


						if(descr.split("#").length > 1) {	//multiple PFAM assignments available
							continue;							
						}
						else {
							Matcher m1 = p1.matcher(descr);
							if(m1.matches()) {

								String pfamAccession = m1.group(1).trim();

								String pfamDescription = null;
								String pfamClanDescription = null;

								pstm2.setString(1, pfamAccession);
								ResultSet rs2 = pstm2.executeQuery();
								while(rs2.next()) {
									pfamDescription = rs2.getString("description");
									pfamClanDescription = rs2.getString("clan_description");
								}
								rs2.close();

								if(instRel > bestPfamAccession_instRel) {
									bestPfamAccession = pfamAccession;
									bestPfamDescription = pfamDescription;
									bestPfamAccession_instRel = instRel;
								}

								if(pfamClanDescription != null) {

									double relCount = 0;
									if(clan2instRel.containsKey(pfamClanDescription)) {
										relCount = clan2instRel.get(pfamClanDescription).doubleValue();
									}

									relCount += instRel;

									clan2instRel.put(pfamClanDescription,Double.valueOf(relCount));
								}
							}
							else{
								System.err.println("Not matching: " +descr);
							}						

						}							

					}
					rs1.close();


					//
					//get superfamily hits
					//
					String bestSuperfamilyAccession = "";
					String bestSuperfamilyDescription = "";
					double bestSuperfamilyAccession_instRel = 0;

					pstm3.setInt(1, mdClusterID);		
					pstm3.setFloat(2, mdClusterThreshold);	
					ResultSet rs3 = pstm3.executeQuery();
					while(rs3.next()) {

						String descr = rs3.getString("link_description");
						double instRel = rs3.getDouble("instances_rel");


						if(descr.split("#").length > 1) {	//multiple PFAM assignments available
							continue;							
						}
						else {
							Matcher m2 = p2.matcher(descr);
							if(m2.matches()) {

								String superfamilyAccession = m2.group(1).trim();
								String superfamilyDescription = m2.group(2).trim();

								String pfamDescription = null;
								String pfamClanDescription = null;

								if(instRel > bestSuperfamilyAccession_instRel) {
									bestSuperfamilyAccession = superfamilyAccession;
									bestSuperfamilyDescription = superfamilyDescription;
									bestSuperfamilyAccession_instRel = instRel;
								}														
							}
							else{
								System.err.println("Not matching: " +descr);
							}						
						}							

					}
					rs1.close();


					//
					//choose best reference
					//
					//priority list:
					// 1. Pfam families
					// 2. Pfam clans
					// 3. Superfamily families
					//
					String bestClanDescription = "";
					double bestClanDescription_instRel = 0;

					Enumeration<String> clans = clan2instRel.keys();
					while(clans.hasMoreElements()) {

						String clan = clans.nextElement();
						double relCount = clan2instRel.get(clan).doubleValue();

						if(relCount > bestClanDescription_instRel) {

							bestClanDescription = clan;
							bestClanDescription_instRel = relCount;
						}
					}


					String clusterDescription = "Uncharacterized MD-cluster "+mdClusterCode;

					if(bestPfamAccession_instRel >= threshold) {

						if(bestClanDescription_instRel >= threshold) {

							if(bestPfamAccession_instRel >= bestClanDescription_instRel) {
								clusterDescription = bestPfamDescription+" [pf]";
							}
							else {
								clusterDescription = bestClanDescription+" [pc]";
							}
						}
						else {
							clusterDescription = bestPfamDescription+" [pf]";
						}
					}
					else{

						if(bestClanDescription_instRel >= threshold) {
							clusterDescription = bestClanDescription+" [pc]";
						}
						else {

							if(bestSuperfamilyAccession_instRel >= threshold) {
								clusterDescription = bestSuperfamilyDescription+" [sf]";
							}
						}
					}


					ArrayList<String> codes = new ArrayList<String>();
					if(description2codes.containsKey(clusterDescription)) {
						codes = description2codes.get(clusterDescription);
					}

					codes.add(mdClusterCode);

					description2codes.put(clusterDescription, codes);

				}



				Enumeration<String> descriptions = description2codes.keys();
				while(descriptions.hasMoreElements()) {

					String description = descriptions.nextElement();

					if(description.startsWith("Uncharacterized MD-cluster")) {
						countClustersWithoutDescription++;
					}

					ArrayList<String> codes = description2codes.get(description);

					if(codes.size() == 1) {

						String code = codes.get(0);

						pstm4.setString(1, description);
						pstm4.setString(2, code);

						pstm4.executeUpdate();

						//System.out.println(code+"\t"+description);
					}
					else {	//multiple clusters are associated with same description
						int count = 0;

						for(String code: codes) {

							count++;

							String newDescription = description + " #"+count;

							pstm4.setString(1, newDescription);
							pstm4.setString(2, code);

							pstm4.executeUpdate();

							//System.out.println(code+"\t"+newDescription);
						}
					}
				}
				rs0.close();			

			}

			System.out.println("Number of clusters without description: " +countClustersWithoutDescription);


			stm.close();

			pstm0.close();
			pstm1.close();
			pstm2.close();
			pstm3.close();
			pstm4.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	public static void addStructuresInfo() {

		try {

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT count(*) " +
							"FROM clusters_mcl_structures " +
					"WHERE cluster_id=? AND cluster_threshold=?");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT count(*) " +
							"FROM fh_clusters_structures " +
					"WHERE code=?");

			Statement stm = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery(
					"SELECT id, " +
							"cluster_id,cluster_threshold,code,type,structures " +
					"FROM cp_clusters FOR UPDATE");

			while(rs.next()) {

				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");
				String code = rs.getString("code");
				String type = rs.getString("type");

				boolean structuresAvailable = false;

				if(type.equals("sc_cluster") || type.equals("md_cluster")) {

					pstm1.setInt(1, clusterID);
					pstm1.setFloat(2, clusterThreshold);
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {
						int countStructures = rs1.getInt("count(*)");
						if(countStructures>0) {
							structuresAvailable = true;
						}
					}
					rs1.close();
				}
				else if(type.equals("fh_cluster")) {

					pstm2.setString(1, code);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						int countStructures = rs2.getInt("count(*)");
						if(countStructures>0) {
							structuresAvailable = true;
						}
					}
					rs2.close();
				}

				if(structuresAvailable) {
					rs.updateString("structures", "Yes");
					rs.updateRow();
				}
				else{
					rs.updateString("structures", "No");
					rs.updateRow();
				}
			}
			rs.close();

			stm.close();
			pstm1.close();
			pstm2.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void addStructuresInfo2() {
		// uses the clusters_mcl_structures0 for updating the structure info in cp_clusters
		// used this function to update structure info in cp_clusters for fh_clusters_structures0 and also mcl_strcutres0
		// i.e >30% identity
		

		try {

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT count(*) " +
							"FROM clusters_mcl_structures0 " +
					"WHERE cluster_id=? AND cluster_threshold=?");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT count(*) " +
							"FROM fh_clusters_structures0 " +
					"WHERE code=?");

			Statement stm = CAMPS_CONNECTION.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs = stm.executeQuery(
					"SELECT id, " +
							"cluster_id,cluster_threshold,code,type,structures " +
					"FROM cp_clusters FOR UPDATE");
			int count  = 0;
			while(rs.next()) {
				count++;
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");
				String code = rs.getString("code");
				String type = rs.getString("type");

				boolean structuresAvailable = false;

				if(type.equals("sc_cluster") || type.equals("md_cluster")) {
					pstm1.setInt(1, clusterID);
					pstm1.setFloat(2, clusterThreshold);
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {
						int countStructures = rs1.getInt("count(*)");
						if(countStructures>0) {
							structuresAvailable = true;
						}
					}
					rs1.close();
				}
				
				else if(type.equals("fh_cluster")) {
				//if(type.equals("fh_cluster")) {
					pstm2.setString(1, code);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						int countStructures = rs2.getInt("count(*)");
						if(countStructures>0) {
							structuresAvailable = true;
						}
					}
					rs2.close();
				}

				if(structuresAvailable) {
					rs.updateString("structures", "Yes");
					rs.updateRow();
				}
				else{
					rs.updateString("structures", "No");
					rs.updateRow();
				}
				System.out.println("Prcessed: "+ count );
			}
			rs.close();

			stm.close();
			pstm1.close();
			pstm2.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
		
		Date startDate = new Date();
		System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
		
		CreateDatabaseTables.create_table_cp_clusters2();
		
		//CpClusters.run("/home/proj/check/RunMetaModel_gef/HMMs/CAMPS4_1/");
		//CpClusters2.run("F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/HMMs/CAMPS4_1/");
		CpClusters2.run("/home/users/saeed/reRUN_CAMPS/MetaModelFinalBackUp/HMMs/CAMPS4_1/");
		///F:\SC_Clust_postHmm\Results_hmm_new16Jan\RunMetaModel_gef\HMMs\CAMPS4_1
		
		DBAdaptor.createIndex("camps4","cp_clusters2",new String[]{"cluster_id","cluster_threshold","type"},"cindex1");
		DBAdaptor.createIndex("camps4","cp_clusters2",new String[]{"super_cluster_id","super_cluster_threshold"},"cindex2");
		DBAdaptor.createIndex("camps4","cp_clusters2",new String[]{"cluster_id","cluster_threshold"},"cindex3");
					
		Date endDate = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
	}

}
