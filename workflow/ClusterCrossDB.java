/*
 * ClusterCrossDB
 * 
 * Version 2.0
 * 
 * 2010-08-09
 * 
 */

package workflow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;

import utils.DBAdaptor;

public class ClusterCrossDB {

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");

	private static final double COVERAGE_THRESHOLD = 30;


	public static void runCath() {
		try {

			System.out.println("Insert CATH cross links");
			addCATHcrosslinks();

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
	public static void runGo() {
		try {

			System.out.println("Insert GO cross links");
			addGOcrosslinks();

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
	public static void runGPCR() {
		try {


			System.out.println("Insert GPCRDB cross links");
			addGPCRDBcrosslinks_map();


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
	public static void runOPM() {
		try {

			System.out.println("Insert OPM cross links");
			addOPMcrosslinks_map();


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
	public static void runPfam() {
		try {

			System.out.println("Insert PFAM cross links");
			addPFAMcrosslinks();

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
	public static void runSCOP() {
		try {

			System.out.println("Insert SCOP cross links");
			addSCOPcrosslinks();


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
	public static void runSuperfamily() {
		try {

			System.out.println("Insert SUPERFAMILY cross links");
			addSUPERFAMILYcrosslinks();

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
	public static void runTCDB() {
		try {
			System.out.println("Insert TCDB cross links");
			addTCDBcrosslinks_map();

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



	private static void addCATHcrosslinks() {
		try {
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_cross_db " +
							"(cluster_id, cluster_threshold, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?,?)");

			// pstm1 removed from here and now being made hash
			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl WHERE cluster_id=? AND cluster_threshold=?");
			System.out.print("\n Getting clusters from tables \n");
			ArrayList<ClusterDs> clusters = new ArrayList<ClusterDs>();
			PreparedStatement ptm1 = CAMPS_CONNECTION.prepareStatement("" +
					"select cluster_id,sequenceid from clusters_mcl where cluster_threshold=?");
			for(int j =5; j<= 100; j++){
				ptm1.setInt(1, j);
				ResultSet rs1 = ptm1.executeQuery();

				boolean check2 = false;

				ClusterDs temp = new ClusterDs();
				temp.clusterThreshold = j;

				while(rs1.next()){
					check2 = true;
					int clus = rs1.getInt(1);
					int sq = rs1.getInt(2);
					if(temp.list.containsKey(clus)){
						ArrayList<Integer> x = temp.list.get(clus);
						x.add(sq);
						temp.list.put(clus, x);
					}
					else{
						ArrayList<Integer> x = new ArrayList<Integer>();
						x.add(sq);
						temp.list.put(clus, x);
					}
				}
				rs1.close();
				// add temp back
				if (check2){
					check2 = false;
					clusters.add(temp);
				}
			}
			ptm1.close();


			//take only best PDBTM hit
			/*
				PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
						"SELECT distinct(sequences_other_database_sequenceid) " +
								"FROM other_database " +
								"WHERE sequenceid=? AND " +
								"db=\"pdbtm\" AND " +
								"query_coverage>="+COVERAGE_THRESHOLD+" AND "+
								"hit_coverage>="+COVERAGE_THRESHOLD+" "+
						"ORDER BY ident desc LIMIT 1");
			 */
			// geting pdbtm hit
			PreparedStatement ptm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid, sequences_other_database_sequenceid " +
							"FROM other_database " +
							"WHERE " +
							"db=\"pdbtm\" AND " +
							"query_coverage>="+COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc");
			System.out.print("\n Getting PDBTm hits \n");
			HashMap <Integer, Integer> ptbl2 = new HashMap<Integer, Integer>(); // key is the sequenceid and value is sequences_other_database_sequenceid

			ResultSet rptm2 = ptm2.executeQuery();
			while(rptm2.next()){
				int Sid = rptm2.getInt("sequenceid");
				int pdbtmSequenceid = rptm2.getInt("sequences_other_database_sequenceid");

				if(!ptbl2.containsKey(Sid)){
					ptbl2.put(Sid, pdbtmSequenceid);
				}
			}
			rptm2.close();
			ptm2.close();



			//PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT name FROM sequences_other_database WHERE sequenceid=? AND db=\"pdbtm\"");
			System.out.print("\n Getting cath classifications \n");
			HashMap <Integer, String> ptbl3 = new HashMap<Integer, String>(); // key is the seq_id and value is name
			PreparedStatement ptm3 = CAMPS_CONNECTION.prepareStatement("SELECT name,sequenceid FROM sequences_other_database WHERE db=\"pdbtm\"");
			ResultSet rptm3 = ptm3.executeQuery();
			while(rptm3.next()){
				String name = rptm3.getString(1);
				int id = rptm3.getInt(2);
				if(!ptbl3.containsKey(id)){
					ptbl3.put(id, name);
				}
			}
			ptm3.close();
			rptm3.close();

			//PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT classification FROM pdbtm2scop_cath WHERE pdb_id=? AND db=\"cath\"");
			// get all data in hash instead of pstm4

			System.out.print("\n Getting cath classifications \n");
			HashMap <String, String> ptbl4 = new HashMap<String, String>(); // key is the pdb_id and value is classification
			PreparedStatement ptm4 = CAMPS_CONNECTION.prepareStatement("SELECT classification,pdb_id FROM pdbtm2scop_cath WHERE db=\"cath\"");
			ResultSet rptm4 = ptm4.executeQuery();
			while(rptm4.next()){
				String c = rptm4.getString(1);
				String p = rptm4.getString(2);
				if(!ptbl4.containsKey(p)){
					ptbl4.put(p, c);
				}
				else if(ptbl4.containsKey(p)){
					String cc = ptbl4.get(p);
					c = c + "*" +cc;
				}
			}
			ptm4.close();
			rptm4.close();

			System.out.print("\n Getting cath descriptions \n");
			//PreparedStatement pstm5 = CAMPS_CONNECTION.prepareStatement("SELECT description FROM other_database_hierarchies WHERE key_=? AND db=\"cath\"");
			// get all data in hash instead of pstm5
			PreparedStatement ptm5 = CAMPS_CONNECTION.prepareStatement("SELECT description,key_ FROM other_database_hierarchies WHERE db=\"cath\"");
			ResultSet rptm5 = ptm5.executeQuery();
			HashMap <String, String> ptbl5 = new HashMap<String, String>(); // key is the key_ and value is description
			while(rptm5.next()){
				String d = rptm5.getString(1);
				String k = rptm5.getString(2);
				if(!ptbl5.containsKey(k)){
					ptbl5.put(k, d);
				}
			}
			ptm5.close();
			rptm5.close();
			System.out.print("\n Running over all clusters Now \n");

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold FROM clusters_mcl_nr_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");

				//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl WHERE cluster_id=? AND cluster_threshold=?");

				//extract cluster members
				BitSet members = new BitSet();
				ClusterDs t = new ClusterDs();
				for(int i =0; i<=clusters.size()-1;i++){
					if(clusters.get(i).clusterThreshold == clusterThreshold){
						t = clusters.get(i);
						break;
					}
				}
				ArrayList<Integer> sids = t.list.get(clusterID);

				//pstm1.setInt(1, clusterID);
				//pstm1.setFloat(2, clusterThreshold);
				//ResultSet rs1 = pstm1.executeQuery();
				/*
				 * while(rs1.next()) {
						int sequenceID = rs1.getInt("sequenceid");
						members.set(sequenceID);
					}
					rs1.close();

				for(int i =0 ; i<=sids.size()-1;i++){
					members.set(sids.get(i));
				}
				 */
				t = null;
				//sids = null;

				//int clusterSize = members.cardinality();
				int clusterSize = sids.size();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get pdbtm and cath data for each member
				//for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {
				for(int i=0;i<=sids.size()-1;i++){

					int sequenceid = sids.get(i);
					//pstm2.setInt(1, sequenceid);
					//ResultSet rs2 = pstm2.executeQuery();
					if (ptbl2.containsKey(sequenceid)){
						//while(rs2.next()) {

						//int pdbtmSequenceid = rs2.getInt("sequences_other_database_sequenceid");
						int pdbtmSequenceid = ptbl2.get(sequenceid);

						//pstm3.setInt(1, pdbtmSequenceid);
						//ResultSet rs3 = pstm3.executeQuery();
						//while(rs3.next()) {



						//String pdbCode = rs3.getString("name");
						if(ptbl3.containsKey(pdbtmSequenceid)){
							String pdbCode = ptbl3.get(pdbtmSequenceid);

							//
							//caution: pdb sequences can have multiple CATH
							//classifications (multidomain sequences!)
							//
							String link = "";

							// commented out changes here due to hashtable

							//pstm4.setString(1, pdbCode);
							//ResultSet rs4 = pstm4.executeQuery();

							//while(rs4.next()) {
							//String cathClassification = rs4.getString("classification");

							if(ptbl4.containsKey(pdbCode)){

								/*
								String cathClassification = ptbl4.get(pdbCode);// retrieve the classification for this pdb code
								String tmp[] = cathClassification.split("\\.");
								String cathFoldClassification = tmp[0]+"."+tmp[1]+"."+tmp[2];
								String cathDescription = "";
								 */
								String cathClassifications = ptbl4.get(pdbCode);// retrieve the classification for this pdb code
								String cathclasses[] = cathClassifications.split("\\*");

								for(int x =0;x<=cathclasses.length-1;x++){
									String tmp[] = cathclasses[x].split("\\.");
									String cathFoldClassification = tmp[0]+"."+tmp[1]+"."+tmp[2];

									String cathDescription = "";

									if (ptbl5.containsKey(cathFoldClassification)){
										cathDescription = ptbl5.get(cathFoldClassification);
									}

									String tmpLink = cathFoldClassification+" - " +cathDescription;
									link += "#"+tmpLink + "*";
								}
								//String tmp[] = scopClassification.split("\\.");


								// commenting out pstm5 and replacing with hash table ptm5

								//pstm5.setString(1, cathFoldClassification);
								//ResultSet rs5 = pstm5.executeQuery();
								//while(rs5.next()) {
								//cathDescription = rs5.getString("description");
								//}
								//rs5.close();

							}
							//}
							//rs4.close();

							if(link.equals("")) {
								continue;
							}

							link = link.substring(1);

							int count = 0;
							if(link2count.containsKey(link)) {
								count = link2count.get(link);
							}
							count = count+1;

							link2count.put(link, new Integer(count));
						}
						//rs3.close();
						//}
					}
					//rs2.close();
				}


				//insert cross links

				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setInt(1, clusterID);
					pstm.setFloat(2, clusterThreshold);
					pstm.setString(3, "cath");
					pstm.setString(4, link);
					pstm.setInt(5, count);
					pstm.setDouble(6, perc);

					pstm.executeUpdate();
				}

				if (statusCount % 1000 == 0){
					System.out.print(statusCount+"\n");
				}
				//pstm1.close();
			}
			rs.close();
			stm.close();


			pstm.close();
			//pstm1.close();
			//pstm2.close();
			//pstm3.close();
			//pstm4.close();
			//pstm5.close();

		}catch(Exception e) {
			System.err.println("Exception in ClusterCrossDB.addCATHcrosslinks(): " +e.getMessage());
			e.printStackTrace();
		}
	}


	private static void addGOcrosslinks() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_cross_db " +
							"(cluster_id, cluster_threshold, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?,?)");



			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl WHERE cluster_id=? AND cluster_threshold=?");			

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT distinct(accession) FROM go_annotations WHERE sequenceid=?");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT name,term_type FROM go_annotations WHERE accession=? limit 1");


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold FROM clusters_mcl_nr_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				BitSet members = new BitSet();
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					int sequenceID = rs1.getInt("sequenceid");
					members.set(sequenceID);
				}
				rs1.close();

				int clusterSize = members.cardinality();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get go annotations for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					pstm2.setInt(1, sequenceid);					
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {

						String acc = rs2.getString("accession");					

						int count = 0;
						if(link2count.containsKey(acc)) {
							count = link2count.get(acc);
						}
						count = count+1;

						link2count.put(acc, new Integer(count));
					}
					rs2.close();

				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String acc = en.nextElement();						

					int count = link2count.get(acc).intValue();

					double perc = 100 * ((double) count/clusterSize);

					String description = "";
					String type = "";
					pstm3.setString(1, acc);
					ResultSet rs3 = pstm3.executeQuery();
					while(rs3.next()) {
						description = rs3.getString("name");
						type = rs3.getString("term_type");
					}
					rs3.close();

					if(type.equals("biological_process")) {
						type = "BP";
					}
					else if(type.equals("cellular_component")) {
						type = "CC";
					}
					else if(type.equals("molecular_function")) {
						type = "MF";
					}

					pstm.setInt(1, clusterID);
					pstm.setFloat(2, clusterThreshold);
					pstm.setString(3, "go");
					pstm.setString(4, acc+" - " +type + " - " +description);
					pstm.setInt(5, count);
					pstm.setDouble(6, perc);

					pstm.executeUpdate();
				}


				if (statusCount % 10000 == 0){
					System.out.print(statusCount+"\n");
				}
			}
			rs.close();
			stm.close();


			pstm.close();
			pstm1.close();
			pstm2.close();
			pstm3.close();

		}catch(Exception e) {
			System.err.println("Exception in ClusterCrossDB.addGOcrosslinks(): " +e.getMessage());
			e.printStackTrace();
		}
	}

	private static void addGPCRDBcrosslinks_map() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_cross_db " +
							"(cluster_id, cluster_threshold, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?,?)");

			//Get Clusters from Table
			System.out.print("\n Getting clusters from tables \n");
			ArrayList<ClusterDs> clusters = new ArrayList<ClusterDs>();
			PreparedStatement ptm1 = CAMPS_CONNECTION.prepareStatement("SELECT cluster_id,sequenceid FROM clusters_mcl WHERE cluster_threshold=?");
			for(int j =5; j<= 100; j++){
				ptm1.setInt(1, j);
				ResultSet rs1 = ptm1.executeQuery();
				boolean check2 = false;

				ClusterDs temp = new ClusterDs();
				temp.clusterThreshold = j;

				while(rs1.next()){
					check2 = true;
					int clus = rs1.getInt(1);
					int sq = rs1.getInt(2);
					if(temp.list.containsKey(clus)){
						ArrayList<Integer> x = temp.list.get(clus);
						x.add(sq);
						temp.list.put(clus, x);
					}
					else{
						ArrayList<Integer> x = new ArrayList<Integer>();
						x.add(sq);
						temp.list.put(clus, x);
					}
				}
				rs1.close();
				// add temp back
				if (check2){
					check2 = false;
					clusters.add(temp);
				}
			}
			ptm1.close();

			//take only best GPCRDB hit
			PreparedStatement ptm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid,sequences_other_database_sequenceid " +
							"FROM other_database " +
							"WHERE " +
							"db=\"gpcrdb\" AND " +
							"query_coverage>="+COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc");
			System.out.print("\n Getting GPCRdb hits \n");
			HashMap <Integer, Integer> ptbl2 = new HashMap<Integer, Integer>(); // key is the sequenceid and value is sequences_other_database_sequenceid

			ResultSet rptm2 = ptm2.executeQuery();
			while(rptm2.next()){
				int Sid = rptm2.getInt("sequenceid");
				int gpcrdbSequenceid = rptm2.getInt("sequences_other_database_sequenceid");

				if(!ptbl2.containsKey(Sid)){
					ptbl2.put(Sid, gpcrdbSequenceid);
				}
			}
			rptm2.close();
			ptm2.close();



			PreparedStatement ptm3 = CAMPS_CONNECTION.prepareStatement("SELECT additional_information,sequenceid FROM sequences_other_database WHERE db=\"gpcrdb\"");
			System.out.print("\n Getting additionalInfo of gpcrdb proteins \n");
			HashMap <Integer, String> ptbl3 = new HashMap<Integer, String>(); // key is the seq_id and value is additional Info
			ResultSet rptm3 = ptm3.executeQuery();
			while(rptm3.next()){
				String info = rptm3.getString(1);
				int id = rptm3.getInt(2);
				if(!ptbl3.containsKey(id)){
					ptbl3.put(id, info);
				}
			}
			ptm3.close();
			rptm3.close();


			PreparedStatement ptm4 = CAMPS_CONNECTION.prepareStatement("SELECT description,key_ FROM other_database_hierarchies WHERE db=\"gpcrdb\"");
			ResultSet rptm4 = ptm4.executeQuery();
			HashMap <String, String> ptbl4 = new HashMap<String, String>(); // key is the key_ and value is description
			while(rptm4.next()){
				String d = rptm4.getString(1);
				String k = rptm4.getString(2);
				if(!ptbl4.containsKey(k)){
					ptbl4.put(k, d);
				}
			}
			ptm4.close();
			rptm4.close();


			System.out.print("\n Running over all clusters Now \n");
			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold FROM clusters_mcl_nr_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				ClusterDs t = new ClusterDs();
				for(int i =0; i<=clusters.size()-1;i++){
					if(clusters.get(i).clusterThreshold == clusterThreshold){
						t = clusters.get(i);
						break;
					}
				}
				ArrayList<Integer> sids = t.list.get(clusterID);
				t= null;
				int clusterSize = sids.size();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get gpcrdb data for each member
				for(int i=0;i<=sids.size()-1;i++) {
					int sequenceid = sids.get(i);
					if (ptbl2.containsKey(sequenceid)){
						int gpcrdbSequenceid = ptbl2.get(sequenceid);
						if (ptbl3.containsKey(gpcrdbSequenceid)){
							String gpcrdbClassification = ptbl3.get(gpcrdbSequenceid);
							String gpcrdbDescription = "";
							if (ptbl4.containsKey(gpcrdbClassification)){
								gpcrdbDescription = ptbl4.get(gpcrdbClassification);
							}
							String link = gpcrdbClassification+" - " +gpcrdbDescription;
							int count = 0;
							if(link2count.containsKey(link)) {
								count = link2count.get(link);
							}
							count = count+1;
							link2count.put(link, new Integer(count));
						}
					}
				}

				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setInt(1, clusterID);
					pstm.setFloat(2, clusterThreshold);
					pstm.setString(3, "gpcrdb");
					pstm.setString(4, link);
					pstm.setInt(5, count);
					pstm.setDouble(6, perc);

					pstm.executeUpdate();
				}
				if (statusCount % 10000 == 0){
					System.out.print(statusCount+"\n");
				}
			}
			rs.close();
			stm.close();


			pstm.close();


		}catch(Exception e) {
			System.err.println("Exception in ClusterCrossDB.addGPCRDBcrosslinks(): " +e.getMessage());
			e.printStackTrace();
		}
	}
	private static void addGPCRDBcrosslinks() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_cross_db " +
							"(cluster_id, cluster_threshold, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?,?)");



			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl WHERE cluster_id=? AND cluster_threshold=?");

			//take only best GPCRDB hit
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT distinct(sequences_other_database_sequenceid) " +
							"FROM other_database " +
							"WHERE sequenceid=? AND " +
							"db=\"gpcrdb\" AND " +
							"query_coverage>="+COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc LIMIT 1");
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT additional_information FROM sequences_other_database WHERE sequenceid=? AND db=\"gpcrdb\"");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT description FROM other_database_hierarchies WHERE key_=? AND db=\"gpcrdb\"");


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold FROM clusters_mcl_nr_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				BitSet members = new BitSet();
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					int sequenceID = rs1.getInt("sequenceid");
					members.set(sequenceID);
				}
				rs1.close();

				int clusterSize = members.cardinality();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get gpcrdb data for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					pstm2.setInt(1, sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {

						int gpcrdbSequenceid = rs2.getInt("sequences_other_database_sequenceid");

						pstm3.setInt(1, gpcrdbSequenceid);
						ResultSet rs3 = pstm3.executeQuery();
						while(rs3.next()) {

							String gpcrdbClassification = rs3.getString("additional_information");

							String gpcrdbDescription = "";
							pstm4.setString(1, gpcrdbClassification);
							ResultSet rs4 = pstm4.executeQuery();
							while(rs4.next()) {
								gpcrdbDescription = rs4.getString("description");
							}
							rs4.close();

							String link = gpcrdbClassification+" - " +gpcrdbDescription;

							int count = 0;
							if(link2count.containsKey(link)) {
								count = link2count.get(link);
							}
							count = count+1;

							link2count.put(link, new Integer(count));

						}
						rs3.close();
					}
					rs2.close();
				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setInt(1, clusterID);
					pstm.setFloat(2, clusterThreshold);
					pstm.setString(3, "gpcrdb");
					pstm.setString(4, link);
					pstm.setInt(5, count);
					pstm.setDouble(6, perc);

					pstm.executeUpdate();
				}
				if (statusCount % 10000 == 0){
					System.out.print(statusCount+"\n");
				}
			}
			rs.close();
			stm.close();


			pstm.close();
			pstm1.close();
			pstm2.close();
			pstm3.close();
			pstm4.close();

		}catch(Exception e) {
			System.err.println("Exception in ClusterCrossDB.addGPCRDBcrosslinks(): " +e.getMessage());
			e.printStackTrace();
		}
	}

	private static void addOPMcrosslinks_map() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_cross_db " +
							"(cluster_id, cluster_threshold, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?,?)");


			PreparedStatement ptm1 = CAMPS_CONNECTION.prepareStatement("SELECT cluster_id,sequenceid FROM clusters_mcl WHERE cluster_threshold=?");
			System.out.print("\n Getting clusters from tables \n");
			ArrayList<ClusterDs> clusters = new ArrayList<ClusterDs>();
			for(int j =5; j<= 100; j++){
				ptm1.setInt(1, j);
				ResultSet rs1 = ptm1.executeQuery();

				boolean check2 = false;

				ClusterDs temp = new ClusterDs();
				temp.clusterThreshold = j;

				while(rs1.next()){
					check2 = true;
					int clus = rs1.getInt(1);
					int sq = rs1.getInt(2);
					if(temp.list.containsKey(clus)){
						ArrayList<Integer> x = temp.list.get(clus);
						x.add(sq);
						temp.list.put(clus, x);
					}
					else{
						ArrayList<Integer> x = new ArrayList<Integer>();
						x.add(sq);
						temp.list.put(clus, x);
					}
				}
				rs1.close();
				// add temp back
				if (check2){
					check2 = false;
					clusters.add(temp);
				}
			}
			ptm1.close();


			//take only best PDBTM hit
			PreparedStatement ptm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid, sequences_other_database_sequenceid " +
							"FROM other_database " +
							"WHERE " +
							"db=\"pdbtm\" AND " +
							"query_coverage>="+COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc");
			System.out.print("\n Getting PDBTm hits \n");
			HashMap <Integer, Integer> ptbl2 = new HashMap<Integer, Integer>(); // key is the sequenceid and value is sequences_other_database_sequenceid
			ResultSet rptm2 = ptm2.executeQuery();
			while(rptm2.next()){
				int Sid = rptm2.getInt("sequenceid");
				int pdbtmSequenceid = rptm2.getInt("sequences_other_database_sequenceid");
				if(!ptbl2.containsKey(Sid)){
					ptbl2.put(Sid, pdbtmSequenceid);
				}
			}
			rptm2.close();
			ptm2.close();



			System.out.print("\n Getting names \n");
			HashMap <Integer, String> ptbl3 = new HashMap<Integer, String>(); // key is the seq_id and value is name
			PreparedStatement ptm3 = CAMPS_CONNECTION.prepareStatement("SELECT name,sequenceid FROM sequences_other_database WHERE db=\"pdbtm\"");
			ResultSet rptm3 = ptm3.executeQuery();
			while(rptm3.next()){
				String name = rptm3.getString(1);
				int id = rptm3.getInt(2);
				if(!ptbl3.containsKey(id)){
					ptbl3.put(id, name);
				}
			}
			ptm3.close();
			rptm3.close();


			System.out.print("\n Getting opm classifications \n");
			HashMap <String, String> ptbl4 = new HashMap<String, String>(); // key is the pdb_id and value is classification
			PreparedStatement ptm4 = CAMPS_CONNECTION.prepareStatement("SELECT classification,pdb_id FROM pdbtm2opm ");
			ResultSet rptm4 = ptm4.executeQuery();
			while(rptm4.next()){
				String c = rptm4.getString(1); // classification opm
				String p = rptm4.getString(2); // pdb Id
				if(!ptbl4.containsKey(p)){
					ptbl4.put(p, c);
				}
				else if(ptbl4.containsKey(p)){
					String cc = ptbl4.get(p);
					c = c + "*" +cc;
				}
			}
			ptm4.close();
			rptm4.close();




			PreparedStatement ptm5 = CAMPS_CONNECTION.prepareStatement("SELECT description,key_ FROM other_database_hierarchies WHERE db=\"opm\"");
			System.out.print("\n Getting opm descriptions \n");
			ResultSet rptm5 = ptm5.executeQuery();
			HashMap <String, String> ptbl5 = new HashMap<String, String>(); // key is the key_ and value is description
			while(rptm5.next()){
				String d = rptm5.getString(1);
				String k = rptm5.getString(2);
				if(!ptbl5.containsKey(k)){
					ptbl5.put(k, d);
				}
			}
			ptm5.close();
			rptm5.close();





			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold FROM clusters_mcl_nr_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");
				//extract cluster members
				ClusterDs t = new ClusterDs();
				for(int i =0; i<=clusters.size()-1;i++){
					if(clusters.get(i).clusterThreshold == clusterThreshold){
						t = clusters.get(i);
						break;
					}
				}
				ArrayList<Integer> sids = t.list.get(clusterID);
				t = null;
				int clusterSize = sids.size();

				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();
				//get pdbtm and opm data for each member
				for(int i=0;i<=sids.size()-1;i++) {
					int sequenceid = sids.get(i);
					int pdbtmSequenceid = 0;
					if(ptbl2.containsKey(sequenceid)){
						pdbtmSequenceid = ptbl2.get(sequenceid);
					}
					else {
						continue;
					}

					String pdbCode = ptbl3.get(pdbtmSequenceid);
					pdbCode = pdbCode.split("_")[0].toLowerCase();
					String opmClassification = "";
					if(ptbl4.containsKey(pdbCode)){
						opmClassification = ptbl4.get(pdbCode);
					}
					else{
						continue;
					}

					String description = "";
					description = ptbl5.get(opmClassification);
					String link = opmClassification+" - " +description;

					int count = 0;
					if(link2count.containsKey(link)) {
						count = link2count.get(link);
					}
					count = count+1;

					link2count.put(link, new Integer(count));
				}
				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setInt(1, clusterID);
					pstm.setFloat(2, clusterThreshold);
					pstm.setString(3, "opm");
					pstm.setString(4, link);
					pstm.setInt(5, count);
					pstm.setDouble(6, perc);

					pstm.executeUpdate();
				}
				if (statusCount % 10000 == 0){
					System.out.print(statusCount+"\n");
				}
			}
			rs.close();
			stm.close();
			pstm.close();

		}catch(Exception e) {
			System.err.println("Exception in ClusterCrossDB.addOPMcrosslinks(): " +e.getMessage());
			e.printStackTrace();
		}
	}

	private static void addOPMcrosslinks() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_cross_db " +
							"(cluster_id, cluster_threshold, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?,?)");



			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl WHERE cluster_id=? AND cluster_threshold=?");

			//take only best PDBTM hit
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT distinct(sequences_other_database_sequenceid) " +
							"FROM other_database " +
							"WHERE sequenceid=? AND " +
							"db=\"pdbtm\" AND " +
							"query_coverage>="+COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc LIMIT 1");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT name FROM sequences_other_database WHERE sequenceid=? AND db=\"pdbtm\"");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT classification FROM pdbtm2opm WHERE pdb_id=?");
			PreparedStatement pstm5 = CAMPS_CONNECTION.prepareStatement("SELECT description FROM other_database_hierarchies WHERE key_=? AND db=\"opm\"");


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold FROM clusters_mcl_nr_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				BitSet members = new BitSet();
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					int sequenceID = rs1.getInt("sequenceid");
					members.set(sequenceID);
				}
				rs1.close();

				int clusterSize = members.cardinality();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get pdbtm and opm data for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					pstm2.setInt(1, sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {

						int pdbtmSequenceid = rs2.getInt("sequences_other_database_sequenceid");

						pstm3.setInt(1, pdbtmSequenceid);
						ResultSet rs3 = pstm3.executeQuery();
						while(rs3.next()) {

							String pdbCode = rs3.getString("name");
							pdbCode = pdbCode.split("_")[0].toLowerCase();

							pstm4.setString(1, pdbCode);
							ResultSet rs4 = pstm4.executeQuery();
							while(rs4.next()) {
								String opmClassification = rs4.getString("classification");

								String description = "";
								pstm5.setString(1, opmClassification);
								ResultSet rs5 = pstm5.executeQuery();
								while(rs5.next()) {
									description = rs5.getString("description");
								}
								rs5.close();

								String link = opmClassification+" - " +description;

								int count = 0;
								if(link2count.containsKey(link)) {
									count = link2count.get(link);
								}
								count = count+1;

								link2count.put(link, new Integer(count));
							}
							rs4.close();
						}
						rs3.close();
					}
					rs2.close();
				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setInt(1, clusterID);
					pstm.setFloat(2, clusterThreshold);
					pstm.setString(3, "opm");
					pstm.setString(4, link);
					pstm.setInt(5, count);
					pstm.setDouble(6, perc);

					pstm.executeUpdate();
				}
				if (statusCount % 10000 == 0){
					System.out.print(statusCount+"\n");
				}
			}
			rs.close();
			stm.close();


			pstm.close();
			pstm1.close();
			pstm2.close();
			pstm3.close();
			pstm4.close();
			pstm5.close();

		}catch(Exception e) {
			System.err.println("Exception in ClusterCrossDB.addOPMcrosslinks(): " +e.getMessage());
			e.printStackTrace();
		}
	}


	private static void addPFAMcrosslinks() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_cross_db " +
							"(cluster_id, cluster_threshold, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?,?)");



			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl WHERE cluster_id=? AND cluster_threshold=?");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT accession,description FROM domains_pfam WHERE sequenceid=? ORDER BY begin");


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold FROM clusters_mcl_nr_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				BitSet members = new BitSet();
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					int sequenceID = rs1.getInt("sequenceid");
					members.set(sequenceID);
				}
				rs1.close();

				int clusterSize = members.cardinality();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get pfam data for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					//
					//caution: sequences can have multiple PFAM
					//classifications (multidomain sequences!)
					//
					String link = "";

					pstm2.setInt(1, sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {

						String pfamAccession = rs2.getString("accession");
						String pfamDescription= rs2.getString("description");

						String tmpLink = pfamAccession+" - " +pfamDescription;
						link += "#"+tmpLink;												
					}

					if(link.equals("")) {
						continue;
					}

					link = link.substring(1);

					int count = 0;
					if(link2count.containsKey(link)) {
						count = link2count.get(link);
					}
					count = count+1;

					link2count.put(link, new Integer(count));


					rs2.close();
				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setInt(1, clusterID);
					pstm.setFloat(2, clusterThreshold);
					pstm.setString(3, "pfam");
					pstm.setString(4, link);
					pstm.setInt(5, count);
					pstm.setDouble(6, perc);

					pstm.executeUpdate();
				}
				if (statusCount % 10000 == 0){
					System.out.print(statusCount+"\n");
				}
			}
			rs.close();
			stm.close();


			pstm.close();
			pstm1.close();
			pstm2.close();

		}catch(Exception e) {
			System.err.println("Exception in ClusterCrossDB.addPFAMcrosslinks(): " +e.getMessage());
			e.printStackTrace();
		}
	}


	private static void addSCOPcrosslinks() {
		try {
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_cross_db " +
							"(cluster_id, cluster_threshold, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?,?)");

			System.out.print("\n Getting clusters from tables \n");
			ArrayList<ClusterDs> clusters = new ArrayList<ClusterDs>();
			PreparedStatement ptm1 = CAMPS_CONNECTION.prepareStatement("" +
					"select cluster_id,sequenceid from clusters_mcl where cluster_threshold=?");
			for(int j =5; j<= 100; j++){
				ptm1.setInt(1, j);
				ResultSet rs1 = ptm1.executeQuery();

				boolean check2 = false;

				ClusterDs temp = new ClusterDs();
				temp.clusterThreshold = j;

				while(rs1.next()){
					check2 = true;
					int clus = rs1.getInt(1);
					int sq = rs1.getInt(2);
					if(temp.list.containsKey(clus)){
						ArrayList<Integer> x = temp.list.get(clus);
						x.add(sq);
						temp.list.put(clus, x);
					}
					else{
						ArrayList<Integer> x = new ArrayList<Integer>();
						x.add(sq);
						temp.list.put(clus, x);
					}
				}
				rs1.close();
				// add temp back
				if (check2){
					check2 = false;
					clusters.add(temp);
				}
			}
			ptm1.close();

			// geting pdbtm hit
			PreparedStatement ptm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid, sequences_other_database_sequenceid " +
							"FROM other_database " +
							"WHERE " +
							"db=\"pdbtm\" AND " +
							"query_coverage>="+COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc");
			System.out.print("\n Getting PDBTm hits \n");
			HashMap <Integer, Integer> ptbl2 = new HashMap<Integer, Integer>(); // key is the sequenceid and value is sequences_other_database_sequenceid

			ResultSet rptm2 = ptm2.executeQuery();
			while(rptm2.next()){
				int Sid = rptm2.getInt("sequenceid");
				int pdbtmSequenceid = rptm2.getInt("sequences_other_database_sequenceid");

				if(!ptbl2.containsKey(Sid)){
					ptbl2.put(Sid, pdbtmSequenceid);
				}
			}
			rptm2.close();
			ptm2.close();

			System.out.print("\n Getting scop classifications \n");
			HashMap <Integer, String> ptbl3 = new HashMap<Integer, String>(); // key is the seq_id and value is name
			PreparedStatement ptm3 = CAMPS_CONNECTION.prepareStatement("SELECT name,sequenceid FROM sequences_other_database WHERE db=\"pdbtm\"");
			ResultSet rptm3 = ptm3.executeQuery();
			while(rptm3.next()){
				String name = rptm3.getString(1);
				int id = rptm3.getInt(2);
				if(!ptbl3.containsKey(id)){
					ptbl3.put(id, name);
				}
			}
			ptm3.close();
			rptm3.close();

			// get all data in hash instead of pstm4

			System.out.print("\n Getting scop classifications \n");
			HashMap <String, String> ptbl4 = new HashMap<String, String>(); // key is the pdb_id and value is classification
			PreparedStatement ptm4 = CAMPS_CONNECTION.prepareStatement("SELECT classification,pdb_id FROM pdbtm2scop_cath WHERE db=\"scop\"");
			ResultSet rptm4 = ptm4.executeQuery();
			while(rptm4.next()){
				String c = rptm4.getString(1);
				String p = rptm4.getString(2);
				if(!ptbl4.containsKey(p)){
					ptbl4.put(p, c);
				}
				else if(ptbl4.containsKey(p)){
					String cc = ptbl4.get(p);
					c = c + "*" +cc;
				}
			}
			ptm4.close();
			rptm4.close();

			System.out.print("\n Getting scop descriptions \n");

			// get all data in hash instead of pstm5
			PreparedStatement ptm5 = CAMPS_CONNECTION.prepareStatement("SELECT description,key_ FROM other_database_hierarchies WHERE db=\"scop\"");
			ResultSet rptm5 = ptm5.executeQuery();
			HashMap <String, String> ptbl5 = new HashMap<String, String>(); // key is the key_ and value is description
			while(rptm5.next()){
				String d = rptm5.getString(1);
				String k = rptm5.getString(2);
				if(!ptbl5.containsKey(k)){
					ptbl5.put(k, d);
				}
			}
			ptm5.close();
			rptm5.close();
			System.out.print("\n Running over all clusters Now \n");

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold FROM clusters_mcl_nr_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				//extract cluster members

				ClusterDs t = new ClusterDs();
				for(int i =0; i<=clusters.size()-1;i++){
					if(clusters.get(i).clusterThreshold == clusterThreshold){
						t = clusters.get(i);
						break;
					}
				}
				ArrayList<Integer> sids = t.list.get(clusterID);

				t = null;
				int clusterSize = sids.size();

				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				for(int i=0;i<=sids.size()-1;i++){

					int sequenceid = sids.get(i);
					if (ptbl2.containsKey(sequenceid)){
						int pdbtmSequenceid = ptbl2.get(sequenceid);

						if(ptbl3.containsKey(pdbtmSequenceid)){
							String pdbCode = ptbl3.get(pdbtmSequenceid);

							//caution: pdb sequences can have multiple CATH
							//classifications (multidomain sequences!)
							//
							String link = "";
							// commented out changes here due to hashtable
							if(ptbl4.containsKey(pdbCode)){

								String scopClassifications = ptbl4.get(pdbCode);// retrieve the classification for this pdb code
								String sclasses[] = scopClassifications.split("\\*");
								for(int x =0;x<=sclasses.length-1;x++){
									String tmp[] = sclasses[x].split("\\.");
									String scopFoldClassification = tmp[0]+"."+tmp[1]+"."+tmp[2];

									String scopDescription = "";

									if (ptbl5.containsKey(scopFoldClassification)){
										scopDescription = ptbl5.get(scopFoldClassification);
									}

									String tmpLink = scopFoldClassification+" - " +scopDescription;
									link += "#"+tmpLink;
								}
								//String tmp[] = scopClassification.split("\\.");

							}

							if(link.equals("")) {
								continue;
							}

							link = link.substring(1);

							int count = 0;
							if(link2count.containsKey(link)) {
								count = link2count.get(link);
							}
							count = count+1;

							link2count.put(link, new Integer(count));
						}
					}
				}

				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setInt(1, clusterID);
					pstm.setFloat(2, clusterThreshold);
					pstm.setString(3, "scop");
					pstm.setString(4, link);
					pstm.setInt(5, count);
					pstm.setDouble(6, perc);

					pstm.executeUpdate();
				}

				if (statusCount % 1000 == 0){
					System.out.print(statusCount+"\n");
				}
			}
			rs.close();
			stm.close();


			pstm.close();
			/*
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_cross_db " +
							"(cluster_id, cluster_threshold, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?,?)");



			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl WHERE cluster_id=? AND cluster_threshold=?");

			//take only best PDBTM hit
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT distinct(sequences_other_database_sequenceid) " +
							"FROM other_database " +
							"WHERE sequenceid=? AND " +
							"db=\"pdbtm\" AND " +
							"query_coverage>="+COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc LIMIT 1");
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT name FROM sequences_other_database WHERE sequenceid=? AND db=\"pdbtm\"");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT classification FROM pdbtm2scop_cath WHERE pdb_id=? AND db=\"scop\"");
			PreparedStatement pstm5 = CAMPS_CONNECTION.prepareStatement("SELECT description FROM other_database_hierarchies WHERE key_=? AND db=\"scop\"");


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold FROM clusters_mcl_nr_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				BitSet members = new BitSet();
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					int sequenceID = rs1.getInt("sequenceid");
					members.set(sequenceID);
				}
				rs1.close();

				int clusterSize = members.cardinality();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get pdbtm and scop data for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					pstm2.setInt(1, sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {

						int pdbtmSequenceid = rs2.getInt("sequences_other_database_sequenceid");

						pstm3.setInt(1, pdbtmSequenceid);
						ResultSet rs3 = pstm3.executeQuery();
						while(rs3.next()) {

							String pdbCode = rs3.getString("name");

							//
							//caution: pdb sequences can have multiple SCOP
							//classifications (multidomain sequences!)
							//
							String link = "";

							pstm4.setString(1, pdbCode);
							ResultSet rs4 = pstm4.executeQuery();
							while(rs4.next()) {
								String scopClassification = rs4.getString("classification");
								String tmp[] = scopClassification.split("\\.");
								String scopFoldClassification = tmp[0]+"."+tmp[1];

								String scopDescription = "";
								pstm5.setString(1, scopFoldClassification);
								ResultSet rs5 = pstm5.executeQuery();
								while(rs5.next()) {
									scopDescription = rs5.getString("description");
								}
								rs5.close();

								String tmpLink = scopFoldClassification+" - " +scopDescription;
								link += "#"+tmpLink;							
							}
							rs4.close();

							if(link.equals("")) {
								continue;
							}

							link = link.substring(1);

							int count = 0;
							if(link2count.containsKey(link)) {
								count = link2count.get(link);
							}
							count = count+1;

							link2count.put(link, new Integer(count));							
						}
						rs3.close();
					}
					rs2.close();
				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setInt(1, clusterID);
					pstm.setFloat(2, clusterThreshold);
					pstm.setString(3, "scop");
					pstm.setString(4, link);
					pstm.setInt(5, count);
					pstm.setDouble(6, perc);

					pstm.executeUpdate();
				}
				if (statusCount % 10000 == 0){
					System.out.print(statusCount+"\n");
				}
			}
			rs.close();
			stm.close();


			pstm.close();
			pstm1.close();
			pstm2.close();
			pstm3.close();
			pstm4.close();
			pstm5.close();
			 */
		}catch(Exception e) {
			System.err.println("Exception in ClusterCrossDB.addSCOPcrosslinks(): " +e.getMessage());
			e.printStackTrace();
		}
	}


	private static void addSUPERFAMILYcrosslinks() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_cross_db " +
							"(cluster_id, cluster_threshold, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?,?)");



			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl WHERE cluster_id=? AND cluster_threshold=?");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT accession,description FROM domains_superfamily WHERE sequenceid=? ORDER BY begin");


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold FROM clusters_mcl_nr_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				BitSet members = new BitSet();
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					int sequenceID = rs1.getInt("sequenceid");
					members.set(sequenceID);
				}
				rs1.close();

				int clusterSize = members.cardinality();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get superfamily data for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					//
					//caution: sequences can have multiple PFAM
					//classifications (multidomain sequences!)
					//
					String link = "";

					pstm2.setInt(1, sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {

						String sfAccession = rs2.getString("accession");
						String sfDescription= rs2.getString("description");

						String tmpLink = sfAccession+" - " +sfDescription;
						link += "#"+tmpLink;												
					}

					if(link.equals("")) {
						continue;
					}

					link = link.substring(1);

					int count = 0;
					if(link2count.containsKey(link)) {
						count = link2count.get(link);
					}
					count = count+1;

					link2count.put(link, new Integer(count));


					rs2.close();
				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setInt(1, clusterID);
					pstm.setFloat(2, clusterThreshold);
					pstm.setString(3, "superfamily");
					pstm.setString(4, link);
					pstm.setInt(5, count);
					pstm.setDouble(6, perc);

					pstm.executeUpdate();
				}
				if (statusCount % 10000 == 0){
					System.out.print(statusCount+"\n");
				}
			}
			rs.close();
			stm.close();


			pstm.close();
			pstm1.close();
			pstm2.close();

		}catch(Exception e) {
			System.err.println("Exception in ClusterCrossDB.addSUPERFAMILYcrosslinks(): " +e.getMessage());
			e.printStackTrace();
		}
	}

	private static void addTCDBcrosslinks_map() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_cross_db " +
							"(cluster_id, cluster_threshold, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?,?)");
			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl WHERE cluster_id=? AND cluster_threshold=?");
			PreparedStatement ptm1 = CAMPS_CONNECTION.prepareStatement("SELECT cluster_id,sequenceid FROM clusters_mcl WHERE cluster_threshold=?");
			System.out.print("\n Getting clusters from tables \n");
			ArrayList<ClusterDs> clusters = new ArrayList<ClusterDs>();
			for(int j =5; j<= 100; j++){
				ptm1.setInt(1, j);
				ResultSet rs1 = ptm1.executeQuery();

				boolean check2 = false;

				ClusterDs temp = new ClusterDs();
				temp.clusterThreshold = j;

				while(rs1.next()){
					check2 = true;
					int clus = rs1.getInt(1);
					int sq = rs1.getInt(2);
					if(temp.list.containsKey(clus)){
						ArrayList<Integer> x = temp.list.get(clus);
						x.add(sq);
						temp.list.put(clus, x);
					}
					else{
						ArrayList<Integer> x = new ArrayList<Integer>();
						x.add(sq);
						temp.list.put(clus, x);
					}
				}
				rs1.close();
				// add temp back
				if (check2){
					check2 = false;
					clusters.add(temp);
				}
			}
			ptm1.close();

			//take only best TCDB hit 
			/*
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT distinct(sequences_other_database_sequenceid) " +
							"FROM other_database " +
							"WHERE sequenceid=? AND " +
							"db=\"tcdb\" AND " +
							"query_coverage>="+COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc LIMIT 1");
			 */
			PreparedStatement ptm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid, sequences_other_database_sequenceid " +
							"FROM other_database " +
							"WHERE " +
							"db=\"tcdb\" AND " +
							"query_coverage>="+COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc");
			System.out.print("\n Getting TCDB hits \n");
			HashMap <Integer, Integer> ptbl2 = new HashMap<Integer, Integer>(); // key is the sequenceid and value is sequences_other_database_sequenceid
			ResultSet rptm2 = ptm2.executeQuery();
			while(rptm2.next()){
				int Sid = rptm2.getInt("sequenceid");
				int tcdbSequenceid = rptm2.getInt("sequences_other_database_sequenceid");
				if(!ptbl2.containsKey(Sid)){
					ptbl2.put(Sid, tcdbSequenceid);
				}
			}
			rptm2.close();
			ptm2.close();

			//PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT name FROM sequences_other_database WHERE sequenceid=? AND db=\"tcdb\"");
			System.out.print("\n Getting names \n");
			HashMap <Integer, String> ptbl3 = new HashMap<Integer, String>(); // key is the seq_id and value is name
			PreparedStatement ptm3 = CAMPS_CONNECTION.prepareStatement("SELECT name,sequenceid FROM sequences_other_database WHERE db=\"tcdb\"");
			ResultSet rptm3 = ptm3.executeQuery();
			while(rptm3.next()){
				String name = rptm3.getString(1);
				int id = rptm3.getInt(2);
				if(!ptbl3.containsKey(id)){
					ptbl3.put(id, name);
				}
			}
			ptm3.close();
			rptm3.close();

			//PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT description FROM other_database_hierarchies WHERE key_=? AND db=\"tcdb\"");
			PreparedStatement ptm4 = CAMPS_CONNECTION.prepareStatement("SELECT description,key_ FROM other_database_hierarchies WHERE db=\"tcdb\"");
			System.out.print("\n Getting tcdb descriptions \n");
			ResultSet rptm4 = ptm4.executeQuery();
			HashMap <String, String> ptbl4 = new HashMap<String, String>(); // key is the key_ and value is description
			while(rptm4.next()){
				String d = rptm4.getString(1);
				String k = rptm4.getString(2);
				if(!ptbl4.containsKey(k)){
					ptbl4.put(k, d);
				}
			}
			ptm4.close();
			rptm4.close();


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold FROM clusters_mcl_nr_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");

				//extract cluster members
				ClusterDs t = new ClusterDs();
				for(int i =0; i<=clusters.size()-1;i++){
					if(clusters.get(i).clusterThreshold == clusterThreshold){
						t = clusters.get(i);
						break;
					}
				}
				ArrayList<Integer> sids = t.list.get(clusterID);
				t = null;
				int clusterSize = sids.size();

				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();
				//get tcdb data for each member
				for(int i=0;i<=sids.size()-1;i++) {
					int sequenceid = sids.get(i);
					int tcdbSequenceid = 0;
					if(ptbl2.containsKey(sequenceid)){
						tcdbSequenceid = ptbl2.get(sequenceid);
					}
					else{
						continue;
					}
					String tcdbCode = "";
					if(ptbl3.containsKey(tcdbSequenceid)){
						tcdbCode = ptbl3.get(tcdbSequenceid);
					}
					else{
						continue;
					}
					 

					String[] tmp = tcdbCode.split("\\.");
					String tcdbClassification = tmp[0]+"."+tmp[1]+"."+tmp[2];

					String tcdbDescription = "";
					tcdbDescription = ptbl4.get(tcdbClassification);

					String link = tcdbClassification+" - " +tcdbDescription;

					int count = 0;
					if(link2count.containsKey(link)) {
						count = link2count.get(link);
					}
					count = count+1;

					link2count.put(link, new Integer(count));

				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setInt(1, clusterID);
					pstm.setFloat(2, clusterThreshold);
					pstm.setString(3, "tcdb");
					pstm.setString(4, link);
					pstm.setInt(5, count);
					pstm.setDouble(6, perc);

					pstm.executeUpdate();
				}
				if (statusCount % 10000 == 0){
					System.out.print(statusCount+"\n");
				}
			}
			rs.close();
			stm.close();
			pstm.close();

		}catch(Exception e) {
			System.err.println("Exception in ClusterCrossDB.addTCDBcrosslinks(): " +e.getMessage());
			e.printStackTrace();
		}
	}
	private static void addTCDBcrosslinks() {
		try {

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_cross_db " +
							"(cluster_id, cluster_threshold, db, link_description, instances_abs, instances_rel) " +
							"VALUES " +
					"(?,?,?,?,?,?)");



			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl WHERE cluster_id=? AND cluster_threshold=?");

			//take only best TCDB hit
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT distinct(sequences_other_database_sequenceid) " +
							"FROM other_database " +
							"WHERE sequenceid=? AND " +
							"db=\"tcdb\" AND " +
							"query_coverage>="+COVERAGE_THRESHOLD+" AND "+
							"hit_coverage>="+COVERAGE_THRESHOLD+" "+
					"ORDER BY ident desc LIMIT 1");
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT name FROM sequences_other_database WHERE sequenceid=? AND db=\"tcdb\"");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT description FROM other_database_hierarchies WHERE key_=? AND db=\"tcdb\"");


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold FROM clusters_mcl_nr_info ORDER BY proteins");
			int statusCount = 0;
			while(rs.next()) {	//iterate over all initial clusters
				statusCount++;
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");

				//System.out.println("In progress " +clusterID+" " +clusterThreshold + " [" +statusCount+ "]");


				//extract cluster members
				BitSet members = new BitSet();
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					int sequenceID = rs1.getInt("sequenceid");
					members.set(sequenceID);
				}
				rs1.close();

				int clusterSize = members.cardinality();


				Hashtable<String,Integer> link2count = new Hashtable<String,Integer>();

				//get tcdb data for each member
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					pstm2.setInt(1, sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {

						int tcdbSequenceid = rs2.getInt("sequences_other_database_sequenceid");

						pstm3.setInt(1, tcdbSequenceid);
						ResultSet rs3 = pstm3.executeQuery();
						while(rs3.next()) {

							String tcdbCode = rs3.getString("name");
							String[] tmp = tcdbCode.split("\\.");
							String tcdbClassification = tmp[0]+"."+tmp[1]+"."+tmp[2];

							String tcdbDescription = "";
							pstm4.setString(1, tcdbClassification);
							ResultSet rs4 = pstm4.executeQuery();
							while(rs4.next()) {
								tcdbDescription = rs4.getString("description");
							}
							rs4.close();

							String link = tcdbClassification+" - " +tcdbDescription;

							int count = 0;
							if(link2count.containsKey(link)) {
								count = link2count.get(link);
							}
							count = count+1;

							link2count.put(link, new Integer(count));

						}
						rs3.close();
					}
					rs2.close();
				}


				//insert cross links
				Enumeration<String> en = link2count.keys();
				while(en.hasMoreElements()) {
					String link = en.nextElement();						

					int count = link2count.get(link).intValue();

					double perc = 100 * ((double) count/clusterSize);

					pstm.setInt(1, clusterID);
					pstm.setFloat(2, clusterThreshold);
					pstm.setString(3, "tcdb");
					pstm.setString(4, link);
					pstm.setInt(5, count);
					pstm.setDouble(6, perc);

					pstm.executeUpdate();
				}
				if (statusCount % 10000 == 0){
					System.out.print(statusCount+"\n");
				}
			}
			rs.close();
			stm.close();


			pstm.close();
			pstm1.close();
			pstm2.close();
			pstm3.close();
			pstm4.close();

		}catch(Exception e) {
			System.err.println("Exception in ClusterCrossDB.addTCDBcrosslinks(): " +e.getMessage());
			e.printStackTrace();
		}
	}


}
