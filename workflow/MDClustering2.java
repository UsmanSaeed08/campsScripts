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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import utils.BitMatrix;
import utils.DBAdaptor;

public class MDClustering2 {

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	private static final Connection CAMPS_CONNECTION_forMclData = DBAdaptor.getConnection("camps4");
	private static final Connection CAMPS_CONNECTION_Insert = DBAdaptor.getConnection("camps4");

	private static final double MIN_PERCENTAGE_IDENTITY = 30;

	private static final double MIN_PERCENTAGE_OCCURRENCES = 70;

	//cluster id of SC-cluster for which MD-clusters should be calculated
	private Integer clusterID;			
	//cluster threshold of SC-cluster for which MD-clusters should be calculated
	private Float clusterThreshold;
	private static int runNumber =0;
	//matrix containing information whether pair of sequences share at least 30% sequence identity
	//private BitMatrix matrix;	
	//for mcl track information
	private static HashMap <String, ArrayList<Protein>> ptbl1 = new HashMap<String, ArrayList<Protein>>();// mcl trackStuff
	private static HashMap <String, Integer> ptbl2 = new HashMap<String, Integer>();// number of sequences in given cluster.. mcl_info
	private static HashMap <String, ArrayList<Integer>> seqIds = new HashMap<String, ArrayList<Integer>>();// for every cluster key the sequences in them
	private static Hashtable<Integer, Dictionary2> dict = new Hashtable<Integer, Dictionary2>(); // alignments table values
	private static HashMap <String,Integer> sc = new HashMap<String,Integer>();

	private static Hashtable<Integer, Integer> SequenceIdsInSC = new Hashtable<Integer, Integer>();
	//mapping between sequence ids and indices in matrix
	//private int[] indizes;
	private static ArrayList<Integer> Members_afterThresh;
	private static ArrayList<Integer> AllMembers = new ArrayList<Integer>();
	private static BufferedWriter bw;

	public MDClustering2(int clusterID, float clusterThreshold) {
		this.clusterID = clusterID;
		this.clusterThreshold = clusterThreshold;
	}

	private static void populate_mclTrack(){
		try{
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT cluster_id,cluster_threshold,child_cluster_id, child_cluster_threshold, intersection_size FROM clusters_mcl_track");
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()) {
				int childClusterID = rs1.getInt("child_cluster_id");
				float childClusterThreshold = rs1.getFloat("child_cluster_threshold");
				int intersectionSize = rs1.getInt("intersection_size");
				int clusid = rs1.getInt("cluster_id");
				float thresh = rs1.getFloat("cluster_threshold");
				String key = Integer.toString(clusid)+Float.toString(thresh);
				if (ptbl1.containsKey(key)){
					// get array List and add element
					ArrayList<Protein> p = ptbl1.get(key);
					Protein temp = new Protein();
					temp.setforTrack(childClusterID, childClusterThreshold,intersectionSize);
					p.add(temp);
					ptbl1.put(key, p);
				}
				else{
					// simply add the element
					ArrayList<Protein> p =  new ArrayList<Protein>();
					Protein temp = new Protein();
					temp.setforTrack(childClusterID, childClusterThreshold,intersectionSize);
					p.add(temp);
					ptbl1.put(key, p);
				}				
			}
			rs1.close();
			pstm1.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void populate_mclTrackForTest(int clusid,float thresh){
		try{
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT child_cluster_id, child_cluster_threshold, intersection_size FROM clusters_mcl_track where cluster_id=? and cluster_threshold=?");
			pstm1.setInt(1, clusid);
			pstm1.setFloat(2, thresh);
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()) {
				int childClusterID = rs1.getInt("child_cluster_id");
				float childClusterThreshold = rs1.getFloat("child_cluster_threshold");
				int intersectionSize = rs1.getInt("intersection_size");
				//int clusid = rs1.getInt("cluster_id");
				//float thresh = rs1.getFloat("cluster_threshold");
				String key = Integer.toString(clusid)+Float.toString(thresh);
				if (ptbl1.containsKey(key)){
					// get array List and add element
					ArrayList<Protein> p = ptbl1.get(key);
					Protein temp = new Protein();
					temp.setforTrack(childClusterID, childClusterThreshold,intersectionSize);
					p.add(temp);
					ptbl1.put(key, p);
				}
				else{
					// simply add the element
					ArrayList<Protein> p =  new ArrayList<Protein>();
					Protein temp = new Protein();
					temp.setforTrack(childClusterID, childClusterThreshold,intersectionSize);
					p.add(temp);
					ptbl1.put(key, p);
				}				
			}
			rs1.close();
			pstm1.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void populate_alignments(){
		try{	
			// make a hashmap with key as seqid_query and value is seqid_hit and identity value.
			// Can use the Dictionary class, as it is exactly what is required.
			PreparedStatement pstm = null;
			int idx =1;
			for (int i=1; i<=17;i++){				
				pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial limit "+idx+","+10000000);
				//pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial");
				idx = i*10000000;
				System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
				ResultSet rs = pstm.executeQuery();
				while(rs.next()) {
					int idhit = rs.getInt("seqid_hit");
					int query = rs.getInt ("seqid_query");
					float identity = rs.getFloat("identity");
					if(identity >= MIN_PERCENTAGE_IDENTITY){
						if(SequenceIdsInSC.containsKey(idhit) || SequenceIdsInSC.containsKey(query)){ // to get only the sequences in camps sc.. and improve memory allocation

							if (dict.containsKey(query)){ // the key already exists...so that you dont delete the previous info of the key
								Dictionary2 temp = (Dictionary2)dict.get(query);
								temp.set(idhit, identity);
								dict.put(query,temp);
							}
							else if (dict.containsKey(idhit)){
								Dictionary2 temp = (Dictionary2)dict.get(idhit);
								temp.set(query, identity);
								dict.put(idhit,temp);
							}
							else{
								Dictionary2 temp = new Dictionary2(idhit, identity);
								dict.put(query,temp);

								//Dictionary temp1 = new Dictionary(query, identity);
								//dict.put(idhit,temp);
							}
						}
					}
				}
				// dict population complete
				rs.close();
			}
			pstm.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	/*
	private static void populate_alignmentsForTestfromFile(int cid, float tesh){
		try{	
			// make a hashmap with key as seqid_query and value is seqid_hit and identity value.
			// Can use the Dictionary class, as it is exactly what is required.
			BufferedReader bw = new BufferedReader(new FileReader(new File("F:/SC_Clust_postHmm/MDClustIssue/Test1")));

			HashMap <Integer, Integer> allseq = new HashMap<Integer, Integer>();// 
			String k = Integer.toString(cid)+Float.toString(tesh);

			if(seqIds.containsKey(k)){
				AllMembers = seqIds.get(k);
			}
			for (int i =0;i<=AllMembers.size()-1;i++){
				allseq.put(AllMembers.get(i), null);
			}
			String line="";
			while((line = bw.readLine())!=null){				
				String x[] =line.split("\t");

				int query = Integer.parseInt(x[0].trim());
				int idhit = Integer.parseInt(x[1].trim());
				float identity = Float.parseFloat(x[2]);

				if (allseq.containsKey(idhit)&&allseq.containsKey(query)){

					if (dict.containsKey(query)){ // the key already exists...so that you dont delete the previous info of the key
						Dictionary temp = (Dictionary)dict.get(query);
						temp.set(idhit, identity);
						dict.put(query,temp);
					}
					else if (dict.containsKey(idhit)){
						Dictionary temp = (Dictionary)dict.get(idhit);
						temp.set(query, identity);
						dict.put(idhit,temp);
					}
					else{
						Dictionary temp = new Dictionary(idhit, identity);
						dict.put(query,temp);
					}
				}
			}
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	 *//*
	private static void populate_alignmentsForTest(int cid, float tesh){
		try{	
			// make a hashmap with key as seqid_query and value is seqid_hit and identity value.
			// Can use the Dictionary class, as it is exactly what is required.
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/SC_Clust_postHmm/MDClustIssue/Test1")));
			PreparedStatement pstm = null;
			int idx =1;

			HashMap <Integer, Integer> allseq = new HashMap<Integer, Integer>();// 
			String k = Integer.toString(cid)+Float.toString(tesh);

			if(seqIds.containsKey(k)){
				AllMembers = seqIds.get(k);
			}
			for (int i =0;i<=AllMembers.size()-1;i++){
				allseq.put(AllMembers.get(i), null);
			}
			for (int i=1; i<=17;i++){				
				pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial limit "+idx+","+10000000);
				//pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial");
				idx = i*10000000;
				System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
				ResultSet rs = pstm.executeQuery();
				while(rs.next()) {
					int idhit = rs.getInt("seqid_hit");
					int query = rs.getInt ("seqid_query");
					float identity = rs.getFloat("identity");

					if (allseq.containsKey(idhit)&&allseq.containsKey(query)){
						bw.write(query+"\t"+idhit+"\t"+identity+"\n");

						if (dict.containsKey(query)){ // the key already exists...so that you dont delete the previous info of the key
							Dictionary temp = (Dictionary)dict.get(query);
							temp.set(idhit, identity);
							dict.put(query,temp);
						}
						else if (dict.containsKey(idhit)){
							Dictionary temp = (Dictionary)dict.get(idhit);
							temp.set(query, identity);
							dict.put(idhit,temp);
						}
						else{
							Dictionary temp = new Dictionary(idhit, identity);
							dict.put(query,temp);
						}
					}
				}

				// dict population complete
				rs.close();
			}
			bw.close();
			pstm.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	  */
	private static void populate_mcl_info(){
		try{

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT cluster_id,cluster_threshold,sequences FROM clusters_mcl_nr_info");

			ResultSet rs2 = pstm2.executeQuery();

			while(rs2.next()) {
				//int numberMembers = rs2.getInt("sequences");
				int clusid = rs2.getInt("cluster_id");
				float thresh = rs2.getFloat("cluster_threshold");
				String key = Integer.toString(clusid)+Float.toString(thresh);
				int numberMembers = seqIds.get(key).size();
				if (!ptbl2.containsKey(key)){
					ptbl2.put(key, numberMembers);
				}
			}

			rs2.close();
			pstm2.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void populate_mcl_infoForTest(int clusid,float thresh){
		try{
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT sequences FROM clusters_mcl_nr_info where cluster_id =? and cluster_threshold=?");
			pstm2.setInt(1, clusid);
			pstm2.setFloat(2, thresh);

			ResultSet rs2 = pstm2.executeQuery();

			while(rs2.next()) {
				//int numberMembers = rs2.getInt("sequences");
				//int clusid = rs2.getInt("cluster_id");
				//float thresh = rs2.getFloat("cluster_threshold");
				String key = Integer.toString(clusid)+Float.toString(thresh);
				int numberMembers = seqIds.get(key).size();
				if (!ptbl2.containsKey(key)){
					ptbl2.put(key, numberMembers);
				}
			}
			rs2.close();
			pstm2.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	public void run() {
		try {		

			//int batchSize = 50;	
			int batchSize = 1;
			runNumber++;
			System.out.print("\nRun Number - SC-Cluster Number " + runNumber+"\n");


			PreparedStatement pstm3 = CAMPS_CONNECTION_Insert.prepareStatement(
					"INSERT INTO cp_clusters " +
							"(cluster_id, cluster_threshold, super_cluster_id, super_cluster_threshold, type) " +
							"VALUES " +
					"(?,?,?,?,?)");


			int countMDCluster = 0;			
			int batchCounter = 0;

			String cluster = clusterID + "#" + clusterThreshold;

			//System.out.println("In progress: " +cluster);

			//
			//store information whether pairs of sequences have identity>=MIN_PERCENTAGE_IDENTITY
			//in triangle bitset matrix
			//
			//ArrayList<Integer> Members_afterThresh = setMatrix(clusterID, clusterThreshold);

			ArrayList<String> clusters = new ArrayList<String>();
			clusters.add(cluster);

			while(!clusters.isEmpty()) {
				String cluster2Test = clusters.get(0);

				int clusterID = Integer.parseInt(cluster2Test.split("#")[0]);
				float clusterThreshold = Float.parseFloat(cluster2Test.split("#")[1]);

				//setMatrix(clusterID, clusterThreshold); // sets the Members_afterThresh;

				int validMembers= setMatrix2(clusterID, clusterThreshold); // sets the Members_afterThresh;
				// and  AllMembers

				//if(isMDCluster2(cluster2Test)) {
				if(isMDCluster2_2(cluster2Test,validMembers)) {	// for setMatrix2

					countMDCluster++;

					Integer mdClusterID = Integer.parseInt(cluster2Test.split("#")[0]);
					Float mdClusterThreshold = Float.parseFloat(cluster2Test.split("#")[1]);

					System.out.println("\tMD cluster: "+cluster2Test);

					//write to db
					// COMMENTING OUT WRITE TO DB PART TO CHECK NUMBER OF MD CLUSTERS


					//pstm.set ...
					batchCounter++;

					String k = this.clusterID.toString()+"_"+this.clusterThreshold.toString();
					if (sc.containsKey(k)){
						sc.remove(k);
					}
					System.out.println(mdClusterID+"\t"+mdClusterThreshold+"\t"+this.clusterID+"\t"+this.clusterThreshold+"\t");
					bw.write(mdClusterID+"\t"+mdClusterThreshold+"\t"+this.clusterID+"\t"+this.clusterThreshold);
					bw.newLine();
					// *****************************************************************************
					/*
					pstm3.setInt(1, mdClusterID);
					pstm3.setFloat(2, mdClusterThreshold);
					pstm3.setInt(3, this.clusterID);
					pstm3.setFloat(4, this.clusterThreshold);
					pstm3.setString(5, "md_cluster");

					pstm3.addBatch();

					if(batchCounter % batchSize == 0) {								
						pstm3.executeBatch();
						pstm3.clearBatch();
						System.out.println(mdClusterID+"\t"+mdClusterThreshold+"\t"+this.clusterID+"\t"+this.clusterThreshold+"\t");
					}
					 */

					clusters.remove(0);
				}
				else {

					clusters.remove(0);
					int clusterID2 = Integer.parseInt(cluster2Test.split("#")[0]);
					float clusterThreshold2 = Float.parseFloat(cluster2Test.split("#")[1]);

					String key = Integer.toString(clusterID2)+Float.toString(clusterThreshold2);
					if(ptbl1.containsKey(key)){
						ArrayList<Protein> p = ptbl1.get(key);
						for(int i =0;i<=p.size()-1;i++){
							int childClusterID = p.get(i).child_clusterid;
							float childClusterThreshold = p.get(i).child_clus_thresh;
							int intersectionSize = p.get(i).intersectionsSz;

							int numberMembers = 0;

							String keyChild = Integer.toString(childClusterID)+Float.toString(childClusterThreshold);
							if(ptbl2.containsKey(keyChild)){
								numberMembers = ptbl2.get(keyChild);
							}

							//							//ignore singletons
							//							if(numberMembers == 1) {
							//								continue;
							//							}

							double percCoverage = 100 * ((double) intersectionSize/numberMembers);

							if(percCoverage >= 20) {							
								String childCluster = childClusterID + "#" + childClusterThreshold;
								clusters.add(childCluster);
								//System.out.println("\t\t### "+childClusterID+"#"+childClusterThreshold+"\t"+percCoverage);
							}
						}
					}
				}
			}			
			System.out.println("\n\nNumber of MD clusters: " +countMDCluster);


		} catch(Exception e) {
			System.err.println("Exception in MDClustering2.run(): " +e.getMessage());
			e.printStackTrace();

		} finally {
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			if (CAMPS_CONNECTION_forMclData != null) {
				try {
					CAMPS_CONNECTION_forMclData.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}

		}
	}
	private static void populate_listofSeqidsForTest(int clusid,float thresh){ // used in set matrix
		try{
			//tatement stm1 = CAMPS_CONNECTION.createStatement();
			//ResultSet rs1 = stm1.executeQuery("SELECT sequenceid,cluster_id,cluster_threshold FROM clusters_mcl");

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl where cluster_id=? and cluster_threshold=?");
			pstm1.setInt(1, clusid);
			pstm1.setFloat(2, thresh);
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()) {

				int sequenceid = rs1.getInt("sequenceid");
				//int clusid = rs1.getInt("cluster_id");
				//float thresh = rs1.getFloat("cluster_threshold");
				String key = Integer.toString(clusid)+Float.toString(thresh);
				//members.set(sequenceid);
				if(seqIds.containsKey(key)){
					ArrayList<Integer> x = seqIds.get(key);
					x.add(sequenceid);
					seqIds.put(key, x);
				}
				else{
					ArrayList<Integer> x = new ArrayList<Integer>();
					x.add(sequenceid);
					seqIds.put(key, x);
				}
			}
			rs1.close();
			rs1 = null;
			pstm1.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void populate_listofSeqids(){ // used in set matrix
		try{

			Statement stm1 = CAMPS_CONNECTION.createStatement();
			ResultSet rs1 = stm1.executeQuery("SELECT cluster_id,cluster_threshold,sequenceid FROM clusters_mcl");
			while(rs1.next()) {

				int sequenceid = rs1.getInt("sequenceid");
				int clusid = rs1.getInt("cluster_id");
				float thresh = rs1.getFloat("cluster_threshold");
				String key = Integer.toString(clusid)+Float.toString(thresh);
				//members.set(sequenceid);
				if(seqIds.containsKey(key)){
					ArrayList<Integer> x = seqIds.get(key);
					x.add(sequenceid);
					seqIds.put(key, x);
				}
				else{
					ArrayList<Integer> x = new ArrayList<Integer>();
					x.add(sequenceid);
					seqIds.put(key, x);
				}
				if(!SequenceIdsInSC.containsKey(sequenceid)){
					SequenceIdsInSC.put(sequenceid, 0);
				}
			}
			rs1.close();
			rs1 = null;
			stm1.close();
			stm1 = null;
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	/*
	private void setMatrix(int clusterID, float clusterThreshold) {
		Members_afterThresh = new ArrayList<Integer>(); //identity >= MIN_PERCENTAGE_IDENTITY
		try {
			// so only make sub-table for those sequences which have less 
			// than the threshold identity etc
			// Get the commplete alignment table too - done, in dict

			// get members 
			String key = Integer.toString(clusterID)+Float.toString(clusterThreshold);
			AllMembers = new ArrayList<Integer>();

			if(seqIds.containsKey(key)){
				AllMembers = seqIds.get(key);
			}
			else{
				System.err.print("Critical error, No members for "+clusterID+" at thresh " +
						+clusterThreshold+"\n Exiting ");
				System.exit(0);
			}
			// so the set of loops below is to get the members which have 
			// identity of atleast minimum percentage identity
			// for all the members, get their alignment scores
			// if any score is between this member and any other member in this cluster
			// then see if its identity is above the required 
			// then if true, add in the arraylist
			for (int i =0; i<=AllMembers.size()-1;i++){
				int query = AllMembers.get(i);
				if (dict.containsKey(query)){
					Dictionary temp = dict.get(query);
					for(int j =1;j<=AllMembers.size()-1;j++){
						int hit = AllMembers.get(j);
						if(temp.protein.contains(hit)){
							int idx = temp.protein.indexOf(hit);
							float identity = temp.score.get(idx);
							if(identity >= MIN_PERCENTAGE_IDENTITY) {
								if (!Members_afterThresh.contains(query) && !Members_afterThresh.contains(hit)){// adding the new and part
									Members_afterThresh.add(query);
									//added part
									Members_afterThresh.add(hit);
								}
								//if (!Members_afterThresh.contains(hit)){
								//	Members_afterThresh.add(hit);
								//}

							}
						}
					}
				}
			}	
		}catch(Exception e) {
			System.err.println("Exception in MDClustering2.createMatrix(): " +e.getMessage());
			e.printStackTrace();

		}
	}*/
	private int setMatrix2(int clusterID, float clusterThreshold) {
		//Members_afterThresh = new ArrayList<Integer>(); //identity >= MIN_PERCENTAGE_IDENTITY
		int validPairs = 0;
		try {
			// so only make sub-table for those sequences which have less 
			// than the threshold identity etc
			// Get the commplete alignment table too - done, in dict

			// get members 
			String key = Integer.toString(clusterID)+Float.toString(clusterThreshold);
			AllMembers = new ArrayList<Integer>();

			if(seqIds.containsKey(key)){
				AllMembers = seqIds.get(key);
			}
			else{
				System.err.print("Critical error, No members for "+clusterID+" at thresh " +
						+clusterThreshold+"\n Exiting ");
				System.exit(0);
			}
			// so the set of loops below is to get the members which have 
			// identity of atleast minimum percentage identity
			// for all the members, get their alignment scores
			// if any score is between this member and any other member in this cluster
			// then see if its identity is above the required 
			// then if true, add in the arraylist

			int numMembers =  AllMembers.size(); 
			int threshold = (int) Math.round(((numMembers-1) * MIN_PERCENTAGE_OCCURRENCES)/((double) 100));

			for (int i =0; i<=AllMembers.size()-1;i++){
				int Pairs = 0;

				int query = AllMembers.get(i);
				if (dict.containsKey(query)){
					Dictionary2 temp = dict.get(query);
					for(int j =0;j<=AllMembers.size()-1;j++){
						int hit = AllMembers.get(j);
						if(temp.protein.containsKey(hit)){
							if(query != hit){
								//int idx = temp.protein.indexOf(hit);
								//float identity = temp.score.get(idx);
								Pairs ++;
								//if(identity >= MIN_PERCENTAGE_IDENTITY) {
								/*if (!Members_afterThresh.contains(query) && !Members_afterThresh.contains(hit)){// adding the new and part
										Members_afterThresh.add(query);
										//added part
										Members_afterThresh.add(hit);
									}*/
								//if (!Members_afterThresh.contains(hit)){
								//	Members_afterThresh.add(hit);
								//}

								//}
							}
						}
					}
					// increment valid pairs
					if(Pairs>=threshold ){ //set threshold here
						validPairs++;
					}
				}
			}	
			return validPairs;
		}catch(Exception e) {
			System.err.println("Exception in MDClustering2.createMatrix(): " +e.getMessage());
			e.printStackTrace();
		}
		return validPairs;
	}
	private boolean isMDCluster2_2(String cluster,int validPairs) {
		boolean isMDCluster = false;
		// it was already ensured that only those members are included in
		// ? which have identity of at least 30%
		// this function ensures if at least 70% of members are attained
		//Members_afterThresh;

		int numMembers =  AllMembers.size(); 
		int threshold = (int) Math.round(((numMembers-1) * MIN_PERCENTAGE_OCCURRENCES)/((double) 100));
		if (validPairs >= threshold){
			isMDCluster = true;
		}
		return isMDCluster;
	}

	/*
	private boolean isMDCluster(String cluster) {
		boolean isMDCluster = false;

		try {

			int clusterID = Integer.parseInt(cluster.split("#")[0]);
			float clusterThreshold = Float.parseFloat(cluster.split("#")[1]);

			BitSet sequenceIDs = new BitSet();

			//get cluster members
			Statement stm = CAMPS_CONNECTION_forMclData.createStatement();
			ResultSet rs = stm.executeQuery("SELECT sequenceid FROM clusters_mcl WHERE cluster_id="+clusterID+" and cluster_threshold="+clusterThreshold);
			while(rs.next()) {
				int sequenceID = rs.getInt("sequenceid");
				sequenceIDs.set(sequenceID);
			}
			rs.close();
			rs = null;
			stm.close();

			int numMembers = sequenceIDs.cardinality();

			int numValidMembers = 0;
			//double threshold = (numMembers * MIN_PERCENTAGE_OCCURRENCES)/((double) 100); //!!! Check: isn't (numMembers-1) correct here?!
			int threshold = (int) Math.round(((numMembers-1) * MIN_PERCENTAGE_OCCURRENCES)/((double) 100)); 
			int counter = 0;

			for(int sequenceID1 = sequenceIDs.nextSetBit(0); sequenceID1>=0; sequenceID1 = sequenceIDs.nextSetBit(sequenceID1+1)) {
				counter++;

				//counter ++;
				if (counter % 100 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (counter % 10000 == 0) {
					System.out.write('\n');
					System.out.flush();
				}

				int countValidPairs = 0;


				for(int sequenceID2 = sequenceIDs.nextSetBit(0); sequenceID2>=0; sequenceID2 = sequenceIDs.nextSetBit(sequenceID2+1)) {


					int rowIndex;
					int colIndex;

					if(sequenceID1 == sequenceID2) {
						continue;
					}
					else if(sequenceID1 < sequenceID2) {

						rowIndex = this.indizes[sequenceID1];
						colIndex = this.indizes[sequenceID2];
					}
					else {

						rowIndex = this.indizes[sequenceID2];
						colIndex = this.indizes[sequenceID1];
					}

					if(this.matrix.get(rowIndex,colIndex)) {
						countValidPairs++;
					}
				}




				if(countValidPairs >= threshold) {
					numValidMembers++;
				}								
			}


			if(numMembers == numValidMembers) {
				isMDCluster = true;				
			}
			else {
				System.out.println("\t\tIgnore: " +cluster+"\t"+numMembers +"\t"+numValidMembers);
			}


		} catch(Exception e) {
			e.printStackTrace();
		}

		return isMDCluster;

	}
	 */

	/*
	 * Checks if error files from running MD-clustering jobs are o.k.
	 */
	public static void checkJobErrorFiles(String dir) {

		try {

			Pattern p = Pattern.compile("mdc2\\_\\d+\\.e\\d+");

			File[] files = new File(dir).listFiles();

			int countFiles = 0;
			int countCorrectFiles = 0;

			for(File file: files) {

				Matcher m = p.matcher(file.getName());

				if(!m.matches()) {
					continue;
				}

				countFiles++;
				boolean isCorrectFile = true;

				System.out.println("Checking " +file.getName()+" ...");

				BufferedReader br = new BufferedReader(new FileReader(file));
				String line;
				while((line=br.readLine()) != null) {

					line = line.trim();

					if(line.contains("In progress:")) {
						continue;
					}
					else if(line.contains("MD cluster:")) {
						continue;
					}
					else if(line.contains("###")) {
						continue;
					}
					else if(line.contains("Ignore")) {
						continue;
					}
					else if(line.replaceAll("\\.", "").isEmpty()) {
						continue;
					}
					else {
						isCorrectFile = false;
						System.out.println(line);
					}
				}
				br.close();

				if(isCorrectFile) {
					countCorrectFiles++;
				}
			}


			System.out.println();
			System.out.println("Number of files: " +countFiles);
			System.out.println("Number of correct files: " +countCorrectFiles);

		}catch(Exception e) {
			e.printStackTrace();
		}

	}


	/*
	 * Checks if output files from running MD-clustering jobs are o.k.
	 */
	public static void checkJobOutputFiles(String dir) {

		try {

			Pattern p = Pattern.compile("mdc2\\_\\d+\\.o\\d+");

			Pattern pStart = Pattern.compile("...\\[\\d+:\\d+:\\d+\\]\\s+Start");
			Pattern pDone = Pattern.compile("...DONE\\s+\\[\\d+:\\d+:\\d+\\]");
			Pattern pMD = Pattern.compile("Number of MD clusters:\\s+(\\d+)");

			File[] files = new File(dir).listFiles();

			int countFiles = 0;
			int countCorrectFiles = 0;
			boolean foundStart = false;
			boolean foundEnd = false;

			int countAllMD = 0;

			for(File file: files) {

				Matcher m = p.matcher(file.getName());

				if(!m.matches()) {
					continue;
				}

				countFiles++;


				BufferedReader br = new BufferedReader(new FileReader(file));
				String line;
				while((line=br.readLine()) != null) {

					line = line.trim();

					Matcher m1 = pStart.matcher(line);
					Matcher m2 = pDone.matcher(line);
					Matcher m3 = pMD.matcher(line);

					if(m1.matches()) {
						foundStart = true;
					}					
					else if(m2.matches()) {
						foundEnd = true;
					}					
					else if(m3.matches()) {
						int countMD = Integer.parseInt(m3.group(1));
						countAllMD += countMD;
					}
				}
				br.close();

				if(foundStart && foundEnd) {
					countCorrectFiles++;
				}
			}


			System.out.println();
			System.out.println("Number of files: " +countFiles);
			System.out.println("Number of correct files: " +countCorrectFiles);
			System.out.println("Number of MD-clusters: " +countAllMD);

		}catch(Exception e) {
			e.printStackTrace();
		}

	}

	public static void testMDClust(){
		//So the issue is that getting all SC clusters as MD at 30 identity 
		// and none at 40 identity
		// taking one test cluster
		//see the identity scores
		try{
			HashMap <Integer, Integer> allseq = new HashMap<Integer, Integer>();// 
			//populate_alignments();
			//populate_listofSeqids();

			int clusterID = 14231;
			float clusterThreshold = 80;
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl WHERE cluster_id=? and cluster_threshold=?");
			pstm1.setInt(1, clusterID);
			pstm1.setFloat(2, clusterThreshold);
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()) {
				int sequenceid = rs1.getInt("sequenceid");
				AllMembers.add(sequenceid);
				allseq.put(sequenceid, null);
			}
			rs1.close();
			rs1 = null;			


			int idx =1;
			PreparedStatement pstm;
			for (int i=1; i<=17;i++){				
				pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial limit "+idx+","+10000000);
				//pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial");
				idx = i*10000000;
				System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
				ResultSet rs = pstm.executeQuery();
				while(rs.next()) {
					int idhit = rs.getInt("seqid_hit");
					int query = rs.getInt ("seqid_query");
					float identity = rs.getFloat("identity");
					if (allseq.containsKey(idhit)&&allseq.containsKey(query)){
						System.out.print(identity+"\n");
					}
				}
			}


			MDClustering2 md = new MDClustering2(clusterID,clusterThreshold);
			//md.setMatrix(clusterID,clusterThreshold);
			boolean isMDCluster = false;
			// it was already ensured that only those members are included in
			// ? which have identity of at least 30%
			// this function ensures if at least 70% of members are attained
			//Members_afterThresh;

			int numMembers =  AllMembers.size(); 
			int threshold = (int) Math.round(((numMembers-1) * MIN_PERCENTAGE_OCCURRENCES)/((double) 100));
			if (Members_afterThresh.size() >= threshold){
				isMDCluster = true;
			}
			//System.out.print(isMDCluster);


		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void InsertInDb(String filePath){
		try{
			System.out.print(filePath);
			int batchSize = 0;
			int batchCounter = 100;
			PreparedStatement pstm3 = CAMPS_CONNECTION_Insert.prepareStatement(
					"INSERT INTO cp_clusters " +
							"(cluster_id, cluster_threshold, super_cluster_id, super_cluster_threshold, type) " +
							"VALUES " +
					"(?,?,?,?,?)");
			BufferedReader br = new BufferedReader(new FileReader(new File(filePath)));
			String line = "";
			while((line = br.readLine())!=null){
				batchSize ++;
				String[] p = line.split("\t");
				int mdId = Integer.parseInt(p[0].trim());
				float mdThresh = Float.parseFloat(p[1].trim());
				int parentId = Integer.parseInt(p[2].trim());
				float parentThresh = Float.parseFloat(p[3].trim());
				
				System.out.println(mdId+"\t"+mdThresh+"\t"+parentId+"\t"+parentThresh+"\t"+batchSize);
				
				//System.out.println(line+"\t"+batchSize);
				pstm3.setInt(1, mdId);
				pstm3.setFloat(2, mdThresh);
				pstm3.setInt(3, parentId);
				pstm3.setFloat(4, parentThresh);
				pstm3.setString(5, "md_cluster");
				pstm3.addBatch();

				if(batchCounter % batchSize == 0) {								
					pstm3.executeBatch();
					pstm3.clearBatch();
				}
			}
			pstm3.executeBatch();
			pstm3.clearBatch();
			pstm3.close();
			
			br.close();			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {

		try {
			
			/*
			 * This code was used to make the MD clusters with value 23580. i.e. the Correct one.
			 * Since I had computed it already and the results were written in a file, I am now just writing a code to
			 * read that file and put all the md cluster into cp_clusters. Function: InsertInDb(String );
			 * For this purpose, using the function below
			 * and exiting
			 * However, I just found out that now the max md size is only 4.. which is just not right
			 * there should be clusters with more size than that... so have to fix the md cluster extraction again.. 
			 * See it in detail whats going on
			 * But it is also probably because of the cluster composition... it is I think cuz of intersection size.. 
			 * For now.. have to re evaluate it.. and then update the cp clusters again... also re calculate tm cores and blocks for md
			 * Right now the tm cores and blocks of md are only the ones from first run of md clusters i.e. ~2k mds
			 * 
			 * 
			 * 
			 * SO I THINK I FOUND THE PROBLEM THAT WHY MAX SIZE OF SC CLUSTERS IS 4... IT IS PROBABLY
			 * BECAUSE OF IDENTITY, AS THE MIN IDENTITY IS NEVER CHECKED MOREOVER, THE PERCENTAGE OCCURANCE
			 * I.E. X NUMBER OF MEMBERS HAVE A CERTAIN MIN IDENTITY IS CHECKED TWICE. ONCE COUNTING
			 * VALID PAIRS AND ONCE COUNTING ISMDCLUSTER. WHEREAS IDENTITY ITSELF IS NOT CHECKED. 
			 * 
			 */
			/*
			System.out.println("Running...");
			InsertInDb("F:/MDClusterReport5.txt");
			System.out.println("Done");
			System.exit(0);
			*/

			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );

			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			//checkparentsinCamps3();

			//testMDClust();
			//System.exit(0);

			// ********************************************* MY TEST ********************************************

			//int clusterID = 14231;
			//float clusterThreshold = 80f;
			populate_listofSeqids();
			//populate_listofSeqidsForTest(clusterID,clusterThreshold);
			populate_mclTrack();
			//populate_mclTrackForTest(clusterID,clusterThreshold);
			populate_mcl_info();
			//populate_mcl_infoForTest(clusterID,clusterThreshold);
			populate_alignments();
			//populate_alignmentsForTest(clusterID,clusterThreshold);
			//populate_alignmentsForTestfromFile(clusterID,clusterThreshold);

			//MDClustering2 md = new MDClustering2(clusterID,clusterThreshold);
			//md.run();
			// have to run it for all the SC clusters.


			//Pattern p1 = Pattern.compile("cluster_(\\d+\\.\\d+)_(\\d+)\\.hmm");
			//String metaModelDirectory = "F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/HMMs/CAMPS4_1/";
			//String metaModelDirectory = "/home/proj/check/RunMetaModel_gef/HMMs/CAMPS4_1/";
			///home/proj/check/MDClustering
			//String metaModelDirectory = "/home/proj/check/MDClustering/"; // for test 50 files

			// initialize bw to write details of md clusters
			bw = new BufferedWriter(new FileWriter (new File("/home/users/saeed/MDClusterReport5.txt")));
			//File[] files = new File(metaModelDirectory).listFiles();
			// ****************** get the sc cluster hash table to compute how many sc clusters yield mds

			ArrayList<Integer> cluids = new ArrayList<Integer>();
			ArrayList<Float> threshs = new ArrayList<Float>();

			PreparedStatement p = CAMPS_CONNECTION.prepareStatement("SELECT cluster_id,cluster_threshold FROM cp_clusters where type=\"sc_cluster\"");
			ResultSet rs1 = p.executeQuery();
			while(rs1.next()) {
				Integer clusterID = rs1.getInt(1);
				Float clusterThreshold = rs1.getFloat(2);
				sc.put(clusterID.toString()+"_"+clusterThreshold.toString(), 0);
				cluids.add(clusterID);
				threshs.add(clusterThreshold);
			} 
			rs1.close();
			p.close();

			for(int i = 0;i<=cluids.size()-1;i++){
				int clusterID = cluids.get(i);
				float clusterThreshold = threshs.get(i);
				MDClustering2 md = new MDClustering2(clusterID,clusterThreshold);
				md.run();
			}

			bw.close();

			//checkJobErrorFiles("/home/proj/Camps3/log/mdClustering/partBasedOnV2/");

			//checkJobOutputFiles("/home/proj/Camps3/log/mdClustering/partBasedOnV2/");

			System.out.println("Number of SC clusters not producing any MD are: "+ sc.size());
			System.out.println("Sc clusters not prducing any MD are given below: ");
			for(int i =0;i<=cluids.size()-1;i++){
				Integer clusterID = cluids.get(i);
				Float clusterThreshold = threshs.get(i);
				String k = clusterID.toString()+"_"+clusterThreshold.toString();
				if(sc.containsKey(k)){
					System.out.println(k);
				}
			}

			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");



		} catch(Exception e) {
			e.printStackTrace();
		}	

	}

	private static void checkparentsinCamps3() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement p = CAMPS_CONNECTION.prepareStatement("SELECT cluster_id,cluster_threshold FROM cp_clusters where type=\"sc_cluster\"");
			ResultSet rs1 = p.executeQuery();
			while(rs1.next()) {
				Integer clusterID = rs1.getInt(1);
				Float clusterThreshold = rs1.getFloat(2);
				sc.put(clusterID.toString()+"_"+clusterThreshold.toString(), 0);
			}
			rs1.close();
			p.close();

			int count = 0;
			PreparedStatement pMD = CAMPS_CONNECTION.prepareStatement("SELECT cluster_id,cluster_threshold FROM cp_clusters where type=\"md_cluster\"");
			ResultSet rsMD = pMD.executeQuery();
			while(rsMD.next()) {
				Integer clusterID = rsMD.getInt(1);
				Float clusterThreshold = rsMD.getFloat(2);
				String k = clusterID.toString()+"_"+clusterThreshold.toString();
				if(sc.containsKey(k)){
					count ++;
				}
			}
			rsMD.close();
			pMD.close();
			System.out.println("Number of MD CLusters in CAMPS3 which are also SC clusters is "+ count);

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
