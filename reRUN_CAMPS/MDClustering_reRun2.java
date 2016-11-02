package reRUN_CAMPS;

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

import utils.BitMatrix;
import utils.DBAdaptor;
import workflow.Protein;

public class MDClustering_reRun2 {

	/**
	 * @param args
	 */
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	//private static final Connection CAMPS_CONNECTIONMD = DBAdaptor.getConnection("camps4");
	//private static final Connection CAMPS_CONNECTIONMatrix = DBAdaptor.getConnection("camps4");

	private static final double MIN_PERCENTAGE_IDENTITY = 40;

	private static final double MIN_PERCENTAGE_OCCURRENCES = 75;

	//private static final double MIN_PERCENTAGE_IDENTITY = 35;

	//private static final double MIN_PERCENTAGE_OCCURRENCES = 75;


	//cluster id of SC-cluster for which MD-clusters should be calculated
	private Integer clusterID;			
	//cluster threshold of SC-cluster for which MD-clusters should be calculated
	private Float clusterThreshold; 
	//matrix containing information whether pair of sequences share at least 30% sequence identity
	private BitMatrix matrix;	
	//mapping between sequence ids and indices in matrix
	private int[] indizes;

	// For alignment File:
	private static ArrayList<Integer> queryy = new ArrayList<Integer>();
	private static ArrayList<Integer> hitt = new ArrayList<Integer>();
	private static ArrayList<Float> scoree = new ArrayList<Float>();

	private static HashMap <String,Integer> sc = new HashMap<String,Integer>();
	private static BufferedWriter bw;
	private static int CurrentclusSize = 0;

	// for fast computation
	private static HashMap <String, ArrayList<Integer>> seqIds = new HashMap<String, ArrayList<Integer>>();// for every cluster key the sequences in them
	private static HashMap <String, ArrayList<Protein>> ptbl1 = new HashMap<String, ArrayList<Protein>>();// mcl trackStuff
	private static HashMap <String, Integer> ptbl2 = new HashMap<String, Integer>();// number of sequences in given cluster.. mcl_info
	private static Hashtable<Integer, Integer> SequenceIdsInSC = new Hashtable<Integer, Integer>();

	private static HashMap <String, Integer> processedClusters = new HashMap<String, Integer>();// for every cluster key the sequences in them

	public MDClustering_reRun2(int clusterID, float clusterThreshold) {
		this.clusterID = clusterID;
		this.clusterThreshold = clusterThreshold;
	}


	public void run() {
		try {


			//int batchSize = 50;	
			//int batchSize = 1;

			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT child_cluster_id, child_cluster_threshold, intersection_size FROM clusters_mcl_track2 WHERE cluster_id=? AND cluster_threshold=?");
			//PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT sequences FROM clusters_mcl_nr_info2 WHERE cluster_id=? AND cluster_threshold=?");

			//			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
			//					"INSERT INTO cp_clusters " +
			//					"(cluster_id, cluster_threshold, super_cluster_id, super_cluster_threshold, type) " +
			//					"VALUES " +
			//					"(?,?,?,?,?)");


			int countMDCluster = 0;			
			//int batchCounter = 0;


			String cluster = clusterID + "#" + clusterThreshold;

			System.out.println("In progress: " +cluster);

			//
			//store information whether pairs of sequences have identity>=MIN_PERCENTAGE_IDENTITY
			//in triangle bitset matrix
			//

			setMatrix(clusterID, clusterThreshold);

			System.out.println("New Matrix Made for SC " +cluster + " of size "+ indizes.length);

			ArrayList<String> clusters = new ArrayList<String>();
			clusters.add(cluster);

			while(!clusters.isEmpty()) {

				String cluster2Test = clusters.get(0);		

				if(isMDCluster(cluster2Test)) {

					countMDCluster++;

					int mdClusterID = Integer.parseInt(cluster2Test.split("#")[0]);
					float mdClusterThreshold = Float.parseFloat(cluster2Test.split("#")[1]);

					//System.err.println("\tMD cluster: "+cluster2Test);

					//write to db

					//pstm.set ...
					//batchCounter++;

					//						pstm3.setInt(1, mdClusterID);
					//						pstm3.setFloat(2, mdClusterThreshold);
					//						pstm3.setInt(3, clusterID);
					//						pstm3.setFloat(4, clusterThreshold);
					//						pstm3.setString(5, "md_cluster");
					//																				
					//						pstm3.addBatch();
					//						
					//						if(batchCounter % batchSize == 0) {								
					//							pstm3.executeBatch();
					//							pstm3.clearBatch();
					//						}

					String k = this.clusterID.toString()+"_"+this.clusterThreshold.toString();
					if (sc.containsKey(k)){
						sc.remove(k);
					}
					System.out.println(mdClusterID+"\t"+mdClusterThreshold+"\t"+CurrentclusSize+"\t"+this.clusterID+"\t"+this.clusterThreshold+"\t");
					bw.write(mdClusterID+"\t"+mdClusterThreshold+"\t"+CurrentclusSize+"\t"+this.clusterID+"\t"+this.clusterThreshold);
					bw.newLine();
					bw.flush();

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
							double percCoverage = 100 * ((double) intersectionSize/numberMembers); // smaller the number of members > the percCovered
							//if(percCoverage >= 40) {		// was 90 before					
							//if(percCoverage >= 15) {		// was 90 before
							if(!processedClusters.containsKey(childClusterID+"_"+childClusterThreshold)){	
								//if(percCoverage >= 15 ) {		// was 90 before
									String childCluster = childClusterID + "#" + childClusterThreshold;
									clusters.add(childCluster);
									System.err.println("ToProcess: "+ clusters.size());
									System.err.println("\t\t### "+childClusterID+"#"+childClusterThreshold+"\t"+percCoverage);
								//}
							}
						}
					}
					/*
					pstm1.setInt(1, clusterID2);
					pstm1.setFloat(2, clusterThreshold2);
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {
						int childClusterID = rs1.getInt("child_cluster_id");
						float childClusterThreshold = rs1.getFloat("child_cluster_threshold");
						int intersectionSize = rs1.getInt("intersection_size");
						int numberMembers = 0;
						pstm2.setInt(1, childClusterID);
						pstm2.setFloat(2, childClusterThreshold);
						ResultSet rs2 = pstm2.executeQuery();
						while(rs2.next()) {
							numberMembers = rs2.getInt("sequences");
						}
						rs2.close();
						double percCoverage = 100 * ((double) intersectionSize/numberMembers);
						if(percCoverage >= 90) {							
							String childCluster = childClusterID + "#" + childClusterThreshold;
							clusters.add(childCluster);
							System.err.println("\t\t### "+childClusterID+"#"+childClusterThreshold+"\t"+percCoverage);
						}
					}
					rs1.close();
					 */

				}
			}			



			//pstm1.close();
			//pstm2.close();
			//			pstm3.executeBatch();	//insert remaining entries
			//			pstm3.clearBatch();
			//			pstm3.close();

			System.out.println("\n\nNumber of MD clusters: " +countMDCluster);


		} catch(Exception e) {
			System.err.println("Exception in MDClustering2.run(): " +e.getMessage());
			e.printStackTrace();
			System.exit(0);

		} 
	}


	private void setMatrix(int clusterID, float clusterThreshold) {
		try {


			BitSet members = new BitSet();
			int maxSequenceid = Integer.MIN_VALUE;			

			//
			//get members
			//
			/*
			Statement stm1 = CAMPS_CONNECTIONMatrix.createStatement();
			ResultSet rs1 = stm1.executeQuery("SELECT sequenceid FROM clusters_mcl2 WHERE cluster_id="+clusterID+" and cluster_threshold="+clusterThreshold);
			while(rs1.next()) {

				int sequenceid = rs1.getInt("sequenceid");
				members.set(sequenceid);

				if(sequenceid > maxSequenceid) {
					maxSequenceid = sequenceid;
				}
			}
			rs1.close();
			rs1 = null;
			stm1.close();
			stm1 = null;
			 */
			//
			// get members from hash table
			//
			String key = Integer.toString(clusterID)+Float.toString(clusterThreshold);
			ArrayList<Integer> sids = seqIds.get(key);
			for(int i =0;i<=sids.size()-1;i++){
				int sequenceid = sids.get(i);
				members.set(sequenceid);

				if(sequenceid > maxSequenceid) {
					maxSequenceid = sequenceid;
				}
			}
			sids = null;

			this.indizes = new int[maxSequenceid+1];

			int index = 0;
			for(int id = members.nextSetBit(0); id>=0; id = members.nextSetBit(id+1)) {

				indizes[id] = index; 
				index++;
			}


			int matrixSize = members.cardinality();
			this.matrix = new BitMatrix(matrixSize);

			//
			//get identities and save information if identity >= MIN_PERCENTAGE_IDENTITY
			//in bitset matrix (only use right upper part of matrix, since identity values
			//are symmetric)
			//
			for(int i =0;i<=queryy.size()-1;i++){

				int sequenceIDQuery = queryy.get(i);
				int sequenceIDHit = hitt.get(i);				
				float identity = scoree.get(i);

				//int sequenceIDQuery = rs2.getInt("sequences_sequenceid_query");
				//int sequenceIDHit = rs2.getInt("sequences_sequenceid_hit");				
				//float identity = 100* rs2.getFloat("identity");

				if(members.get(sequenceIDQuery) && members.get(sequenceIDHit)) {

					int rowIndex = indizes[sequenceIDQuery];
					int colIndex = indizes[sequenceIDHit];

					if(identity >= MIN_PERCENTAGE_IDENTITY) {
						matrix.set(rowIndex, colIndex);
						matrix.set(colIndex,rowIndex); // to fill complete matrix
					}					
				}
			}
			//rs2.close();
			//rs2 = null;
			//stm2.close();
			//stm2 = null;

		}catch(Exception e) {
			System.err.println("Exception in MDClustering2.createMatrix(): " +e.getMessage());
			e.printStackTrace();

		}
	}


	private boolean isMDCluster(String cluster) {
		boolean isMDCluster = false;

		try {


			int clusterID = Integer.parseInt(cluster.split("#")[0]);
			float clusterThreshold = Float.parseFloat(cluster.split("#")[1]);

			processedClusters.put(clusterID+"_"+clusterThreshold,null);
			BitSet sequenceIDs = new BitSet();

			//get cluster members
			/*
			Statement stm = CAMPS_CONNECTIONMD.createStatement();
			ResultSet rs = stm.executeQuery("SELECT sequenceid FROM clusters_mcl2 WHERE cluster_id="+clusterID+" and cluster_threshold="+clusterThreshold);
			while(rs.next()) {

				int sequenceID = rs.getInt("sequenceid");
				sequenceIDs.set(sequenceID);

			}
			rs.close();
			rs = null;
			stm.close();
			 */
			//
			// get cluster members - from hash table
			//
			String key = Integer.toString(clusterID)+Float.toString(clusterThreshold);
			ArrayList<Integer> sids = seqIds.get(key);
			for(int i =0;i<=sids.size()-1;i++){
				int sequenceID = sids.get(i);
				sequenceIDs.set(sequenceID);
			}
			sids = null;

			int numMembers = sequenceIDs.cardinality();

			int numValidMembers = 0;
			//double threshold = (numMembers * MIN_PERCENTAGE_OCCURRENCES)/((double) 100); //!!! Check: isn't (numMembers-1) correct here?!
			int threshold = (int) Math.round(((numMembers-1) * MIN_PERCENTAGE_OCCURRENCES)/((double) 100)); 


			int counter = 0;

			for(int sequenceID1 = sequenceIDs.nextSetBit(0); sequenceID1>=0; sequenceID1 = sequenceIDs.nextSetBit(sequenceID1+1)) {


				counter++;

				counter ++;
				if (counter % 100 == 0) {
					System.err.write('.');
					System.err.flush();
				}
				if (counter % 10000 == 0) {
					System.err.write('\n');
					System.err.flush();
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
					try{
						if(this.matrix.get(rowIndex,colIndex)) {
							countValidPairs++;
						}
						else if(this.matrix.get(colIndex,rowIndex)) {
							countValidPairs++;
						}
					}
					catch(Exception e){
						System.err.print("at counting valid painrs: ");
						e.printStackTrace();
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
				System.err.println("\t\tIgnore: " +cluster+"\t"+numMembers +"\t"+numValidMembers);
			}

			CurrentclusSize = numMembers;
		} catch(Exception e) {
			System.err.println("AT is MD");
			e.printStackTrace();
			System.exit(0);
		}

		return isMDCluster;

	}

	private static void populate_alignmentsNew(String thresh05File){
		try{	
			// make a hashmap with key as seqid_query and value is seqid_hit and identity value.
			// Can use the Dictionary class, as it is exactly what is required.
			//PreparedStatement pstm = null;
			System.out.println("Fetching Alignment");
			int lineNo =0;
			BufferedReader br = new BufferedReader(new FileReader(new File(thresh05File))); 
			String line = "";
			while((line=br.readLine())!=null){
				lineNo++;
				String[] p = line.split(" ");
				int query = Integer.parseInt(p[0].trim());
				int idhit = Integer.parseInt(p[1].trim());
				float identity = 0f;
				try{
					identity = Float.parseFloat(p[2]);
				}
				catch(NumberFormatException e){
					identity = 101;
				}
				if(identity >= MIN_PERCENTAGE_IDENTITY){
					queryy.add(query);
					hitt.add(idhit);
					scoree.add(identity);
				}
				if(lineNo%100000 == 0){
					System.out.println("Hits Processed: "+lineNo);
				}
			}
			br.close();
			System.out.println("Populated Alignments..Size: "+ queryy.size());

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void populate_mclTrack(){
		try{
			System.out.println("Fetching MCL Track");
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT cluster_id,cluster_threshold,child_cluster_id, child_cluster_threshold, intersection_size FROM clusters_mcl_track2");
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
	private static void populate_listofSeqids(){ // used in set matrix
		try{
			System.out.println("Fetching Seqids");
			Statement stm1 = CAMPS_CONNECTION.createStatement();
			ResultSet rs1 = stm1.executeQuery("SELECT cluster_id,cluster_threshold,sequenceid FROM clusters_mcl2");
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
	private static void populate_mcl_info(){
		try{
			System.out.println("Fetching MCL info");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT cluster_id,cluster_threshold,sequences FROM clusters_mcl_nr_info2");

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

	public static void main(String[] args) {

		try {


			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );

			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");

			populate_listofSeqids();
			populate_mclTrack();
			populate_mcl_info();

			//String alignemtnsFileSmall = "/localscratch/CAMPS/SCClusters_alignmentFile.abc";
			String alignemtnsFileSmall = "/home/users/saeed/SCClusters_alignmentFile.abc";
			populate_alignmentsNew(alignemtnsFileSmall);

			//MDClustering2 md = new MDClustering2(clusterID,clusterThreshold);
			//md.run();

			bw = new BufferedWriter(new FileWriter (new File("/home/users/saeed/MDClusterReportReRun_Test.txt")));
			//bw = new BufferedWriter(new FileWriter (new File("/home/users/saeed/MDClusterReportReRun_runNo6.txt")));
			//File[] files = new File(metaModelDirectory).listFiles();
			// ****************** get the sc cluster hash table to compute how many sc clusters yield mds

			ArrayList<Integer> cluids = new ArrayList<Integer>();
			ArrayList<Float> threshs = new ArrayList<Float>();

			PreparedStatement p = CAMPS_CONNECTION.prepareStatement("SELECT cluster_id,cluster_threshold FROM cp_clusters2 where type=\"sc_cluster\"");
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

			System.out.println("SC clusters in total: "+cluids.size());
			for(int i = 0;i<=cluids.size()-1;i++){
				int cID = cluids.get(i);
				float clusterThr = threshs.get(i);
				MDClustering_reRun2 md = new MDClustering_reRun2(cID,clusterThr);
				md.run();
			}

			bw.close();

			/*
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
			 */
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");



		} catch(Exception e) {
			e.printStackTrace();
		}


	}


}
