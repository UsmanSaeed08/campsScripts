package reRUN_CAMPS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;

import utils.DBAdaptor;
import workflow.Protein;

public class MDClustering_reRun3 {

	/**
	 * @param args
	 */
	private static HashMap <String, ArrayList<Integer>> seqIds = new HashMap<String, ArrayList<Integer>>();// for every cluster key the sequences in them
	private static HashMap <String, ArrayList<Protein>> ptbl1 = new HashMap<String, ArrayList<Protein>>();// mcl trackStuff
	private static HashMap <String, Integer> ptbl2 = new HashMap<String, Integer>();// number of sequences in given cluster.. mcl_info
	private static HashMap <Integer, Integer> insc = new HashMap<Integer, Integer>();// number of sequences in given cluster.. mcl_info
	private static HashMap <String,Integer> sc = new HashMap<String,Integer>();
	//private static String alignemtnsFileSmall = "/home/users/saeed/SCClusters_alignmentFile.abc"; //thresh.005.abc
	//private static String alignemtnsFileSmall = "/localscratch/CAMPS/SCClusters_alignmentFile2.abc"; //thresh.005.abc
	private static String alignemtnsFileSmall = "/localscratch/CAMPS/SCClusters_alignmentFile15Identity.abc"; //thresh.005.abc
	//private static String alignemtnsFileSmall = "/localscratch/CAMPS/thresh.005.abc"; //thresh.005.abc
	//private static String alignemtnsFileSmall = "F:/testAlgn_CMSC0702";
	//private static String alignemtnsFileSmall = "F:/SCClusters_alignmentFile.abc";
	//private static String alignemtnsFileSmall = "F:/testAlgn_CMSC0001";

	private static BufferedWriter bw;

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");

	private static final double MIN_PERCENTAGE_IDENTITY_forFile = 15;

	private static final double MIN_PERCENTAGE_IDENTITY = 40;
	private static final double MIN_PERCENTAGE_OCCURRENCES = 80;

	private static final double MIN_PERCENTAGE_IDENTITYLow = 15;
	private static final double MIN_PERCENTAGE_OCCURRENCESLow = 1;//60;

	private static String status ;
	private static int countMDClusterTotal = 0;
	private static int countMDClusterLowTotal = 0;
	private static int processedTotal = 1;
	
	//cluster id of SC-cluster for which MD-clusters should be calculated
	private Integer clusterID;			
	//cluster threshold of SC-cluster for which MD-clusters should be calculated
	private Float clusterThreshold;

	private static int CurrentclusSize = 0;

	private static ArrayList<Integer> queryy = new ArrayList<Integer>();// contains complete set of hits
	private static ArrayList<Integer> hitt = new ArrayList<Integer>();
	private static ArrayList<Float> scoree = new ArrayList<Float>();

	private static ArrayList<Integer> query_temp = new ArrayList<Integer>(); // contains the hits for specified cluster with highest possible identity
	private static ArrayList<Integer> hit_temp = new ArrayList<Integer>();
	
	private static ArrayList<Integer> query_tempLow = new ArrayList<Integer>(); // contains the hits for specified cluster with Lower identity
	private static ArrayList<Integer> hit_tempLow = new ArrayList<Integer>();

	public MDClustering_reRun3(int clusterID, float clusterThreshold) {
		this.clusterID = clusterID;
		this.clusterThreshold = clusterThreshold;
	}

	private static void populate_listofSeqids(boolean test,int c,float t){ // used in set matrix
		try{
			System.out.println("Fetching Seqids");
			Statement stm1 = CAMPS_CONNECTION.createStatement();
			ResultSet rs1;
			if(test){
				rs1 = stm1.executeQuery("SELECT cluster_id,cluster_threshold,sequenceid FROM clusters_mcl2 where cluster_id="+c+
						" and cluster_threshold="+t);
			}
			else{
				rs1 = stm1.executeQuery("SELECT cluster_id,cluster_threshold,sequenceid FROM clusters_mcl2");
			}
			while(rs1.next()) {

				int sequenceid = rs1.getInt("sequenceid");
				int clusid = rs1.getInt("cluster_id");
				float thresh = rs1.getFloat("cluster_threshold");
				String key = Integer.toString(clusid)+"_"+Float.toString(thresh);
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
				if(!insc.containsKey(sequenceid)){
					insc.put(sequenceid,null);
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
				String key = Integer.toString(clusid)+"_"+Float.toString(thresh);
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
	private static void populate_mcl_info(){
		try{
			System.out.println("Fetching MCL info");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT cluster_id,cluster_threshold,sequences FROM clusters_mcl_nr_info2");

			ResultSet rs2 = pstm2.executeQuery();

			while(rs2.next()) {
				//int numberMembers = rs2.getInt("sequences");
				int clusid = rs2.getInt("cluster_id");
				float thresh = rs2.getFloat("cluster_threshold");
				String key = Integer.toString(clusid)+"_"+Float.toString(thresh);
				//int numberMembers = seqIds.get(key).size();
				int numberMembers = rs2.getInt("sequences");
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
	private static void PopulateAlignFile(){
		try{
			// the function is like populatealignments, but rather it uses the file to read every time for every sc cluster
			// and populates the queryy and score and hit. 
			System.out.println("Fetching Alignment");
			queryy = new ArrayList<Integer>();
			hitt = new ArrayList<Integer>();
			scoree = new ArrayList<Float>();
			int lineNo =0;
			BufferedReader br = new BufferedReader(new FileReader(new File(alignemtnsFileSmall))); 
			String line = "";
			while((line=br.readLine())!=null){
				lineNo++;
				String[] p = line.split(" ");
				int query = Integer.parseInt(p[0].trim());
				int idhit = Integer.parseInt(p[1].trim());

				if(insc.containsKey(query) && insc.containsKey(idhit)){	
					float identity = 0f;
					try{
						identity = Float.parseFloat(p[2]);
					}
					catch(NumberFormatException e){
						identity = 101;
					}
					//if(identity >= MIN_PERCENTAGE_IDENTITY){// populate the basic array with least possible identity
					if(identity >= MIN_PERCENTAGE_IDENTITYLow){// populate the basic array with least possible identity
						queryy.add(query);
						hitt.add(idhit);
						scoree.add(identity);
					}
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
	private static void setMatrixUsingAlignFileHighandLow(HashMap<Integer,Integer> clusterMembers){
		try{
			// the function is like populatealignments, but rather it uses the file to read every time for every sc cluster
			// and populates the queryy and score and hit. 
			System.out.println("Processing hits...");
			query_temp = new ArrayList<Integer>();
			hit_temp = new ArrayList<Integer>();
			
			query_tempLow = new ArrayList<Integer>();
			hit_tempLow = new ArrayList<Integer>();
			for(int i =0;i<=queryy.size()-1;i++){
				if(scoree.get(i)>=MIN_PERCENTAGE_IDENTITY){
					if(clusterMembers.containsKey(queryy.get(i)) && clusterMembers.containsKey(hitt.get(i))){
						query_temp.add(queryy.get(i));
						hit_temp.add(hitt.get(i));
					}
				}
				if(scoree.get(i)>=MIN_PERCENTAGE_IDENTITYLow){
					if(clusterMembers.containsKey(queryy.get(i)) && clusterMembers.containsKey(hitt.get(i))){
						query_tempLow.add(queryy.get(i));
						hit_tempLow.add(hitt.get(i));
					}
				}
			}
			System.out.println("Populated Alignments..Size: "+ query_temp.size());
			System.out.println("Populated Alignments with lower identity..Size: "+ query_tempLow.size());
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	


	private boolean isMDCluster(String cluster) {
		boolean isMDCluster = false;
		// So i noticed that here the threshold is calculated based on number of member, but these are redundant number of members
		// why not use nr number of members
		try {
			int clusterID = Integer.parseInt(cluster.split("#")[0]);
			float clusterThreshold = Float.parseFloat(cluster.split("#")[1]);
			//processedClusters.put(clusterID+"_"+clusterThreshold,null);
			ArrayList<Integer> sequenceIDs = new ArrayList<Integer>();
			HashMap<Integer,Integer> seqIdMap = new HashMap<Integer,Integer>();
			//
			// get cluster members - from hash table
			//
			String key = Integer.toString(clusterID)+"_"+Float.toString(clusterThreshold);
			ArrayList<Integer> sids = seqIds.get(key);
			for(int i =0;i<=sids.size()-1;i++){
				int sequenceID = sids.get(i);
				seqIdMap.put(sequenceID, null);
				sequenceIDs.add(sequenceID);
			}
			sids = null;
			// make a small subset of the cluster members, so dont have to go through unnecessary hits and query

			//ArrayList<Integer> q_temp = new ArrayList<Integer>();
			//ArrayList<Integer> h_temp = new ArrayList<Integer>();


			HashMap<Integer,Integer> id2count = new HashMap<Integer,Integer>();

			for(int m =0 ;m<=query_temp.size()-1;m++){
				int q = query_temp.get(m);
				int h = hit_temp.get(m);
				if(seqIdMap.containsKey(q) && seqIdMap.containsKey(h)){
					if(id2count.containsKey(q)){
						int temp = id2count.get(q);
						temp ++;
						id2count.put(q, temp);
					}
					else{
						id2count.put(q, 1);
					}

					if(id2count.containsKey(h)){
						int temp = id2count.get(h);
						temp ++;
						id2count.put(h, temp);
					}
					else{
						id2count.put(h, 1);
					}
				}
			}


			int numMembers = sequenceIDs.size();
			int numValidMembers = 0;
			//double threshold = (numMembers * MIN_PERCENTAGE_OCCURRENCES)/((double) 100); //!!! Check: isn't (numMembers-1) correct here?!
			int threshold = (int) Math.round(((numMembers-1) * MIN_PERCENTAGE_OCCURRENCES)/((double) 100)); 
			int counter = 0;

			//for(int sequenceID1 = sequenceIDs.nextSetBit(0); sequenceID1>=0; sequenceID1 = sequenceIDs.nextSetBit(sequenceID1+1)) {
			/*for(int i =0;i<=sequenceIDs.size()-1;i++){
				int sequenceID1 = sequenceIDs.get(i);
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
				for(int j=0;j<=q_temp.size()-1;j++){
					int q = q_temp.get(j);
					int h = h_temp.get(j);
					//float s = scoree.get(j);
					if (q==sequenceID1 && seqIdMap.containsKey(h)){
						countValidPairs++;
					}
					else if(h == sequenceID1 && seqIdMap.containsKey(q)){
						countValidPairs++;
					}
				}
				if(countValidPairs >= threshold) {
					numValidMembers++;
				}								
			}*/
			for(int i =0;i<=sequenceIDs.size()-1;i++){
				int sequenceID1 = sequenceIDs.get(i);
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
				if(id2count.containsKey(sequenceID1)){
					countValidPairs = id2count.get(sequenceID1);
				}

				if(countValidPairs >= threshold) {
					numValidMembers++;
				}								
			}

			// ****
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

	private boolean isMDClusterLow(String cluster) {
		boolean isMDCluster = false;
		// So i noticed that here the threshold is calculated based on number of member, but these are redundant number of members
		// why not use nr number of members
		try {
			int clusterID = Integer.parseInt(cluster.split("#")[0]);
			float clusterThreshold = Float.parseFloat(cluster.split("#")[1]);
			//processedClusters.put(clusterID+"_"+clusterThreshold,null);
			ArrayList<Integer> sequenceIDs = new ArrayList<Integer>();
			HashMap<Integer,Integer> seqIdMap = new HashMap<Integer,Integer>();
			//
			// get cluster members - from hash table
			//
			String key = Integer.toString(clusterID)+"_"+Float.toString(clusterThreshold);
			ArrayList<Integer> sids = seqIds.get(key);
			for(int i =0;i<=sids.size()-1;i++){
				int sequenceID = sids.get(i);
				seqIdMap.put(sequenceID, null);
				sequenceIDs.add(sequenceID);
			}
			sids = null;
			
			HashMap<Integer,Integer> id2count = new HashMap<Integer,Integer>(); // seq to no of hits

			for(int m =0 ;m<=query_tempLow.size()-1;m++){
				int q = query_tempLow.get(m);
				int h = hit_tempLow.get(m);
				if(seqIdMap.containsKey(q) && seqIdMap.containsKey(h)){
					if(id2count.containsKey(q)){
						int temp = id2count.get(q);
						temp ++;
						id2count.put(q, temp);
					}
					else{
						id2count.put(q, 1);
					}

					if(id2count.containsKey(h)){
						int temp = id2count.get(h);
						temp ++;
						id2count.put(h, temp);
					}
					else{
						id2count.put(h, 1);
					}
				}
			}


			int numMembers = sequenceIDs.size();
			int numValidMembers = 0;
			//double threshold = (numMembers * MIN_PERCENTAGE_OCCURRENCES)/((double) 100); //!!! Check: isn't (numMembers-1) correct here?!
			int threshold = (int) Math.round(((numMembers-1) * MIN_PERCENTAGE_OCCURRENCESLow)/((double) 100)); 
			int counter = 0;

			for(int i =0;i<=sequenceIDs.size()-1;i++){
				int sequenceID1 = sequenceIDs.get(i);
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
				if(id2count.containsKey(sequenceID1)){
					countValidPairs = id2count.get(sequenceID1);
				}

				if(countValidPairs >= threshold) {
					numValidMembers++;
				}								
			}

			// ****
			if(numMembers == numValidMembers) {
				isMDCluster = true;				
			}
			else {
				System.err.println("\t\tCluster also failed Low Test: " +cluster+"\t"+numMembers +"\t"+numValidMembers);
			}

			CurrentclusSize = numMembers;
		} catch(Exception e) {
			System.err.println("AT is MD Low");
			e.printStackTrace();
			System.exit(0);
		}
		return isMDCluster;
	}

	public void run() {
		try {
			int countMDCluster = 0;
			int countMDClusterLow = 0;
			int processed = 1;
			
			String cluster = clusterID + "#" + clusterThreshold;
			System.out.println("In progress: " +cluster);

			// first get all cluster members
			HashMap<Integer,Integer> members = new HashMap<Integer,Integer>();

			String key = clusterID.toString()+"_"+clusterThreshold.toString();

			if(seqIds.containsKey(key)){
				ArrayList<Integer> x = seqIds.get(key);
				for(int i=0;i<=x.size()-1;i++){
					members.put(x.get(i), null);
				}
			}
			if(members.isEmpty()){
				System.err.println("Critical Eroor.. no cluster members...Exit");
				System.exit(0);
			}
			// then use these to populate their min Identity score
			setMatrixUsingAlignFileHighandLow(members); // set the arrays based on high identity

			ArrayList<String> clusters = new ArrayList<String>();
			clusters.add(cluster);

			while(!clusters.isEmpty()) {
				processedTotal++;
				String cluster2Test = clusters.get(0);		

				if(isMDCluster(cluster2Test)) {

					countMDCluster++;
					countMDClusterTotal++;

					int mdClusterID = Integer.parseInt(cluster2Test.split("#")[0]);
					float mdClusterThreshold = Float.parseFloat(cluster2Test.split("#")[1]);


					//String k = this.clusterID.toString()+"_"+this.clusterThreshold.toString();

					System.out.println(mdClusterID+"\t"+mdClusterThreshold+"\t"+CurrentclusSize+"\t"+this.clusterID+"\t"+this.clusterThreshold+"\t");
					bw.write(mdClusterID+"\t"+mdClusterThreshold+"\t"+CurrentclusSize+"\t"+this.clusterID+"\t"+this.clusterThreshold);
					bw.newLine();
					bw.flush();
					System.out.flush();

					clusters.remove(0);
				}
				else {

					clusters.remove(0);
					int clusterID2 = Integer.parseInt(cluster2Test.split("#")[0]);
					float clusterThreshold2 = Float.parseFloat(cluster2Test.split("#")[1]);

					key = Integer.toString(clusterID2)+"_"+Float.toString(clusterThreshold2);
					if(ptbl1.containsKey(key)){
						ArrayList<Protein> p = ptbl1.get(key);
						for(int i =0;i<=p.size()-1;i++){
							int childClusterID = p.get(i).child_clusterid;
							float childClusterThreshold = p.get(i).child_clus_thresh;
							//int intersectionSize = p.get(i).intersectionsSz;
							//int numberMembers = 0;

							//String keyChild = Integer.toString(childClusterID)+"_"+Float.toString(childClusterThreshold);
							//if(ptbl2.containsKey(keyChild)){
							//numberMembers = ptbl2.get(keyChild);
							//}
							//double percCoverage = 100 * ((double) intersectionSize/numberMembers); // smaller the number of members > the percCovered
							//if(percCoverage >= 40) {		// was 90 before					
							//if(percCoverage >= 90) {		// was 90 before
							//if(!processedClusters.containsKey(childClusterID+"_"+childClusterThreshold)){	
							//if(percCoverage >= 90 ) {		// was 90 before
							processed++;
							String childCluster = childClusterID + "#" + childClusterThreshold;

							//if(!checkedClus.containsKey(childCluster)){
							//checkedClus.put(childCluster, null);
							clusters.add(childCluster);
							System.out.println("Clusters Processed: "+ processed);
							System.out.println("Remaining Clusters: "+ clusters.size());
							System.out.println("Added Cluster: "+ childCluster);
							System.out.println("Status: "+ status);
							//}
							//else{
							//System.err.println("Recursive Process for: "+ childCluster);
							//System.out.println("Exiting...Clusters Processed: "+ processed);
							//System.exit(0);
							//}

							//System.err.println("ToProcess: "+ clusters.size());
							//System.err.println("\t\t### "+childClusterID+"#"+childClusterThreshold+"\t"+percCoverage);
							//}
							//}
						}
					}
					else{
						// the cluster has no childs because
						// 1. it is either having just no childs
						// 2. no childs because has reached the threshold level 100
						// in either case - run less stringent checking method
						if(isMDClusterLow(cluster2Test)){
							// do not remove from clusters array because
							// the tested cluster was already removed
							countMDClusterLow++;
							countMDClusterLowTotal++;
							int mdClusterID = Integer.parseInt(cluster2Test.split("#")[0]);
							float mdClusterThreshold = Float.parseFloat(cluster2Test.split("#")[1]);

							System.out.println(mdClusterID+"\t"+mdClusterThreshold+"\t"+CurrentclusSize+"\t"+this.clusterID+"\t"+this.clusterThreshold+"\t"+"LowIdentityCluster");
							bw.write(mdClusterID+"\t"+mdClusterThreshold+"\t"+CurrentclusSize+"\t"+this.clusterID+"\t"+this.clusterThreshold+"\t"+"LowIdentityCluster");
							bw.newLine();
							bw.flush();
							System.out.flush();
						}
					}
				}

			}			
			System.out.println("\n\nNumber of MD clusters: " +countMDCluster);
			System.out.println("\n\nNumber of Low identity MD clusters: " +countMDClusterLow);
			System.out.println("Clusters Processed: "+ processed);


		} catch(Exception e) {
			System.err.println("Exception in MDClustering2.run(): " +e.getMessage());
			e.printStackTrace();
			System.exit(0);

		} 
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try{
			System.out.println("Running...");
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );

			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			// to run making small file
			//String outfile = "/localscratch/CAMPS/SCClusters_alignmentFile15Identity.abc";
			//makeSmallSCFile(alignemtnsFileSmall,outfile); // based on the identity threshold, make small file of the set of sequences only in sc
			//System.exit(0);
			// end runing the small file making

			bw = new BufferedWriter(new FileWriter (new File("/home/users/saeed/MDClusterReportReRun_Test12.txt")));
			//Integer clusterID = 16218; //
			//Float clusterThreshold = 24f;
			//Integer clusterID = 0; //
			//Float clusterThreshold = 5f;

			// 16218 |                24 // mds= 6

			//populate_listofSeqids(true,clusterID,clusterThreshold); // to run test
			populate_listofSeqids(false,0,0f); // to run normal
			populate_mclTrack();
			populate_mcl_info();
			PopulateAlignFile();
			// no alignments to be populated... because the idea is to read the file each time for each SC cluster
			//String alignemtnsFileSmall = "/home/users/saeed/SCClusters_alignmentFile.abc";
			//populate_alignmentsNew(alignemtnsFileSmall);

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
				status = i+" of "+cluids.size();
				MDClustering_reRun3 md = new MDClustering_reRun3(cID,clusterThr);
				md.run();
			}

			bw.close();

			System.out.println("\n\nNumber of MD clusters: " +countMDClusterTotal);
			System.out.println("\n\nNumber of Low identity MD clusters: " +countMDClusterLowTotal);
			System.out.println("Clusters Processed: "+ processedTotal);
			
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

	private static void makeSmallSCFile(String infile,
			String outfile) {
		// TODO Auto-generated method stub
		try{
			// the function is like populatealignments, but rather it uses the file to read every time for every sc cluster
			// and populates the queryy and score and hit.
			/*System.out.println("Fetching SequenceList");
			Statement stm1 = CAMPS_CONNECTION.createStatement();
			ResultSet rs1 = stm1.executeQuery("SELECT sequenceid from sequences2 where in_SC=\"Yes\"");
			while(rs1.next()) {
				int sequenceid = rs1.getInt("sequenceid");
				if(!insc.containsKey(sequenceid)){
					insc.put(sequenceid,null);
				}
			}
			rs1.close();
			rs1 = null;
			stm1.close();
			stm1 = null;*/
			System.out.println("Fetching SequenceList");
			Statement stm1 = CAMPS_CONNECTION.createStatement();
			ResultSet rs1 = stm1.executeQuery("SELECT cluster_id,cluster_threshold from cp_clusters2 where type=\"sc_cluster\"");
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_id=? and cluster_threshold=?");
			while(rs1.next()) {
				int id = rs1.getInt(1);
				float th = rs1.getFloat(2);
				pstm.setInt(1, id);
				pstm.setFloat(2, th);
				ResultSet re = pstm.executeQuery();
				while(re.next()){
					int sequenceid = re.getInt("sequenceid");
					if(!insc.containsKey(sequenceid)){
						insc.put(sequenceid,null);
					}
				}
			}
			rs1.close();
			rs1 = null;
			stm1.close();
			stm1 = null;
			System.out.println("Size of sequences: " +insc.size());


			System.out.println("Fetching Alignment");
			
			int lineNo =0;
			BufferedReader br = new BufferedReader(new FileReader(new File(infile))); 
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outfile)));

			String line = "";
			while((line=br.readLine())!=null){
				lineNo++;
				String[] p = line.split(" ");
				int query = Integer.parseInt(p[0].trim());
				int idhit = Integer.parseInt(p[1].trim());

				if(insc.containsKey(query) && insc.containsKey(idhit)){	
					float identity = 0f;
					try{
						identity = Float.parseFloat(p[2]);
					}
					catch(NumberFormatException e){
						identity = 101;
					}
					if(identity >= MIN_PERCENTAGE_IDENTITY_forFile){
						bw.write(line);
						bw.newLine();
					}
				}

				if(lineNo%100000 == 0){
					System.out.println("Hits Processed: "+lineNo);
				}
			}
			br.close();
			bw.close();
			System.out.println("Populated Alignments..Size: "+ queryy.size());


		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
