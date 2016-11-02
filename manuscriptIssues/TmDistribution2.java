package manuscriptIssues;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import utils.DBAdaptor;
import workflow.Dictionary3;

public class TmDistribution2 {

	/**
	 * @param args
	 * The class deals with the issue that the frequency of TM proteins in CAMPS SC clusters is different from how their frequency
	 * would be in genomes. i.e. 1tm highest and gradually decreasing till 13 and greater. 
	 * So basically this trend is established to be true. However, now the question is why does this happen?
	 * In order to assess the situation following would be calculated in this class
	 * 
	 * 1. no of clusters vs tms_range
	 * 2. cluster_sizes vs tm_range
	 * 
	 */

	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");

	private static ArrayList<String> tm_range = new ArrayList<String>();

	private static HashMap<String,Integer> range2NoOfCluster = new HashMap<String,Integer>();
	private static HashMap<String,Integer> range2len = new HashMap<String,Integer>();
	private static HashMap<String,Integer> range2size = new HashMap<String,Integer>();


	// FOR NoOfClusterVsTmsRange()
	private static Hashtable<Integer,Integer> SC_sequences = new Hashtable<Integer,Integer>();	// for all the sequences in SC clusters  - key seqid, value tm number
	private static ArrayList<Integer> seqids = new ArrayList<Integer>();
	private static Hashtable<Integer,Integer> tmNovsNoOfClust = new Hashtable<Integer,Integer>(); // key is tmNo..value is number of clusters
	private static Hashtable<Integer,Integer> tmNovsNoOfProts = new Hashtable<Integer,Integer>();
	private static Hashtable<Integer,ArrayList<Integer>> tmNotoSeqids = new Hashtable<Integer,ArrayList<Integer>>();

	// FOR populatetmNoToDescription
	private static Hashtable<Integer,Integer> tmNovsCount = new Hashtable<Integer,Integer>(); // key is tmNo and value is count of proteins with little or not description

	// FOR populatetmNoToAllignments
	private static Hashtable<Integer,Integer> tmNovsHits = new Hashtable<Integer,Integer>(); // key is tmNo and value is count of hits
	private static Hashtable<Integer,Integer> tmNovsHitsTotal = new Hashtable<Integer,Integer>(); // key is tmNo and value is count of hits

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Run***...");
		//System.exit(0);

		populateInfo();
		fixavg(); // 
		print();

		NoOfCLustersVsTmsNumber(); // computes for each tm protein type.. i.e. acc to number of helices.. and number of clusters

		//ClusterSizeVsTmsRange();

		// now have to calculate the hypothetical proteins and which tm group do they belong to
		populatetmNoToDescription();
		// now to check the number of hits
		String thresh05File = "/localscratch/CAMPS/thresh.005.abc";
		//String thresh05File = "F:/mcl/testJava/dict/dictionary.5.abc";
		reRUN_CAMPS.CoreExtraction.populatedictionaryFromSimilarityFile(thresh05File);
		calculateAlignmentStuff();
		printClustVsTm();

	}

	private static void calculateAlignmentStuff() {
		// TODO Auto-generated method stub
		try{
			// as the alignment stuff is calculated so now to check no of hits
			// the idea to calculate the number of hits for this seqiD with this number of tm
			// the hits must be in tm range of +-1
			for(int i=0;i<=seqids.size()-1;i++){
				int id = seqids.get(i);
				int tm = SC_sequences.get(id);

				ArrayList<Integer> seqIds_tm = new ArrayList<Integer>();
				ArrayList<Integer> seqIds_tmMinus1 = new ArrayList<Integer>();
				ArrayList<Integer> seqIds_tmPlus1 =  new ArrayList<Integer>();
				if(tm-1>0){
					seqIds_tmMinus1 = tmNotoSeqids.get(tm-1);
				}
				else{
					seqIds_tmMinus1 = new ArrayList<Integer>();
				}
				
				if(tm>=14){
					seqIds_tmPlus1 = tmNotoSeqids.get(14);
					tm = 14;
				}
				else{
					seqIds_tmPlus1 = tmNotoSeqids.get(tm+1);
				}
				
				if(!tmNotoSeqids.get(tm).isEmpty()){
					seqIds_tm = tmNotoSeqids.get(tm);
				}




				if(reRUN_CAMPS.CoreExtraction.dict.containsKey(id)){
					Dictionary3 hits = reRUN_CAMPS.CoreExtraction.dict.get(id);
					if(!hits.prot2score.isEmpty()){
					for(int j =0;j<=seqIds_tm.size()-1;j++){
						int currentId = seqIds_tm.get(j);
						if(currentId!=id){ // self hit
							if(hits.prot2score.containsKey(currentId)){
								// count ++
								if(tmNovsHits.containsKey(tm)){
									int count = tmNovsHits.get(tm);
									count ++;
									tmNovsHits.put(tm,count);
								}
								else{
									tmNovsHits.put(tm,1);
								}
							}
						}
					}

					try{
					if(!seqIds_tmMinus1.isEmpty() ){
						//System.out.println("Not Found TM -1 where tm is :"+tm);
						//continue;
						//}
						// minus 1
						for(int j =0;j<=seqIds_tmMinus1.size()-1;j++){
							int currentId = seqIds_tmMinus1.get(j);
							if(currentId!=id){ // self hit
								if(hits.prot2score.containsKey(currentId)){
									// count ++
									if(tmNovsHits.containsKey(tm)){
										int count = tmNovsHits.get(tm);
										count ++;
										tmNovsHits.put(tm,count);
									}
									else{
										tmNovsHits.put(tm,1);
									}
								}
							}
						}
					}
					}
					catch(Exception e){
						System.out.println("");
						System.out.println("Error at tm value: "+ tm);
						System.out.println(" and id: " + id);
						
						e.printStackTrace();
						System.exit(0);
					}
					if(!seqIds_tmPlus1.isEmpty() ){
						//System.out.println("Not Found TM +1 where tm is :"+tm);
						//continue;
						//}

						// plus 1
						for(int j =0;j<=seqIds_tmPlus1.size()-1;j++){
							int currentId = seqIds_tmPlus1.get(j);
							if(currentId!=id){ // self hit
								if(hits.prot2score.containsKey(currentId)){
									// count ++
									if(tmNovsHits.containsKey(tm)){
										int count = tmNovsHits.get(tm);
										count ++;
										tmNovsHits.put(tm,count);
									}
									else{
										tmNovsHits.put(tm,1);
									}
								}
							}
						}
					}
					// now calculate the total number of hits
					if(tmNovsHitsTotal.containsKey(tm)){
						int count = tmNovsHitsTotal.get(tm);
						count = count + hits.prot2score.size();
						tmNovsHitsTotal.put(tm,count);
					}
					else{
						tmNovsHitsTotal.put(tm,hits.prot2score.size());
					}
					
				}
			}
				else{
					System.out.println("Not Found:"+id);
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

	private static void populatetmNoToDescription() {
		// TODO Auto-generated method stub
		try{
			System.out.print("Descriptions set being processed\n");
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select cluster_id, cluster_threshold from cp_clusters2 where type=?");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_id=? and cluster_threshold=? ");
			pstm1.setString(1, "sc_cluster");
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				Integer clusid = rs1.getInt(1);
				Float thresh = rs1.getFloat(2);
				pstm2.setInt(1, clusid);
				pstm2.setFloat(2, thresh);
				int seqid;
				ResultSet rs2 = pstm2.executeQuery();
				// we have to check that this particular tm Number for this cluster has has not been processed, in order to avoid all extra counts, so,
				//boolean notProcessed = true;

				while(rs2.next()){
					seqid = rs2.getInt(1);
					//int tmNo = getTmNo(seqid);
					int tmNo = SC_sequences.get(seqid);
					if(tmNo>=14){
						tmNo = 14;
					}
					String descr = getDescription(seqid);
					if(descr == null || descr.isEmpty() || descr.contains("hypothetical")){
						if (tmNovsCount.containsKey(tmNo)){
							int t = tmNovsCount.get(tmNo);
							t++;
							tmNovsCount.put(tmNo, t);
						}
						else{
							tmNovsCount.put(tmNo, 1);
						}
					}
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static String getDescription(int seqid) {
		// TODO Auto-generated method stub
		try{
			String Description = "";
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select description from proteins2 where sequenceid=?");
			pstm2.setInt(1, seqid);
			ResultSet rs2 = pstm2.executeQuery();

			while(rs2.next()){
				Description = rs2.getString(1);
			}
			return Description;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	private static void printClustVsTm() {
		// TODO Auto-generated method stub
		System.out.println("");
		System.out.println("tmNo\tNumberOfClusters\tNumberOfProteins\tRatioOfProtsvsClusters\tProteinsWithNoDescription\tNumberofHitsInTmRange\tNumberofHitsInTotal");
		for(int i =1;i<=14;i++){
			float ratio = (float)tmNovsNoOfClust.get(i)/(float)tmNovsNoOfProts.get(i);
			System.out.println(i+"\t"+tmNovsNoOfClust.get(i)+"\t"+tmNovsNoOfProts.get(i)+"\t"+ratio+"\t"+tmNovsCount.get(i)+"\t"+tmNovsHits.get(i)+"\t"+tmNovsHitsTotal.get(i));
		}
		System.out.println("The ratio is higher showing low homogenity");
		System.out.println("The ratio is lower showing high homogenity");
		System.out.println("High homogeneity is when low number of proteins make higher number of clusters. Showing they had less homologs.");
	}

	private static void NoOfCLustersVsTmsNumber() {
		// TODO Auto-generated method stub
		try{
			// get the tm number for all the proteins in sc clusts
			System.out.print("SC set being processed\n");
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select cluster_id, cluster_threshold from cp_clusters2 where type=?");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_id=? and cluster_threshold=? ");
			pstm1.setString(1, "sc_cluster");
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				Integer clusid = rs1.getInt(1);
				Float thresh = rs1.getFloat(2);
				pstm2.setInt(1, clusid);
				pstm2.setFloat(2, thresh);
				int seqid;
				ResultSet rs2 = pstm2.executeQuery();
				// we have to check that this particular tm Number for this cluster has has not been processed, in order to avoid all extra counts, so,
				//boolean notProcessed = true;
				HashMap <String,Integer> temp = new HashMap<String,Integer>(); // key is processed: cluster_tmNo
				while(rs2.next()){
					seqid = rs2.getInt(1);
					int tmNo = getTmNo(seqid);
					if(tmNo>=14){
						tmNo =14;
					}
					SC_sequences.put(seqid, tmNo);
					seqids.add(seqid);

					if(tmNotoSeqids.containsKey(tmNo)){
						ArrayList<Integer> tem = tmNotoSeqids.get(tmNo);
						tem.add(seqid);
						tmNotoSeqids.put(tmNo, tem);
					}
					else{
						ArrayList<Integer> tem = new ArrayList<Integer>(); 
						tem.add(seqid);
						tmNotoSeqids.put(tmNo, tem);
					}

				

					String k = clusid.toString()+"_"+thresh.toString()+"_"+tmNo;
					if (temp.containsKey(k)){ // has been processed
						// do nothing
					}
					else{ // has not been processed
						populatetmNovsNoOfClust(seqid, tmNo);
						temp.put(k, 0);
					}

					// populate the tmNovsNoOfProts
					if(tmNovsNoOfProts.containsKey(tmNo)){
						int j = tmNovsNoOfProts.get(tmNo);
						j++;
						tmNovsNoOfProts.put(tmNo, j);
					}
					else{
						tmNovsNoOfProts.put(tmNo, 1);
					}
				}
				rs2.close();
				pstm2.clearBatch();
			}
			rs1.close();
			pstm1.close();
			pstm2.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

	private static void populatetmNovsNoOfClust(int seqid, int tmNo) {
		// TODO Auto-generated method stub

		if(tmNovsNoOfClust.containsKey(tmNo)){
			int x = tmNovsNoOfClust.get(tmNo);
			x++;
			tmNovsNoOfClust.put(tmNo, x);
		}
		else{
			tmNovsNoOfClust.put(tmNo, 1);
		}

	}

	public static int getTmNo(int seqid) {
		// TODO Auto-generated method stub
		int TMnumber = 0;
		try{
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select max(tms_id) from tms where sequenceid=?");
			pstm2.setInt(1, seqid);
			ResultSet rs2 = pstm2.executeQuery();

			while(rs2.next()){
				TMnumber = rs2.getInt(1);
			}
			return TMnumber;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return 0;
	}

	private static void fixavg() {
		// TODO Auto-generated method stub

	}

	private static void print() {
		// TODO Auto-generated method stub
		try{
			System.out.println("tmRange\tNoOfCluster\tLength\tSize");
			for(int i =0;i<=tm_range.size()-1;i++){
				String range = tm_range.get(i);
				int no = range2NoOfCluster.get(range);
				float len2 = range2len.get(range)/no;
				float sz = range2size.get(range)/no;

				System.out.println(range+"\t"+no+"\t"+len2+"\t"+sz);
				//System.out.println(range+"\t"+range2NoOfCluster.get(range)+"\t"+range2len.get(range)+"\t"+range2size.get(range));
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void populateInfo() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters2 where type=\"sc_cluster\"");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequences,average_length,tms_range from clusters_mcl_nr_info2 where" +
					" cluster_id=? and cluster_threshold=?");

			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				int clusid = rs1.getInt(1);
				float thresh = rs1.getFloat(2);

				pstm2.setInt(1, clusid);
				pstm2.setFloat(2, thresh);
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()){
					int size = rs2.getInt(1); // this is the cluster size.. do not confuse with number of clusters
					int len = rs2.getInt(2);
					String tmr = rs2.getString(3);

					if(!tm_range.contains(tmr)){
						tm_range.add(tmr);
					}

					if(range2NoOfCluster.containsKey(tmr)){
						int temp = range2NoOfCluster.get(tmr);
						temp ++;
						range2NoOfCluster.put(tmr, temp);
					}
					else{
						range2NoOfCluster.put(tmr, 1);
					}
					// now to calculate avg length... 
					// since we have avg length for a cluster.. we can sum the length of all clusters in this range and then divide it by the number of cluster in that range
					if(range2len.containsKey(tmr)){
						int temp = range2len.get(tmr);
						temp = temp + len;
						range2len.put(tmr, temp);
					}
					else{
						range2len.put(tmr, len);
					}


					// now to calculate avg size... 
					// since we have avg size for a cluster.. we can sum the size of all clusters in this range and then divide it by the number of cluster in that range
					if(range2size.containsKey(tmr)){
						int temp = range2size.get(tmr);
						temp = temp + size;
						range2size.put(tmr, temp);
					}
					else{
						range2size.put(tmr, size);
					}

				}
			}

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}


}
