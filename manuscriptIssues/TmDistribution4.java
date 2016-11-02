package manuscriptIssues;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import utils.DBAdaptor;

public class TmDistribution4 {

	/**
	 * @param args
	 * the class is an extension on TmDistribution3. 
	 * It however is focused on calculating 1. the sequences and their tm
	 * then see all mcl clusters with sizes <15 and plot their distribution acc to single tm  
	 */
	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");

	private static HashMap<Integer,Integer> allProtsMap = new HashMap<Integer,Integer>(); // key is seqid and value is tmNo
	private static ArrayList<Integer> allprots = new ArrayList<Integer>(); // sequenceids
	//private static Hashtable<Integer,ArrayList<Integer>> tmNotoSeqids = new Hashtable<Integer,ArrayList<Integer>>();
	private static HashMap<Integer,Integer> tm2count = new HashMap<Integer,Integer>(); // key is tmNo and value is count is number of these proteins in clusters < 15
	


	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Running...");
		System.out.println("Getting all sequences and their Tms ...");
		getTMProts();
		//Initial2TMGroups();
		System.out.println("Processing clusters ...");
		calculateClusters();
		System.out.println("Printing...");
		System.out.println();
		print();
	}
	
	private static void print() {
		// TODO Auto-generated method stub
		try{
			for (int i =1;i<=14;i++){
				System.out.println(i+"\t"+tm2count.get(i));
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void calculateClusters() {
		// TODO Auto-generated method stub
		try{

			int count = 0;
			int countp = 0;
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT cluster_id, cluster_threshold FROM clusters_mcl_nr_info2");
			
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl2 WHERE cluster_id=? and cluster_threshold=? AND redundant=\"No\"");
			ResultSet rs = pstm1.executeQuery();
			while(rs.next()){
				int clusid = rs.getInt(1);
				float thre = rs.getFloat(2);
				ArrayList<Integer> seqIds = new ArrayList<Integer>(); 
				
				pstm2.setInt(1, clusid);		
				pstm2.setFloat(2, thre);
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()) {	//get cluster members from clusters_mcl2 -- sequence ids for this particular clusterid and threshold
					// while traversing through these this.start - this.length  
					int seqid = rs2.getInt("sequenceid");
					seqIds.add(seqid);					
				}
				rs2.close();
				rs2 = null;
				if(seqIds.size()<=15){
					count ++;
					for(int i =0;i<=seqIds.size()-1;i++){
						countp++;
						int id = seqIds.get(i);
						
						int tm = allProtsMap.get(id);
						if(tm >=14){
							tm =14;
						}
						
						if (tm2count.containsKey(tm)){
							int c = tm2count.get(tm);
							c++;
							tm2count.put(tm, c);
						}
						else{
							tm2count.put(tm, 1);
						}
					}
				}
			}
			rs.close();
			pstm1.close();
			System.out.print("Clusters in consideration: "+ count);
			System.out.print("Proteins counted: "+ countp);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
/*
	private static void Initial2TMGroups() { // organizes the proteins according to their TM groups
		// TODO Auto-generated method stub
		try{
			for(int i =0;i<=allprots.size()-1;i++){

				int seqid = allprots.get(i);

				int tmNo = allProtsMap.get(seqid);
				if(tmNo>=14){
					tmNo =14;
				}

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
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	*/
	private static void getTMProts() {
		// return the HashTable of proteins with given tmNo
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select distinct(sequenceid) from sequences2");
			ResultSet rs = pstm1.executeQuery();
			int x = 0;
			while(rs.next()){
				x++;
				int seqid = rs.getInt(1);
				int tm = TmDistribution2.getTmNo(seqid); // returns the number of tm in a protein
				allProtsMap.put(seqid, tm);
				allprots.add(seqid);
				if(x%1000==0){
					System.out.println(x);
					System.out.flush();
				}
			}
			rs.close();
			pstm1.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
