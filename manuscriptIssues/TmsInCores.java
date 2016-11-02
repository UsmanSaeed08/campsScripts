package manuscriptIssues;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import utils.DBAdaptor;

public class TmsInCores {

	/**
	 * @param args
	 */
	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");
	private static HashMap<Integer,Integer> seqidsCores = new HashMap<Integer,Integer>(); // seqids to tmNo -- specially for the sequences in tmCores
	private static ArrayList<Integer> allprots = new ArrayList<Integer>(); // sequenceids
	private static Hashtable<Integer,ArrayList<Integer>> tmNotoSeqids = new Hashtable<Integer,ArrayList<Integer>>();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("Checks the exact number of sequences in tmCores and SC");
		 //cores();
		scClusts();
		 Initial2TMGroups();
		 print();
		 
		
	}
private static void print() {
		// TODO Auto-generated method stub
		try{
			for(int i =1;i<=14;i++){
				System.out.println(i+"\t"+tmNotoSeqids.get(i).size());
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	
	}

	private static void scClusts() {
		// TODO Auto-generated method stub
		try{
			//HashMap<Integer,Integer> seqidsCores = new HashMap<Integer,Integer>(); // 
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters2 where type=\"sc_cluster\"");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_id=? and cluster_threshold=?");
			ResultSet rs = pstm1.executeQuery();
			while(rs.next()){
				int id = rs.getInt(1);
				float thresh = rs.getFloat(2);
				
				pstm2.setInt(1, id);
				pstm2.setFloat(2, thresh);
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()){
					int seqid = rs2.getInt(1);
					if(!seqidsCores.containsKey(seqid)){
						int tm = TmDistribution2.getTmNo(seqid);
						seqidsCores.put(seqid, tm);
						allprots.add(seqid);
					}
				}
				System.out.println("In SC: -->    id: "+id +"\t thresh"+thresh);
			}
			System.out.println("Sequences in sc_CLusters : "+ seqidsCores.size());
		
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private static void Initial2TMGroups() { // organizes the proteins according to their TM groups
		// TODO Auto-generated method stub
		try{
			for(int i =0;i<=allprots.size()-1;i++){

				int seqid = allprots.get(i);

				int tmNo = seqidsCores.get(seqid);
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

	private static void cores() {
		// TODO Auto-generated method stub
		try{
			
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select distinct cluster_id from tms_cores2 where cluster_threshold=100");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_id=? and cluster_threshold=?");
			ResultSet rs = pstm1.executeQuery();
			while(rs.next()){
				int id = rs.getInt(1);
				//float thresh = rs.getFloat(2);
				float thresh = 100f;
				
				pstm2.setInt(1, id);
				pstm2.setFloat(2, thresh);
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()){
					int seqid = rs2.getInt(1);
					if(!seqidsCores.containsKey(seqid)){
						int tm = TmDistribution2.getTmNo(seqid);
						seqidsCores.put(seqid, tm);
						
						allprots.add(seqid);
					}
				}
				System.out.println("id: "+id +"\t thresh"+thresh);
			}
			System.out.println("Sequences in TM cores: "+ seqidsCores.size());
		
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
