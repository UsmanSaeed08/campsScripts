package stats;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

import utils.DBAdaptor;

public class CAMPSandSuperfamilyMapping {

	/**
	 * @param args
	 */
	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");
	public static HashMap <String, Integer> pdb2camps = new HashMap<String, Integer>();
	public static ArrayList <String> pdbIds = new ArrayList<String>();
	public static ArrayList <Integer> CampsSeqIds = new ArrayList<Integer>();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Lets Start");

		get_camps2pdb();
		check();

	}

	private static void check() {
		// TODO Auto-generated method stub
		try{
			//CampsSeqIds
			PreparedStatement prepst;
			ResultSet r = null;
			prepst = CAMPS_CONNECTION.prepareStatement("select sequenceid,accession from domains_superfamily");
			HashMap <Integer, Integer> counterSuperfamilyDomains = new HashMap<Integer, Integer>(); 
			//key seqid and value is number of superfamily accessions
			r = prepst.executeQuery();
			while(r.next()){
				int sqid = r.getInt(1);
				String acc = r.getString(2);
				if (CampsSeqIds.contains(sqid)){
					if(counterSuperfamilyDomains.containsKey(sqid)){
						int n = counterSuperfamilyDomains.get(sqid);
						n = n+1;
						counterSuperfamilyDomains.put(sqid, n);
					}
					else{
						counterSuperfamilyDomains.put(sqid, 1);
					}
				}
			}
			// print
			for(int i =0;i<=CampsSeqIds.size()-1;i++){
				int id = CampsSeqIds.get(i);
				// seq id and number of of acc
				System.out.println(id + "\t"+ counterSuperfamilyDomains.get(id));
			}
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}

	private static void get_camps2pdb() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement prepst;
			ResultSet r = null;
			prepst = CAMPS_CONNECTION.prepareStatement("select pdb_name,sequenceid from camps2pdb");
			r = prepst.executeQuery();
			while(r.next()){
				String pdb_name = r.getString(1);
				int seq = r.getInt(2);
				// use this if i need sequences of camps... first make a hashmap and then un comment below
				//if(!InCamps.containsKey(pdb_name)){
				//	InCamps.put(pdb_name, "");
				//}
				if (!pdb2camps.containsKey(pdb_name)){
					pdb2camps.put(pdb_name,seq);
					pdbIds.add(pdb_name);
					CampsSeqIds.add(seq);
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
