package stats;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import utils.DBAdaptor;

public class CountPdbInCAMPSSC {

	/**
	 * @param args
	 * counts the pdb sequences in camps sc clusters
	 */
	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//run(); uses the table clusters_mcl_structures
		run2(); //uses original table of other_databases
	}
	
	private static void run2(){
		try{
			// first just get all the sequences in other db table;
			PreparedStatement pstmx = CAMPS_CONNECTION.prepareStatement("select distinct(sequenceid) from other_database where db=\"pdbtm\" and ident>=30");
			ResultSet rsx = pstmx.executeQuery();
			HashMap <Integer,Integer> idsWithStruct = new HashMap<Integer,Integer>();
			System.out.println("Fetching seqIds with Pdbs...");
			System.out.flush();
			while(rsx.next()){
				idsWithStruct.put(rsx.getInt(1), null);
			}
			rsx.close();
			pstmx.close();
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters2 where type=\"sc_cluster\"");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_id= ? and cluster_threshold=?");
			
			HashMap <String,String> scClust = new HashMap<String,String>();
			//HashMap <String,String> pdbs = new HashMap<String,String>();
			
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				int id = rs1.getInt(1);
				float thre = rs1.getFloat(2);
				
				pstm2.setInt(1, id);
				pstm2.setFloat(2, thre);
				System.out.println(id+"_"+thre+" -- Processing");
				System.out.flush();
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()){
					int seqid = rs2.getInt(1);
					if(idsWithStruct.containsKey(seqid)){
						if(!scClust.containsKey(id+"_"+thre)){
							scClust.put(id+"_"+thre,null);
							break;
						}
					}
				}
				rs2.close();
				pstm2.clearParameters();
			}
			rs1.close();
			pstm1.close();
			System.out.println("Number of clusters with pdb structures: "+ scClust.size());
			//System.out.println("Number of pdb structures in SC: "+ pdbs.size());
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void run(){
		try{
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters where type=\"sc_cluster\"");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select pdbid from clusters_mcl_structures where cluster_id= ? and cluster_threshold=?");
			HashMap <String,String> scClust = new HashMap<String,String>();
			HashMap <String,String> pdbs = new HashMap<String,String>();
			
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				int id = rs1.getInt(1);
				float thre = rs1.getFloat(2);
				
				pstm2.setInt(1, id);
				pstm2.setFloat(2, thre);
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()){
					String name = rs2.getString(1);
					if(!scClust.containsKey(id+"_"+thre)){
						scClust.put(id+"_"+thre,null);
					}
					if(!pdbs.containsKey(name)){
						pdbs.put(name,null);
					}
				}
				rs2.close();
				pstm2.clearParameters();
			}
			rs1.close();
			pstm1.close();
			System.out.println("Number of clusters with pdb structures: "+ scClust.size());
			System.out.println("Number of pdb structures in SC: "+ pdbs.size());
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
