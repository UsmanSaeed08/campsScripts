package stats;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Hashtable;

import utils.DBAdaptor;

public class SC_clusterTable {

	/**
	 * @param args
	 */
	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");
	//private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS3");
	//private static Hashtable<Integer,String> SC = new Hashtable<Integer,String>();	// For arranging the clusters acc to size
	private static ArrayList<String> scArray = new ArrayList<String>();
	// key is size and String is data

	// the function gets all the sc_clusters from cp_clusters
	private static void run(){
		System.out.print("Running...\n");
		try{
			boolean camps3 = true; //remember to change the connection above and file name in print file
			boolean camps2 = false;
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("Select cluster_id,cluster_threshold,code,description from cp_clusters where type=?");
			PreparedStatement pstm2 = null;
			if(camps3){
				pstm2 = CAMPS_CONNECTION.prepareStatement("Select sequenceid from clusters_mcl where cluster_id=? and cluster_threshold=?");
			}
			else if(camps2){
				pstm2 = CAMPS_CONNECTION.prepareStatement("Select sequences_sequenceid from clusters_mcl where cluster_id=? and cluster_threshold=?");
			}
			pstm1.setString(1, "sc_cluster");
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				int clusId = rs1.getInt(1);
				float thresh = rs1.getFloat(2);
				int size = 0;
				String TMRange = "";
				String taxa = "";

				String code = rs1.getString(3);
				String description = rs1.getString(4);

				// Now to get size 				
				pstm2.setInt(1, clusId);
				pstm2.setFloat(2, thresh);
				ResultSet rs2 = pstm2.executeQuery();
				Hashtable<Integer,Integer> members = new Hashtable<Integer,Integer>();	// members of cluster
				while(rs2.next()){
					size++;
					int seqid = rs2.getInt(1);
					if(!members.containsKey(seqid)){
						members.put(seqid, 0);
					}
				}
				rs2.close();

				//Now to get tm-range and Taxa
				
				if(camps3==true){
					String[] temp = getTaxa(clusId,thresh).split("\t");
					TMRange = temp[0].trim();
					taxa = temp[1].trim();
					String RepStruct = getRepStruct(clusId,thresh,members);
					System.out.print(code+"\t"+description+"\t"+size+"\t"+TMRange+"\t"+taxa+"\t"+RepStruct+"\n");
					scArray.add(code+"\t"+description+"\t"+size+"\t"+TMRange+"\t"+taxa+"\t"+RepStruct);
				}
				else if(camps2==true){
					TMRange = "NotRequired";;
					taxa = "NotRequired";;
					String RepStruct = "NotRequired";
					System.out.print(code+"\t"+description+"\t"+size+"\t"+TMRange+"\t"+taxa+"\t"+RepStruct+"\n");
					scArray.add(code+"\t"+description+"\t"+size+"\t"+TMRange+"\t"+taxa+"\t"+RepStruct);
				}				
				
			}
			System.out.print("\n\nSC Size "+scArray.size());
			System.out.print("\nSorting...\n");
			sort();
			printtoFile();
			
			rs1.close();
			pstm1.close();
			pstm2.close();
			CAMPS_CONNECTION.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void printtoFile(){
		try{
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/SC_Cluster_table_temp.txt")));
		for (int i =0;i<=scArray.size()-1;i++){
			System.out.print(scArray.get(i)+"\n");
			bw.write(scArray.get(i));
			bw.newLine();
		}
		bw.close();
		System.out.print("\nResults, Sorted and Printed to F:/SC_Cluster_table.txt\n\n");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private static void sort(){
		for (int i =0;i<=scArray.size()-1;i++){
			//scArray.add(code+"\t"+description+"\t"+size+"\t"+TMRange+"\t"+taxa+"\t"+RepStruct);
			
			for(int j =0; j<=scArray.size()-2;j++){
				String ll = scArray.get(j);
				String ll_parts[] = ll.split("\t");
				int sizeJ = Integer.parseInt(ll_parts[2]);
				
				String l = scArray.get(j+1);
				String l_parts[] = l.split("\t");
				int sizeJ1 = Integer.parseInt(l_parts[2]);
				
				if (sizeJ>sizeJ1){
					//swap
					scArray.set(j, l);
					scArray.set(j+1, ll);
				}
			}
					
		}
	}

	private static String getRepStruct(int clusId, float thresh,Hashtable<Integer,Integer> mems) {
		// TODO Auto-generated method stub
		try{
			Hashtable<String,Integer> processedPdbs = new Hashtable<String,Integer>();	// For already processed pdbs in this round

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("Select sequenceid,pdb_name from camps2pdb");
			ResultSet rs = pstm.executeQuery();
			String reps = "";
			boolean found = false;
			while(rs.next()){
				int sqid = rs.getInt(1);
				String pdb = rs.getString(2);
				if(mems.containsKey(sqid)){
					if(!processedPdbs.containsKey(pdb)){
						reps = reps + pdb+",";
						processedPdbs.put(pdb, 0);
						found = true;
					}
				}
			}
			rs.close();
			pstm.close();
			if(found){
				reps = reps.trim();
				reps = reps.substring(0, reps.length()-1);
			}
			return reps;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	private static String getTaxa(int clusId, float thresh) {
		// TODO Auto-generated method stub
		try{
			String tms ="";
			String tax ="";

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT tms_range," +
					"proteins_archaea,proteins_bacteria,proteins_eukaryota,proteins_viruses " +
					"FROM clusters_mcl_nr_info WHERE cluster_id=? and cluster_threshold=?;");

			pstm3.setInt(1, clusId);
			pstm3.setFloat(2, thresh);

			ResultSet rs3 = pstm3.executeQuery();
			while(rs3.next()){
				tms = rs3.getString(1);

				int arch = rs3.getInt(2);
				int bact = rs3.getInt(3);
				int eu = rs3.getInt(4);
				int vir = rs3.getInt(5);

				if(eu > 0){
					tax = "Eu ";
				}
				if(arch > 0 || bact >0){
					tax = tax + "Pro ";
				}
				if(vir > 0){
					tax = tax + "Vir";
				}
			}
			rs3.close();
			pstm3.close();
			return tms+"\t"+tax;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("BloodStream");
		run();

	}

}
