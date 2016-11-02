package stats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import utils.DBAdaptor;

public class CountPdbInCAMPSSC2 {
	/**
	 * @param args
	 * After the reRun.. and new extrated pdb from simap. 
	 * this script is used to get basic stats from the mapped file
	 */
	private static HashMap<String,Integer> allpdbTM = new HashMap<String,Integer>();

	private static HashMap<Integer,Integer> sqidz = new HashMap<Integer,Integer>();
	private static HashMap<String,Integer> pdbz = new HashMap<String,Integer>();
	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("Running..");	
		String f = "F:/Scratch/reRunCAMPS_CATH_PDB/reRUNCAMPSSeqToPDB.txt";
		String pdbtm = "F:/Scratch/reRunCAMPS_CATH_PDB/pdbtm_alpha.list.txt";

		getpdbtms(pdbtm);

		// main2 uses the simap out file to make camps2pdb mapfile such that all the hits
		// are only pdbtm hits.
		//String outfile = "F:/Scratch/reRunCAMPS_CATH_PDB/reRUNCAMPSSeqToPDBTMOnly.txt";
		//main2(f,outfile);
		//System.exit(0);
		// Functions bellow can run without calling main2

		run(f); // finds the number of seqid with a pdb.. and number of pdb representing the sc
		System.out.println("Number of seqids with a pdb rep: "+sqidz.size());
		System.out.println("Number of pdbs in CAMPS SC: "+pdbz.size());
		pdbAndSCclusters();
		System.out.println("Number of seqids with a pdb rep: "+sqidz.size());
		System.out.println("Number of pdbs in CAMPS SC: "+pdbz.size());
		// finds the number of clusters with at least 1 pdb
	}
	private static void main2(String f,String out){
		try{
			//allpdbTM
			BufferedReader br = new BufferedReader(new FileReader(new File(f)));
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(out)));
			//425	3mku_B	30.2	4.78E-61	19	445	14	442	3mku Multi antimicrobial extrusion protein (Na(+)
			String l = "";
			while((l=br.readLine())!= null){
				if(!l.isEmpty()){					
					String[] parts = l.split("\t");
					String pdb = parts[1].trim();
					if(allpdbTM.containsKey(pdb)){ // if the protein is a experimentally validated tm protein structure
						bw.write(l);
						bw.newLine();
					}
				}
			}
			br.close();
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void pdbAndSCclusters() {
		// TODO Auto-generated method stub
		try{
			int count = 0;
			HashMap<String,String> countedCluster = new HashMap<String,String>();
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters2 where type=\"sc_cluster\"");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_id = ? and cluster_threshold = ?");
			ResultSet rs = pstm.executeQuery();
			int i =0;
			while(rs.next()){
				i++;
				int clusid = rs.getInt(1);
				float thres = rs.getFloat(2);
				String k = clusid+"_"+thres;
				pstm2.setInt(1, clusid);
				pstm2.setFloat(2, thres);
				ResultSet rs2 = pstm2.executeQuery();
				System.out.println("Processing cluster: "+i);
				while(rs2.next()){
					int seq = rs2.getInt(1);
					if(!countedCluster.containsKey(k)){
						if(sqidz.containsKey(seq)){
							countedCluster.put(k, "");
							count++;
							break;
						}
					}
				}
				rs2.close();
			}
			rs.close();
			System.out.println("Number of clusters with a pdb rep = "+count);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void getpdbtms(String x){
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(x)));
			String l = "";
			while((l=br.readLine())!= null){
				if(!l.isEmpty()){
					allpdbTM.put(l.trim(), null);
				}
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void run(String f){
		try{
			//425	3mku_B	30.2	4.78E-61	19	445	14	442	3mku Multi antimicrobial extrusion protein (Na(+)/
			BufferedReader br = new BufferedReader(new FileReader(new File(f)));
			String l = "";
			String last = "";
			while((l=br.readLine())!= null){
				if(!l.isEmpty()){
					last = l;
					String[] parts = l.split("\t");
					int sid = Integer.parseInt(parts[0].trim());
					String pdb = parts[1].trim();
					if(allpdbTM.containsKey(pdb)){ // if the protein is a experimentally validated tm protein structure
						if(!sqidz.containsKey(sid)){
							sqidz.put(sid, null);
						}
						if(!pdbz.containsKey(pdb)){
							pdbz.put(pdb, null);
						}
					}
				}
				else{
					System.out.println(last);
				}
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
