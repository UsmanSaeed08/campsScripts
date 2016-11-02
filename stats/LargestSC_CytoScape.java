package stats;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import utils.DBAdaptor;

public class LargestSC_CytoScape {

	/**
	 * @param args
	 * Input the cluster code and it would make the file at given path for cytoscape
	 * uses the table clusters_mcl and alignments_initial. 
	 */
	
	public static HashMap <Integer, Integer> SC_clusters = new HashMap<Integer, Integer>();// list of proteins in 5 largest SC
	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");
	
	private static String campsCode = "";
	private static String outPath = "";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		float h = 30;
		if(args[0].contains("-h")){
			System.out.println("Input the cluster code and it would make the file at given path for cytoscape");
			System.out.println("uses the table clusters_mcl and alignments_initial.");
			System.out.println();
			System.out.println("param 0--> CAMPS clusterCode");
			System.out.println("param 1--> minimum identity threshold of similarity");
			System.out.println("param 2--> outFile Path ending with /");
		}
		else{
		if(args[0].isEmpty() || args[1].isEmpty()){
			System.err.println("Wrong Input.. Exit");
			System.exit(0);
		}
		else{
			campsCode = args[0].trim();
			h = Float.parseFloat(args[1]);
			outPath = args[2].trim();
		}
			
			
		System.out.print("\nRunning..\n");
		System.out.print("\nGetting sequences\n");
		// CMSC0001
		// CMSC0007
		// CMSC0002
		// CMSC0003
		// CMSC0005
		
		//String clusid = "CMSC0001";
		// these are the five largest structural clusters
		
		//getSequences("CMSC0001"); // gets the sequences for given strcutural custer
		//getSequences("CMSC0007");
		//getSequences("CMSC0002");
		//getSequences("CMSC0003");
		//getSequences("CMSC0005");
		//

		//getSequences("CMSC0546");
		//getSequences("CMSC0577");
		//getSequences("CMSC0484");
		//CMSC0494
		//getSequences("CMSC0494");
		getSequences(campsCode);
		System.out.println("\nWriting scores to file...\n");
		WriteScores(h);
	
		closeConnection();
		System.out.println("\nDone\n");
		}
	}
	/**
	 * Takes the minimum identity as input... so keeps only those relations with identity above a threshold
	 */
	
	private static void WriteScores(float x) {
		// TODO Auto-generated method stub
		try{
			//BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/Scratch/cytoscapeFiles/SC_CLustersScoresForCytoscape")));
			//BufferedWriter bw = new BufferedWriter(new FileWriter(new File("/home/users/saeed/SC_CLustersScoresForCytoscape")));
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outPath+"SC_CLustersScoresForCytoscape_"+campsCode)));
			//source	target	interaction	directed	value
			bw.write("Source\tTarget\tInteraction\tDirected\tValue");
			bw.newLine();
			
			int idx =1;
			for (int i=1; i<=17;i++){
				PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select " +
						"seqid_query,seqid_hit,identity,evalue from alignments_initial limit "+idx+","+10000000);
				idx = i*10000000;
				//System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
				int setsize = 0;
				ResultSet rs = pstm.executeQuery();
				while (rs.next()){
					setsize++;
					int q = rs.getInt(1);
					int h = rs.getInt(2);
					float identity = rs.getFloat(3);
					double eval = rs.getDouble(4);
					if( q!=h && identity>x && eval < 1.0E-5){
						if(SC_clusters.containsKey(q) && SC_clusters.containsKey(h)){
							bw.write(q+"\t"+h+"\t"+"Identity"+"\t"+"TRUE"+"\t"+identity);
							bw.newLine();
						}
					}
				}
				pstm.close();
				rs.close();
				System.out.print("\n Hashtable of Alignments "+idx+", the set size was:"+setsize+"\n");
			}
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
	private static void getSequences(String code) {
		// TODO Auto-generated method stub
		try{
			PreparedStatement p = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters where code=?");
			p.setString(1, code);
			ResultSet r = p.executeQuery();
			int clusId = 0;
			float clus_thresh = 0f;
			while(r.next()){
				clusId = r.getInt(1);
				clus_thresh = r.getFloat(2);
			}
			p.close();
			r.close();
			
			// now get the sequences from clusters_mcl
			PreparedStatement p2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl where cluster_id=? and cluster_threshold=? and redundant=\"No\"");
			p2.setInt(1, clusId);
			p2.setFloat(2, clus_thresh);
			
			ResultSet r2 = p2.executeQuery();
			int seqId = 0;
			while(r2.next()){
				seqId = r2.getInt(1);
				if(!SC_clusters.containsKey(seqId)){
					SC_clusters.put(seqId, 0);
				}
			}
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void closeConnection() {
		// TODO Auto-generated method stub
		try{
			CAMPS_CONNECTION.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
