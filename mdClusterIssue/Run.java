package mdClusterIssue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import utils.DBAdaptor;
import workflow.Dictionary;

public class Run {

	/**
	 * @param args
	 * 
	 * This whole package deals with the md clusters issue....
	 * Issue: there are 2123 MD clusters in CAMPS3 whereas in CAMPS2 > 22k. Why is this number so low in camps3.
	 * Already done: 
	 * 1. checked the errors in scripts.. did not find any errors or anything...
	 * 2. reran the md cluster generation script and got the same number of md clusters
	 * 3. the reason i gave was that sc clusters in CAMPS 3 have highly homologous protein members. -- proof is needed
	 * So writing this code below
	 * It gets 5 biggest and smallest camps clusters in both Camps3 and camps2. 
	 * it then investigates the identity percentage in these clusters and number of md clusters produced by each. 
	 *   
	 */

	private static Connection CAMPS3_CONNECTION = DBAdaptor.getConnection("CAMPS4");
	private static Connection CAMPS2_CONNECTION = DBAdaptor.getConnection("CAMPS3");

	private static HashMap<String, Integer> cmp3 = new HashMap<String, Integer>(); //cluster code and its size
	private static HashMap<String, Integer> cmp2 = new HashMap<String, Integer>();

	private static HashMap<String, Integer> cmp3CodeToNumberMD = new HashMap<String, Integer>();
	private static HashMap<String, Integer> cmp2CodeToNumberMD = new HashMap<String, Integer>();

	private static HashMap<String, ArrayList<Integer>> cmp3CodeToMembers = new HashMap<String, ArrayList<Integer>>();
	private static HashMap<String, ArrayList<Integer>> cmp2CodeToMembers = new HashMap<String, ArrayList<Integer>>();

	private static HashMap<Integer, Integer> Allcamps3Sequences = new HashMap<Integer, Integer>();

	private static HashMap<String, Float> cmp3codeToIdentity = new HashMap<String, Float>();
	// maxes for camps3
	private static int max1 =0;
	private static String  max1Code = "";
	private static int max2 =0;
	private static String  max2Code = "";
	private static int max3 =0;
	private static String  max3Code ="";
	//maxes for camps2
	private static int max1camp2 =0;
	private static String  max1Codecamp2 = "";
	private static int max2camp2 =0;
	private static String  max2Codecamp2 = "";
	private static int max3camp2 =0;
	private static String  max3Codecamp2 ="";

	private static Hashtable<Integer, Dictionary> dict = new Hashtable<Integer, Dictionary>(); // alignments table values

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// prerequiisite --> sorted files for both sc clusters in camps3 and camps2... was done by using 
		// package stats.SC_clusterTable
		System.out.print("Whats Up?\n");
		// reads the file and gets the biggest/smallest clusters
		// n is the number of clusters required... b for biggest and s for smallest
		int n = 15;
		///home/users/saeed
		//String camps3file = "F:/SC_Cluster_table.txt";
		//String camps2file = "F:/SC_Cluster_tableCamps2.txt";

		String camps3file = "/home/users/saeed/SC_Cluster_table.txt";
		String camps2file = "/home/users/saeed/SC_Cluster_tableCamps2.txt";
		//	**********************Do below to run for max md cluster giving clusters
		//ArrayList<String> Camps3 = getScWithMostMD(camps3file,true);
		//ArrayList<String> Camps2 = getScWithMostMD(camps2file,false);
		//**************************************************************************************

		//**********************Do below to run for largest and smallest sc cluster		
		ArrayList<String> Camps3= GetScClusters(n,camps3file, true); //first n number of smallest codes.. then n number of biggest
		ArrayList<String> Camps2= GetScClusters(n, camps2file, false);
		//**************************************************************************************
		System.out.print("Have fetched the biggest and smallest clusters in both camps3 and 2\n");
		GetChildMDs(Camps3,true);
		GetChildMDs(Camps2,false);
		// also got childs and members... now calculate avg identity for camps3
		System.out.print("Have fetched the Number of MDs in both camps3 and 2 for sc\n");
		System.out.print("Fetching alignment scores\n");
		populate_alignments();
		System.out.print("Calculating Identity \n");
		CalculatePercentIdentity(Camps3);
		System.out.print("Print Results \n");
		printResult(Camps3,Camps2);
	}

	private static ArrayList<String> getScWithMostMD(String file,
			boolean b) {
		// TODO Auto-generated method stub
		try{
			ArrayList<String> clusters = new ArrayList<String>();
			ArrayList<String> maxes;
			
			BufferedReader br2 = new BufferedReader(new FileReader(new File(file)));
			String l = "";
			while((l = br2.readLine())!=null){
				String[] pr = l.split("\t");				
				String clusName = pr[0];
				clusters.add(clusName);
				Integer clusSize = Integer.parseInt(pr[2]);
				if(b){
					cmp3.put(clusName, clusSize);
				}
				else{
					cmp2.put(clusName, clusSize);
				}
			}
			br2.close();

			PreparedStatement pstm;
			PreparedStatement pstmMD; 
			if(b){
				pstm = CAMPS3_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters" +
						" where code = ?");
				pstmMD = CAMPS3_CONNECTION.prepareStatement("select code from cp_clusters where " +
						"super_cluster_id=? and super_cluster_threshold =? and type=\"md_cluster\"");
			}
			else{
				pstm = CAMPS2_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters" +
						" where code = ?");
				pstmMD = CAMPS2_CONNECTION.prepareStatement("select code from cp_clusters where " +
						"super_cluster_id=? and super_cluster_threshold =? and type=\"md_cluster\"");
			}
			for(int i = 0; i<=clusters.size()-1;i++){
				String code = clusters.get(i);
				pstm.setString(1, code);
				ResultSet rs = pstm.executeQuery();
				while(rs.next()){
					Integer clusId = rs.getInt(1);
					Float clusthresh = rs.getFloat(2);
					pstmMD.setInt(1, clusId);
					pstmMD.setFloat(2,clusthresh);
					ResultSet rs2 = pstmMD.executeQuery();
					int noOfMD = 0;
					while(rs2.next()){
						noOfMD ++;
					}
					rs2.close();
					if(b)
						cmp3CodeToNumberMD.put(code, noOfMD);
					else
						cmp2CodeToNumberMD.put(code, noOfMD);
				}
				rs.close();
			}
			// get the top 3 clusters with max number of mdclusters camps 3
			System.out.print("Getting max\n");
			if(b){
				maxes = new ArrayList<String>();
				for(int x= 0;x<=clusters.size()-1;x++){
					String code = clusters.get(x);
					int mdCount = cmp3CodeToNumberMD.get(code);

					if (mdCount > max1)
					{
						max3 = max2; max2 = max1; max1 = mdCount;
						max3Code = max2Code; max2Code = max1Code; max1Code = code; 
					}
					else if (mdCount > max2)
					{
						max3 = max2; max2 = mdCount;
						max3Code = max2Code; max2Code = code;
					}
					else if (mdCount > max3)
					{
						max3 = mdCount;
						max3Code = code;
					}
				}
				maxes.add(max1Code);
				maxes.add(max2Code);
				maxes.add(max3Code);
			}
			else{
				maxes = new ArrayList<String>();
				for(int x= 0;x<=clusters.size()-1;x++){
					String code = clusters.get(x);
					int mdCount = cmp2CodeToNumberMD.get(code);
					
					if (mdCount > max1camp2)
				    {
				        max3camp2 = max2camp2; max2camp2 = max1camp2; max1camp2 = mdCount;
				        max3Codecamp2 = max2Codecamp2; max2Codecamp2 = max1Codecamp2; max1Codecamp2 = code; 
				    }
				    else if (mdCount > max2camp2)
				    {
				        max3camp2 = max2camp2; max2camp2 = mdCount;
				        max3Codecamp2 = max2Codecamp2; max2Codecamp2 = code;
				    }
				    else if (mdCount > max3camp2)
				    {
				        max3camp2 = mdCount;
				        max3Codecamp2 = code;
				    }
				}
				maxes.add(max1Codecamp2);
				maxes.add(max2Codecamp2);
				maxes.add(max3Codecamp2);
			}
			return maxes;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	private static void printResult(ArrayList<String> camps3,
			ArrayList<String> camps2) {
		// TODO Auto-generated method stub
		//camps3_code size no_of_md identity camps2_code size no_of_md 
		for(int i = 0;i<=camps3.size()-1;i++){
			String code3 = camps3.get(i);
			String code2 = camps2.get(i);
			//cmp3CodeToNumberMD
			System.out.print(code3+"\t"+cmp3.get(code3)+"\t"+cmp3CodeToNumberMD.get(code3)+"\t"+cmp3codeToIdentity.get(code3)
					+"\t"+code2+"\t"+cmp2.get(code2)+"\t"+cmp2CodeToNumberMD.get(code2)+"\n");		
		}
	}

	private static void CalculatePercentIdentity(ArrayList<String> clusters) {
		// TODO Auto-generated method stub
		try{
			for(int x =0;x<=clusters.size()-1;x++){
				String scCluster = clusters.get(x);
				ArrayList<Integer> clusterMembers = cmp3CodeToMembers.get(scCluster);
				HashMap<Integer,Integer> clusterMembersHighIdentity = new HashMap<Integer,Integer>();

				for (int i =0; i<=clusterMembers.size()-1;i++){
					int query = clusterMembers.get(i);
					if (dict.containsKey(query)){
						Dictionary temp = dict.get(query);
						for(int j =1;j<=clusterMembers.size()-1;j++){
							int hit = clusterMembers.get(j);
							if(temp.protein.contains(hit)){
								int idx = temp.protein.indexOf(hit);
								float identity = temp.score.get(idx);
								if(query!=hit){ // selfhit
								if(identity >= 40) {
									if (!clusterMembersHighIdentity.containsKey(query) && !clusterMembersHighIdentity.containsKey(hit)){// adding the new and part
										clusterMembersHighIdentity.put(query,0);
										clusterMembersHighIdentity.put(hit,0);
									}/*
									if (!clusterMembersHighIdentity.containsKey(hit)){
										clusterMembersHighIdentity.put(hit,0);
									}*/

								}
								}
							}
						}
					}
				}
				//cmp3codeToIdentity
				int numMembers =  clusterMembers.size(); 
				int threshold = (int) Math.round(((numMembers-1) * 80)/((double) 100));
				if (clusterMembersHighIdentity.size() >= threshold){
					System.out.print("Yes: "+scCluster+"    "+threshold+"\n");
				}
				else{
					System.out.print("No: "+scCluster+"    "+threshold+"\n");
				}
				
				Float originalSize = (float) clusterMembers.size();
				Float afterIdentitySize = (float) clusterMembersHighIdentity.size();
				Float perc = (afterIdentitySize/originalSize)*100f; 
				cmp3codeToIdentity.put(scCluster, perc);
			}






		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void populate_alignments(){ // fetches only those scores which are required
		try{	
			// make a hashmap with key as seqid_query and value is seqid_hit and identity value.
			// Can use the Dictionary class, as it is exactly what is required.
			PreparedStatement pstm = null;
			int idx =1;
			for (int i=1; i<=17;i++){				
				pstm = CAMPS3_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial limit "+idx+","+10000000);
				//pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial");
				idx = i*10000000;
				System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
				ResultSet rs = pstm.executeQuery();
				while(rs.next()) {
					int idhit = rs.getInt("seqid_hit");
					int query = rs.getInt ("seqid_query");
					float identity = rs.getFloat("identity");
					if (Allcamps3Sequences.containsKey(idhit) && Allcamps3Sequences.containsKey(query)){
						if (dict.containsKey(query)){ // the key already exists...so that you dont delete the previous info of the key
							Dictionary temp = (Dictionary)dict.get(query);
							temp.set(idhit, identity);
							dict.put(query,temp);
						}
						else if (dict.containsKey(idhit)){
							Dictionary temp = (Dictionary)dict.get(idhit);
							temp.set(query, identity);
							dict.put(idhit,temp);
						}
						else{
							Dictionary temp = new Dictionary(idhit, identity);
							dict.put(query,temp);
						}
					}
				}
				// dict population complete
				rs.close();
			}
			pstm.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void GetChildMDs(ArrayList<String> camps, boolean b) {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm = null;
			PreparedStatement pstmMD = null;
			PreparedStatement pstmGetMembers = null;
			if(b){
				pstm = CAMPS3_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters" +
						" where code = ?");
				pstmMD = CAMPS3_CONNECTION.prepareStatement("select cluster_id,cluster_threshold,code from cp_clusters where " +
						"super_cluster_id=? and super_cluster_threshold =? and type=\"md_cluster\"");
				pstmGetMembers = CAMPS3_CONNECTION.prepareStatement("select sequenceid from clusters_mcl where " +
						"cluster_id=? and cluster_threshold =?");

				for(int i =0; i<=camps.size()-1;i++){
					pstm.setString(1, camps.get(i));
					ResultSet rs = pstm.executeQuery();
					while(rs.next()){
						Integer clusId = rs.getInt(1);
						Float clusthresh = rs.getFloat(2);
						pstmMD.setInt(1, clusId);
						pstmMD.setFloat(2,clusthresh);
						ResultSet rs2 = pstmMD.executeQuery();
						int noOfMD = 0;
						while(rs2.next()){
							noOfMD ++;
						}
						rs2.close();

						cmp3CodeToNumberMD.put(camps.get(i), noOfMD);
						// now get member sequences
						pstmGetMembers.setInt(1, clusId);
						pstmGetMembers.setFloat(2,clusthresh);
						ResultSet rs4 = pstmGetMembers.executeQuery();
						ArrayList<Integer> temp = new ArrayList<Integer>();
						while(rs4.next()){
							int seqid = rs4.getInt(1);
							Allcamps3Sequences.put(seqid, 0);
							temp.add(seqid);
						}
						rs4.close();
						cmp3CodeToMembers.put(camps.get(i), temp);
					}
					rs.close();
				}
				pstm.close();
				pstmMD.close();
				pstmGetMembers.close();
			}
			else{
				//pstm = CAMPS2_CONNECTION.prepareStatement("");
				pstm = CAMPS2_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters" +
						" where code = ?");
				pstmMD = CAMPS2_CONNECTION.prepareStatement("select cluster_id,cluster_threshold,code from cp_clusters where " +
						"super_cluster_id=? and super_cluster_threshold =? and type=\"md_cluster\"");
				pstmGetMembers = CAMPS3_CONNECTION.prepareStatement("select sequences_sequenceid from clusters_mcl where " +
						"cluster_id=? and cluster_threshold =?");

				for(int i =0; i<=camps.size()-1;i++){
					pstm.setString(1, camps.get(i));
					ResultSet rs = pstm.executeQuery();
					while(rs.next()){
						Integer clusId = rs.getInt(1);
						Float clusthresh = rs.getFloat(2);
						pstmMD.setInt(1, clusId);
						pstmMD.setFloat(2,clusthresh);
						ResultSet rs2 = pstmMD.executeQuery();
						int noOfMD = 0;
						while(rs2.next()){
							noOfMD ++;
						}
						rs2.close();

						cmp2CodeToNumberMD.put(camps.get(i), noOfMD);
						// now get member sequences
						pstmGetMembers.setInt(1, clusId);
						pstmGetMembers.setFloat(2,clusthresh);
						ResultSet rs3 = pstmMD.executeQuery();
						ArrayList<Integer> temp = new ArrayList<Integer>();
						while(rs3.next()){
							int seqid = rs3.getInt(1);
							temp.add(seqid);
						}
						rs3.close();
						cmp2CodeToMembers.put(camps.get(i), temp);
					}
					rs.close();
				}
				pstm.close();
				pstmMD.close();
				pstmGetMembers.close();
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static ArrayList<String> GetScClusters(int n,String file,boolean cm3) {
		// TODO Auto-generated method stub
		// boolean cm3 = true if camps 3 else camps 2
		ArrayList<String> clusters = new ArrayList<String>();
		try{
			// get 
			// get biggest
			BufferedReader br = new BufferedReader(new FileReader(new File(file)));
			String l;
			int NumberOfLines = 0;
			while((l = br.readLine())!=null){
				NumberOfLines ++;
			}
			br.close();

			BufferedReader br2 = new BufferedReader(new FileReader(new File(file)));
			l = "";
			int CurrentLine = 0;
			while((l = br2.readLine())!=null){
				CurrentLine++;
				if( CurrentLine <= n || (NumberOfLines - CurrentLine <= n) ){
					//if( CurrentLine <= n ) { // to make a test case... original condition is above
					// smallest five
					String[] pr = l.split("\t");
					Integer clusSize = Integer.parseInt(pr[2]);
					String clusName = pr[0];
					clusters.add(clusName);
					if(cm3){
						cmp3.put(clusName, clusSize);
					}
					else{
						cmp2.put(clusName, clusSize);
					}
				}
			}
			br2.close();
			return clusters;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

}
