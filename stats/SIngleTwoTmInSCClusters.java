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

import manuscriptIssues.TmDistribution2;

public class SIngleTwoTmInSCClusters {

	/**
	 * @param args
	 * the program is designed to find out the following
	 * 
	 * There are XXX and YYY  SC clusters in CAMPS 3.0 that contain proteins with one and two helices, respectively
	 * 
	 */
	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");
	
	private static HashMap<Integer, Integer> singleTm = new HashMap<Integer,Integer>();
	private static HashMap<Integer, Integer> twoTm = new HashMap<Integer,Integer>();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//scrap(); //outputs fasta file for camps cluster
		//scrap2(); // uses the percentage identity matrix and output the least and average pairwise percentage identity
		scrap3();
		//System.out.print("BloodStream Alive...");
		//run();
	}
	private static void scrap3(){ // uses the file SizeVsNoClusters to get the no of cluster below and over 500 size
		try{
			BufferedReader br  = new BufferedReader(new FileReader(new File("C:/Users/Usman Saeed/Documents/CAMPS_PaperDraft/results/metaModels/reRun_Journal/SizeVsNoClusters.txt")));
			String l = "";
			int below500 = 0;
			int over501 = 0;
			while((l = br.readLine())!=null){
				if(!l.isEmpty()){
					int size = Integer.parseInt(l.split("\t")[0]);
					int freq = Integer.parseInt(l.split("\t")[1]);
					if(size<500){
						below500 = below500+freq;
					}
					else{
						over501 = over501+freq;
					}
					
				}
			}
			br.close();
			System.out.println("Cluster below 500 size are: "+below500);
			System.out.println("Cluster over 500 size are: "+over501);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void scrap(){
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("C:/Users/Usman Saeed/Documents/CAMPS_PaperDraft/results/network/cmsc0300.fasta")));
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_id=14108 and cluster_threshold=17");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequence from sequences2 where sequenceid=?");
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				int sid = rs1.getInt(1);
				pstm2.setInt(1, sid);
				ResultSet rs2 = pstm2.executeQuery();
				String seq = "";
				while(rs2.next()){
					seq = rs2.getString(1);
				}
				bw.write(">"+sid);
				bw.newLine();
				bw.write(seq);
				bw.newLine();
				
				rs2.close();
				pstm2.clearParameters();
				
			}
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void scrap2(){
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File("C:/Users/Usman Saeed/Documents/CAMPS_PaperDraft/results/network/cmsc0300.pim")));
			String l = "";
			int countSeq = 0;// sequences are equal to the number of rows
			Float identitiesSum = 0f;
			int numberOfIdentities = 0;
			Float minIden = 100f;
			while((l=br.readLine())!=null){
				if(!l.startsWith("#") && !l.isEmpty()){
					countSeq ++;
					
					String scores = l.split(":")[1].trim();
					String score[] = scores.split(" ");
					for(int i =1;i<=score.length-1;i++){ // because at 0 is sequnceid
						if(!score[i].isEmpty()){
							String s = score[i];
							s = s.trim();
							if(!s.isEmpty()&&!s.equals(" ")){
								float ident = Float.parseFloat(s);
								identitiesSum = identitiesSum + ident;
								
								numberOfIdentities++;
								if(ident<minIden){
									minIden = ident;
								}
							}
							
						}
					}
					
					
				}
			}
			Float avgIdent = identitiesSum / numberOfIdentities;
			
			System.out.println("Number of Sequences: "+ countSeq);
			System.out.println("MinIdentity: "+ minIden);
			System.out.println("Average Identity: "+ avgIdent);
			
			
			
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void getTMProts() { // the function is a modification of getTMProts in TmDistribution4
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
				if(tm ==1 ){
					singleTm.put(seqid, tm);
				}
				else if(tm ==2){
					twoTm.put(seqid, tm);
				}
				
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
	private static void run(){
		// get the
		//proteins with single and two tm
		//getTMProts();
		// now for all sc clusters
		HashMap<String,String> singleSCs = new HashMap<String,String>();
		HashMap<String,String> twoSCs = new HashMap<String,String>();
		HashMap<String,String> threeSCs = new HashMap<String,String>();
		HashMap<String,String> fourSCs = new HashMap<String,String>();
		HashMap<String,String> fiveSCs = new HashMap<String,String>();
		HashMap<String,String> SCs6 = new HashMap<String,String>();
		HashMap<String,String> SCs7 = new HashMap<String,String>();
		HashMap<String,String> SCs8 = new HashMap<String,String>();
		HashMap<String,String> SCs9 = new HashMap<String,String>();
		HashMap<String,String> SCs10 = new HashMap<String,String>();
		HashMap<String,String> SCs11 = new HashMap<String,String>();
		//HashMap<String,String> SCs12 = new HashMap<String,String>();
		
		
		
		try{
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters2 where type=\"sc_cluster\"");
			
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 " +
					"where cluster_id=? and cluster_threshold=?");
			
			ResultSet rs = pstm1.executeQuery();
			int x = 0;
			int count_only1 = 0;
			int count_only2 = 0;
			int count_only1and2 = 0;
			
			
			while(rs.next()){
				int clusid = rs.getInt(1);
				float thre = rs.getFloat(2);
				
				pstm2.setInt(1, clusid);
				pstm2.setFloat(2, thre);
				
				ResultSet rs2 = pstm2.executeQuery();
				String k = clusid+"_"+thre;
				boolean one = false;
				boolean two = false;
				boolean three = false;
				boolean four = false;
				
				
				while(rs2.next()){
					x++;
					int seqid = rs2.getInt(1);
					int tm = TmDistribution2.getTmNo(seqid); // returns the number of tm in a protein
					
					
					//if(singleTm.containsKey(seqid)){
					if(tm==1){
						one = true;
						//String k = clusid+"_"+thre;
						if(!singleSCs.containsKey(k)){
							singleSCs.put(k, "");
						}
					}
					//if(twoTm.containsKey(seqid)){
					if(tm==2){
						two = true;
						//String k = clusid+"_"+thre;
						if(!twoSCs.containsKey(k)){
							twoSCs.put(k, "");
						}
					}
					if(tm==3){
						three = true;
						if(!threeSCs.containsKey(k)){
							threeSCs.put(k, "");
						}
					}
					if(tm==4){
						four = true;
						if(!fourSCs.containsKey(k)){
							fourSCs.put(k, "");
						}
					}
					if(tm==5){
						if(!fiveSCs.containsKey(k)){
							fiveSCs.put(k, "");
						}
					}
					if(tm==6){
						if(!SCs6.containsKey(k)){
							SCs6.put(k, "");
						}
					}
					if(tm==7){
						if(!SCs7.containsKey(k)){
							SCs7.put(k, "");
						}
					}
					if(tm==8){
						if(!SCs8.containsKey(k)){
							SCs8.put(k, "");
						}
					}
					if(tm==9){
						if(!SCs9.containsKey(k)){
							SCs9.put(k, "");
						}
					}
					if(tm==10){
						if(!SCs10.containsKey(k)){
							SCs10.put(k, "");
						}
					}
					if(tm>=11){
						if(!SCs11.containsKey(k)){
							SCs11.put(k, "");
						}
					}
					//if(tm==12){
					//	if(!SCs12.containsKey(k)){
					//		SCs12.put(k, "");
					//	}
					//}
					
					if(x%1000==0){
						System.out.println(x);
						System.out.flush();
					}
				}
				rs2.close();
				pstm2.clearBatch();
				pstm2.clearParameters();
				if(one && !two && !three && !four){
					// only one
					count_only1++;
				}
				if(two && !one && !three && !four){
					// only two
					count_only2++;
				}
				if(one && two && !three && !four){
					// only one and two
					count_only1and2++;
				}
			}
			rs.close();
			pstm1.close();
			pstm2.close();
			System.out.println();
			System.out.println("There are "+singleSCs.size()+" and "+twoSCs.size()+" SC clusters in " +
					"CAMPS 3.0 that contain proteins with one and two helices, respectively");
			System.out.println("Three: " +threeSCs.size());
			System.out.println("four: " +fourSCs.size());
			System.out.println("five: " +fiveSCs.size());
			System.out.println("six: " +SCs6.size());
			System.out.println("seven: " +SCs7.size());
			System.out.println("eight: " +SCs8.size());
			System.out.println("nine: " +SCs9.size());
			System.out.println("ten: " +SCs10.size());
			System.out.println("eleven and greater: " +SCs11.size());
			//System.out.println("twelv: " +SCs12.size());
			
			System.out.println("Clusters with one only" +count_only1);
			System.out.println("Clusters with two only" +count_only2);
			System.out.println("Clusters with one and two only" +count_only1and2);
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
