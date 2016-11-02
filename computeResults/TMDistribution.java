package computeResults;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;

import utils.DBAdaptor;

public class TMDistribution {

	/**
	 * @param args
	 * this class is made to get the TM protein distribution in the the initial data set and the SC cluster
	 * SO,
	 * the number of 1TM in SC
	 * the number of 2TM in SC , and so on
	 * avg length of nTM protein in CAMPS - where n is any number of TM segment
	 * 
	 * ALSO
	 * // get the number of single TM proteins in initial set and their avg length
		// get the number of single TM proteins in SC  and their avg length
	 */

	// For initial data set
	private static Hashtable<Integer,Integer> tmInit = new Hashtable<Integer,Integer>();	// Key is the number of segments of TM and Value is the count of such proteins
	private static int tmInitLarge =0;	// protien with tm segments >13 in initial set
	
	public static ArrayList<Integer> allProteins = new ArrayList<Integer>(); // all the proteins in SC clusters
	
	public static Hashtable<Integer,Integer> Initial_sequences = new Hashtable<Integer,Integer>();	// for all the sequences in Initial Set
	// key is sequenceid, value is number of TM
	private static Hashtable<Integer,Integer> Sequences_length = new Hashtable<Integer,Integer>();	// for all the sequences in Initial Set
	// key is sequenceid, value is lenght of sequence
	private static Hashtable<Integer,Integer> Loop_lengths = new Hashtable<Integer,Integer>();	// for all the sequences in Initial Set
	// key is sequenceid, value is total loop lengths
	private static Hashtable<Integer,Integer> TM_lengths = new Hashtable<Integer,Integer>();	// for all the sequences in Initial Set
	// key is sequenceid, value is total TM lengths
	
	//For SC 
	private static Hashtable<Integer,Integer> tmSC = new Hashtable<Integer,Integer>();	// key is the number of segments of TM and Value is the count of such proteins
	private static int tmSCLarge =0;	// protien with tm segments >13 in initial set
	private static Hashtable<Integer,Integer> Loop_SC = new Hashtable<Integer,Integer>(); // key is the number of segments of TM and Value is the sum of loops residues
	private static Hashtable<Integer,Integer> TM_len_SC = new Hashtable<Integer,Integer>();
	
	private static ArrayList<Integer> singleTmProts = new ArrayList<Integer>();
	
	private static int LoopLargeLen =0;	// >13
	private static int TMLargeLen =0;
	
	private static int TMTotalLen =0;
	private static int LoopTotalLen =0;
	
	
	private static Hashtable<Integer,Integer> SC_sequences = new Hashtable<Integer,Integer>();	// for all the sequences in SC clusters  
	// key is sequenceid, value is number of TM

	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");

	private static void print (){
		System.out.print("\nThe Results for complete CAMPS dataset are given Below: \n\n");
		for (int key : tmInit.keySet()){
			System.out.print("TM_Number:\t"+key+"\tCount:\t"+tmInit.get(key)+"\n");
		}
		System.out.print("TM_Number > 13\t" + "Count:\t"+tmInitLarge+"\n");


		System.out.print("\n\nThe Results for CAMPS SC Clusters are given Below: \n\n");
		for (int key : tmSC.keySet()){
			System.out.print("TM_Number:\t"+key+"\tCount:\t"+tmSC.get(key)+"\tLoopLength:\t"+Loop_SC.get(key)+"\tTM_Lengths:\t"+TM_len_SC.get(key)+"\n");
		}
		System.out.print("TM_Number > 13\t" + "Count:\t"+tmSCLarge+"\tLoopLength:\t"+LoopLargeLen+"\tTM_Lengths:\t"+TMLargeLen+"\n");
		System.out.print("Total LoopLength = "+LoopTotalLen+"\n");
		System.out.print("Total TMLength = "+TMTotalLen+"\n");
	}

	private static void getStat(){
		// go through all the proteins in CAMPS
		for(int key : Initial_sequences.keySet()){
			int tmNo = Initial_sequences.get(key);
			if (tmNo > 13){
				tmInitLarge ++;
			}
			else{
				if(tmInit.containsKey(tmNo)){
					int temp = tmInit.get(tmNo);
					temp++;
					tmInit.put(tmNo, temp);
				}
				else{
					tmInit.put(tmNo, 1);
				}
			}
		}

		// Go through all the proteins in SC
		for(int key : SC_sequences.keySet()){
			int tmNo = SC_sequences.get(key);
			
			int loop = Loop_lengths.get(key);
			int tmLen = TM_lengths.get(key);
			
			LoopTotalLen = LoopTotalLen + loop;
			TMTotalLen = TMTotalLen + tmLen;
			
			
			if (tmNo > 13){
				tmSCLarge ++;
				LoopLargeLen = LoopLargeLen + loop;
				TMLargeLen = TMLargeLen + tmLen;
			}
			else{
				if(tmSC.containsKey(tmNo)){
					int temp = tmSC.get(tmNo);
					temp++;
					tmSC.put(tmNo, temp);
					
					int looptemp = Loop_SC.get(tmNo);
					looptemp = looptemp + loop;
					Loop_SC.put(tmNo, looptemp);
					
					int tmlenTemp = TM_len_SC.get(tmNo);
					tmlenTemp = tmlenTemp + tmLen;
					TM_len_SC.put(tmNo, tmlenTemp);
				}
				else{
					tmSC.put(tmNo, 1);
					
					Loop_SC.put(tmNo, 1);
					TM_len_SC.put(tmNo, 1);
				}
			}
		}
	}
	
	// if onlySc is true.. then it means that fetch only those proteins which are in SC 
	public static void getInitialSet(boolean onlySc){

		try{
			PreparedStatement pstm1;
			
			if(onlySc){
				pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,length from sequences2 where in_SC=\"Yes\"");
			}
			else{
				pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,length from sequences2");
			}
			

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select max(tms_id) from tms where sequenceid=?");

			ResultSet rs1 = pstm1.executeQuery();
			int count =0;
			while(rs1.next()){
				int seqid = rs1.getInt(1);
				int len = rs1.getInt(2);
				TMDistribution.Sequences_length.put(seqid, len);

				pstm2.setInt(1, seqid);
				ResultSet rs2 = pstm2.executeQuery();
				int TMnumber = 0;
				while(rs2.next()){
					TMnumber = rs2.getInt(1);
					if(TMnumber==1){
						// add to single tm arrayList
						singleTmProts.add(seqid);
					}
				}
				Initial_sequences.put(seqid, TMnumber);
				allProteins.add(seqid);
				pstm2.clearBatch();
				rs2.close();
				count++;
				if(count %10000==0){
					System.out.print(count+"\n");
				}
			}
			pstm1.close();
			pstm2.close();
			rs1.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void getSCset(){
		// since all the sequences in SC are from initial set therefore
		// going through the initial set and getting the TM number
		// However, first get sequenceids
		try{
			System.out.print("SC set being processed\n");
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select cluster_id, cluster_threshold from cp_clusters2 where type=?");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_id=? and cluster_threshold=? ");
			pstm1.setString(1, "sc_cluster");
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				int clusid = rs1.getInt(1);
				float thresh = rs1.getFloat(2);
				pstm2.setInt(1, clusid);
				pstm2.setFloat(2, thresh);
				int seqid;
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()){
					seqid = rs2.getInt(1);
					int tmNo = Initial_sequences.get(seqid);
					SC_sequences.put(seqid, tmNo);
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

	private static void getTmAndLoop(){
		try{
			PreparedStatement pr_tm = CAMPS_CONNECTION.prepareStatement("select begin,end,length from tms where " +
					"sequenceid=? order by tms_id");

			for(int sequenceId : Sequences_length.keySet()){
				int len = Sequences_length.get(sequenceId);

				pr_tm.setInt(1, sequenceId);
				ResultSet rs_tm = pr_tm.executeQuery();
				int loopLen = 0;
				int tmLen = 0 ; 
				while(rs_tm.next()){
					int begin = rs_tm.getInt(1);
					int end = rs_tm.getInt(2);
					int t = end - begin;
					tmLen = tmLen + t; 
				}
				loopLen = len - tmLen;
				rs_tm.close();
				Loop_lengths.put(sequenceId, loopLen);
				TM_lengths.put(sequenceId, tmLen);
				
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*
		System.out.print("Getting Initial Set\n");
		// get the sequenceids in initial Set
		// get the sequences in SC
		getInitialSet();
		getTmAndLoop();

		System.out.print("Getting SC Set\n");
		getSCset();

		System.out.print("Calculating Stats\n");
		//Now have both hashtables qith sequenceids and their TM no... Now to count simple
		getStat();
		print();
		*/
		// get the number of single TM proteins in initial set and their avg length
		// get the number of single TM proteins in SC  and their avg length
		getInitialSet(false); // false so that all proteins are fetched... no only those which are in SC
		getTmAndLoop();
		calcSingleTms();
		getStat();
		print();
		
	}

	private static void calcSingleTms() {
		// TODO Auto-generated method stub
		// Sequences_length -- seqid key and length value
		// Initial_sequences -- seqidKey and maxTM No is value
		Float avgLenInitial = 0f;
		Float avgLenSc = 0f;
		Integer countSCsingle = 0;
		// get all the single TM proteins from the hash
		
		// for the initial set
		for(int i =0;i<=singleTmProts.size()-1;i++){
			int id = singleTmProts.get(i);
			avgLenInitial = avgLenInitial + Sequences_length.get(id);
		}
		avgLenInitial = (avgLenInitial / singleTmProts.size());
		
		System.out.print("\nNumber of Single TM proteins in initial Set "+singleTmProts.size()+"\n");
		System.out.print("\nAvg Len of Single TM proteins in initial Set "+avgLenInitial+"\n");
		
		//for the SC
		getSCset();
		// for all the proteins in SC_sequences
		
		for(Integer key : SC_sequences.keySet()){
			if (singleTmProts.contains(key)){
				// is in SC and is single TM
				avgLenSc = avgLenSc + Sequences_length.get(key);
				countSCsingle ++;
			}
			avgLenSc = (avgLenSc / countSCsingle); 
		}
		System.out.print("\nNumber of Single TM proteins in SC "+countSCsingle+"\n");
		System.out.print("\nAvg Len of Single TM proteins in SC Set "+avgLenSc+"\n");
		
		
		
	}


}
