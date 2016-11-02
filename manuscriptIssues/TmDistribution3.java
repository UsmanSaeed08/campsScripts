package manuscriptIssues;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import extract_proteins.TMS;

import utils.DBAdaptor;

public class TmDistribution3 {

	/**
	 * @param args
	 */

	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");

	private static HashMap<Integer,Integer> allProtsMap = new HashMap<Integer,Integer>(); // key is seqid and value is tmNo
	private static ArrayList<Integer> allprots = new ArrayList<Integer>(); // sequenceids

	private static HashMap<Integer,Integer> allProtsAlignmentTableMap = new HashMap<Integer,Integer>(); // key is seqid and value is tmNo
	private static ArrayList<Integer> allProtsAlignmentTable = new ArrayList<Integer>(); // sequenceids

	private static Hashtable<Integer,ArrayList<Integer>> tmNotoSeqids = new Hashtable<Integer,ArrayList<Integer>>();

	private static HashMap<Integer,Integer> failed = new HashMap<Integer,Integer>(); // Proteins failed to enter Alignments table

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		///*
		String algnFile = "/localscratch/CAMPS/thresh.005.abc";
		String outfileP = "/localscratch/CAMPS/passed.out";
		String outfileF = "/localscratch/CAMPS/failed.out";
		//int tmNo = 1;
		getTMProts();
		Initial2TMGroups();
		CheckInAlignmentsLevel(algnFile);
		CountInAlignments();
		WriteTable(outfileP,outfileF); // writes the table giving proteins included or failed in alignemnts table
		//*/
		// all the above was done and sent to Prof. 2 issues were identified
		// 1. why are there some proteins in failed file with no accession
		// 2. proteins in failed file give blast hits
		// Solution 1: in order to fix the first issue, I make a new failed file, by reading and retaining the ones with uniprot id, while adding
		// alternative id and description for those without any id, using tables proteins_merged.
		//Solution 2: To solve this, I have to read complete similarity score file for the given sequenceid and see all its hits,
		// write all the hits separately for this sequence, then make another file writing as to why each hit failed.

		// Below Solving 1
		/*
		String oldFailedFile = "/localscratch/CAMPS/failed.out";
		String newFile = "/localscratch/CAMPS/nfailed.out";
		WriteFailedFile(oldFailedFile,newFile);
		*/
		// Below Solving 2
		//RunAlignmentStuff();
	}



	private static void WriteFailedFile(String oldf, String n) {
		// TODO Auto-generated method stub
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(oldf)));
			BufferedWriter bwf = new BufferedWriter(new FileWriter(new File(n)));
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select name,description from proteins_merged where sequenceid=?");
			String l = "";
			while((l=br.readLine())!=null){
				String[] p = l.split("\t");
				if(p[2].contains("-")){ // without any uniprotId
					/*
					 * 58890021        1       -       -       XGDISFEPHETVVSMEYLLGLVLALLGGSVKMQDY
					 */
					int seqid = Integer.parseInt(p[0].trim());
					String seq = p[4].trim();
					pstm1.setInt(1, seqid);
					ResultSet rs = pstm1.executeQuery();
					ArrayList<String> processed = new ArrayList<String>();
					while(rs.next()){
						String x = rs.getString(1)+"\t"+rs.getString(2);
						if(!processed.contains(x)){
							processed.add(x);

							bwf.write(p[0]+"\t"+p[1]+"\t"+x+"\t"+seq);
							bwf.newLine();
							System.out.println(p[0]+"\t"+p[1]+"\t"+x+"\t"+seq);
						}
					}
					rs.close();
					pstm1.clearBatch();
				}
				else{
					bwf.write(l);
					bwf.newLine();
				}
			}
			br.close();
			bwf.close();
			pstm1.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void WriteTable(String p,String f) {
		// TODO Auto-generated method stub
		try{
			BufferedWriter bwp = new BufferedWriter(new FileWriter(new File(p)));
			BufferedWriter bwf = new BufferedWriter(new FileWriter(new File(f)));
			System.out.println("Writing Files...");
			bwp.write("CAMPSId\tTMNumbers\tAccession\tDescription\tSequence");
			bwp.newLine();

			bwf.write("CAMPSId\tTMNumbers\tAccession\tDescription\tSequence");
			bwf.newLine();

			int tmNo = 1;
			ArrayList<Integer> singleProts = tmNotoSeqids.get(tmNo);

			for(int i = 0;i<=singleProts.size()-1;i++){
				int prot = singleProts.get(i);
				String x = getUniProtInfo(prot); // return uniprot Name and description
				if(allProtsAlignmentTableMap.containsKey(prot)){
					// write in bwp
					bwp.write(prot+"\t"+tmNo+"\t"+x);
					bwp.newLine();
				}
				else if(failed.containsKey(prot)){
					// write in bwf
					bwf.write(prot+"\t"+tmNo+"\t"+x);
					bwf.newLine();
				}
			}
			bwp.close();
			bwf.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static String getUniProtInfo(int prot) {
		// TODO Auto-generated method stub
		try{
			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select name,description from sequences2names where sequenceid=?");
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select accession,description from camps2uniprot where sequenceid=?");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequence from sequences2 where sequenceid=?");

			pstm1.setInt(1, prot);

			pstm2.setInt(1, prot);
			String seq = "";
			ResultSet rs2 = pstm2.executeQuery();
			while(rs2.next()){
				seq = rs2.getString(1).toUpperCase();
			}


			ResultSet rs = pstm1.executeQuery();
			boolean found = false;
			while(rs.next()){
				found = true;
				String acc = rs.getString(1);
				String descr = rs.getString(2);
				rs.close();
				return acc+"\t"+descr+"\t"+seq;
			}
			if(!found){
				return "-"+"\t"+"-"+"\t"+seq;
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	private static void CountInAlignments() {
		// TODO Auto-generated method stub
		try{
			System.out.println("TMNumber"+"\t"+"InitialCount"+"\t"+"CountInAlignmentsTable");
			for(int x=1;x<=14;x++){ // no of tm loop
				ArrayList<Integer> prots = tmNotoSeqids.get(x);
				int count =0;
				for(int j=0;j<=prots.size()-1;j++){
					int prot = prots.get(j);
					if(allProtsAlignmentTableMap.containsKey(prot)){
						count++;
					}
					else{
						failed.put(prot,null);
					}
				}
				System.out.println(x+"\t"+prots.size()+"\t"+count);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void CheckInAlignmentsLevel(String algnFile) {
		// TODO Auto-generated method stub
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(algnFile))); 
			String line = "";
			while((line=br.readLine())!=null){				
				String[] p = line.split(" ");
				int q = Integer.parseInt(p[0].trim());
				int h = Integer.parseInt(p[1].trim());
				if(!allProtsAlignmentTableMap.containsKey(q)){
					allProtsAlignmentTableMap.put(q,null);
					allProtsAlignmentTable.add(q);
				}
				if(!allProtsAlignmentTableMap.containsKey(h)){
					allProtsAlignmentTableMap.put(h,null);
					allProtsAlignmentTable.add(h);
				}
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void getTMProts() {
		// return the HashTable of proteins with given tmNo
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select distinct(sequenceid) from sequences2");
			ResultSet rs = pstm1.executeQuery();
			while(rs.next()){
				int seqid = rs.getInt(1);
				int tm = TmDistribution2.getTmNo(seqid); // returns the number of tm in a protein
				allProtsMap.put(seqid, tm);
				allprots.add(seqid);
			}
			rs.close();
			pstm1.close();
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

	// ********************************************* ALL the ALIGNMENT STUFF DONE BELOW **************************************
	// ********************************************* ALL the ALIGNMENT STUFF DONE BELOW **************************************
	// ********************************************* ALL the ALIGNMENT STUFF DONE BELOW **************************************
	// ********************************************* ALL the ALIGNMENT STUFF DONE BELOW **************************************
	// ********************************************* ALL the ALIGNMENT STUFF DONE BELOW **************************************



	// ********************************************* ALL the ALIGNMENT STUFF DONE BELOW **************************************
	// ********************************************* ALL the ALIGNMENT STUFF DONE BELOW **************************************
	// ********************************************* ALL the ALIGNMENT STUFF DONE BELOW **************************************


	// READ MAIN() For details


	private static final String similarityFile= "/localscratch/CampsSimilarityScores/camps_seq_file.matrix";

	private static final String outFileAllHits = "/localscratch/CampsSimilarityScores/AllHitsFound.matrix";
	private static final String outFileRejectedHits = "/localscratch/CampsSimilarityScores/RejectedHitsFound.matrix";


	private static HashMap <Integer, TMS> tms = new HashMap <Integer, TMS>();


	// variables
	private static double covered_tms_perc_query;
	private static double covered_tms_perc_hit;
	private static int covered_tms_query;
	private static int covered_tms_hit;
	private static int thisNumberofTmQuery;
	private static int thisNumberofTmHit;

	// thresholds
	//private static final int minScore = 95;
	private static final double minBitScore = 50.0;	//default
	private static final  double maxEvalue = 1E-5;
	private static final int MULTIROW_INSERT_SIZE = 10000;
	private static final float minLengthRatio = 0.5f;

	public static void Initialize(){
		covered_tms_perc_query = 0 ;
		covered_tms_perc_hit = 0 ;
		covered_tms_query = 0 ;
		covered_tms_hit = 0 ;
		thisNumberofTmQuery = 0;
		thisNumberofTmHit = 0;
	}

	private static void RunAlignmentStuff() {
		// TODO Auto-generated method stub

		int seqid = 30232302; // the one to check for hits

		System.out.println("Populating TMS...");
		populate_tms();
		System.out.println("Processsing similarity File");
		ProcessSimilarityFile(seqid);
		//System.out.println("Exception sequences: ");

	}
	private static void ProcessSimilarityFile(int seqid) { // this is the seqid to check for its hits
		// TODO Auto-generated method stub
		try{
			// *************************
			// CAUTION --- THERE IS AN ERROR IN THE SCRIPT BELOW --- HITS REJECTED BY OTHER THAN EVAL AND ALIGNMENT COVERAGE ARE 
			// NOT RELIABLE, ALTHOUGH THE HIT IS REJECTED.. WHICH IS TRUE.. BUT REJECTION REASON IS NOT RELIABLE
			// *************************
			
			StringBuilder sb = new StringBuilder();
			HashMap<Integer,Integer> sequences = new HashMap<Integer,Integer>();
			long proceesedLines = 0;
			int batchCounter1 = 0;
			BufferedReader br = new BufferedReader(new FileReader(new File(similarityFile)));
			String sCurrentLine = "";
			BufferedWriter bw = new BufferedWriter (new FileWriter(new File(outFileAllHits)));
			
			BufferedWriter bwReject = new BufferedWriter (new FileWriter(new File(outFileRejectedHits)));

			/*PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO alignments2 " +
							"(seqid_query,seqid_hit,query_begin,query_end,hit_begin,hit_end,bit_score, " +
							"sw_score, evalue, identity, positives, covered_tms_query, perc_covered_tms_query, " +
							"covered_tms_hit, perc_covered_tms_hit, similarity, overlap, alignment_coverage_query," +
							"alignment_coverage_hit, selfscoreRatio_query, selfscoreRatio_hit) VALUES " +
					"(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");*/	
			while((sCurrentLine=br.readLine())!=null){
				proceesedLines++;
				String q2[] = sCurrentLine.split("\t");

				int seqidquery = Integer.parseInt(q2[0]);
				int seqidhit = Integer.parseInt(q2[1]);
				
				if(seqidquery == seqid || seqidhit==seqid){

					int query_begin = Integer.parseInt(q2[12]);
					int query_end = Integer.parseInt(q2[13]);
					int hit_begin = Integer.parseInt(q2[14]);
					int hit_end = Integer.parseInt(q2[15]);
					double bitscore = Double.parseDouble(q2[3]);
					int sw_score = Integer.parseInt(q2[2]);
					double evalue= Double.parseDouble(q2[4]);
					float identity= Float.parseFloat(q2[5]);

					float positives= 0f;

					float similarity = Float.parseFloat(q2[6]);
					int overlap = Integer.parseInt(q2[7]);
					float alignment_coverage_query = Float.parseFloat(q2[8]);
					float alignment_coverage_hit = Float.parseFloat(q2[9]);
					float selfscoreRatio_query = Float.parseFloat(q2[10]);
					float selfscoreRatio_hit = Float.parseFloat(q2[11]);

					q2 = null;
					// CALCULATE
					// PARAMS************************************
					if(seqidquery!=seqidhit){ // self hit
						if(evalue < maxEvalue && bitscore > minBitScore){		
							Initialize();
							calculate_tmCoverage(query_begin,query_end, hit_begin, hit_end,seqidhit,seqidquery);
							// all calculated just put in table
							boolean showHit = true;
							if (alignment_coverage_query < minLengthRatio && alignment_coverage_hit< minLengthRatio) {
								showHit = false;
							}
							else{ // low Length Ratio of hit or query Reject 
								bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
										+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
										+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
										+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-LowAlignmentCoverage-");
								bwReject.newLine();
							}
							// query
							if (thisNumberofTmQuery>=3){
								if(covered_tms_query < 2 || covered_tms_perc_query < 40) {
									showHit = false;
								}
								else{ // low tmcoverage in query reject
									bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
											+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
											+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
											+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredQuery-");
									bwReject.newLine();
								}
							}
							else{
								if(covered_tms_query < 1 ) {	// TM numbers are 1, 2 
									showHit = false;			// then at least 1 should be covered
								}
								else{ // low tm coverage in query reject
									bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
											+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
											+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
											+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredQuery-");
									bwReject.newLine();
								}
							}
							//hit
							if (thisNumberofTmHit>=3){
								if(covered_tms_hit < 2 || covered_tms_perc_hit < 40) {
									showHit = false;
								}
								else{ // low tm coverage in hit reject
									bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
											+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
											+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
											+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredHit-");
									bwReject.newLine();
								}
							}
							else{
								if(covered_tms_hit < 1 ) {		// TM numbers are 1, 2
									showHit = false;			// then at least 1 should be covered
								}
								else{ // low tm coverage in hit reject
									bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
											+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
											+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
											+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredHit-");
									bwReject.newLine();
								}
							}
							if(showHit == true){
								/*pstm1.setInt(1, seqidquery);
							pstm1.setInt(2, seqidhit);
							pstm1.setInt(3, query_begin);
							pstm1.setInt(4, query_end);
							pstm1.setInt(5, hit_begin);
							pstm1.setInt(6, hit_end);
							pstm1.setDouble(7, bitscore);
							pstm1.setInt(8, sw_score);
							pstm1.setDouble(9, evalue);
							pstm1.setFloat(10, identity);
							pstm1.setFloat(11, positives);
							pstm1.setInt(12, covered_tms_query);
							pstm1.setDouble(13, covered_tms_perc_query);
							pstm1.setInt(14, covered_tms_hit);
							pstm1.setDouble(15, covered_tms_perc_hit);

							pstm1.setFloat(16, similarity);
							pstm1.setInt(17, overlap);
							pstm1.setFloat(18, alignment_coverage_query);
							pstm1.setFloat(19, alignment_coverage_hit);
							pstm1.setFloat(20, selfscoreRatio_query);
							pstm1.setFloat(21, selfscoreRatio_hit);

							pstm1.addBatch();*/
								String insert = seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
										+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
										+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
										+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\n";

								//  
								// Do your code that continuously adds new messages/strings.
								sb.append(insert);  
								// Then once you are done...
								// 
								batchCounter1 ++;


								if(!sequences.containsKey(seqidquery)){
									sequences.put(seqidquery, null);
								}
								if(!sequences.containsKey(seqidhit)){
									sequences.put(seqidhit, null);
								}
								if (batchCounter1 % MULTIROW_INSERT_SIZE == 0){
									String result = sb.toString();
									bw.write(result);
									sb = new StringBuilder();
									//pstm1.executeBatch();
									//pstm1.clearBatch();
								}
							}
						}
						else{ // low eval OR bitscore reject
							Initialize();
							bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
									+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+"-"+"\t"+"-"
									+"\t"+"-"+"\t"+"-"+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
									+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-LowIdentityOrEval-");
							bwReject.newLine();
						}
					}
					if(proceesedLines%100000 == 0){
						System.out.println("Lines Processsed:" + proceesedLines);
					}
				}
			}
			//pstm1.executeBatch();
			//pstm1.clearBatch();
			//pstm1.close();
			//pstm1 = null;
			String result = sb.toString();
			bw.write(result);
			bw.close();
			bwReject.close();

			CAMPS_CONNECTION.close();
			CAMPS_CONNECTION = null;

			br.close();
			System.out.print("Total Sequences: "+ sequences.size());

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void populate_tms(){
		try{
			int x = 0;
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("" +
					"select sequenceid,tms_id,begin,end from tms group by sequenceid,tms_id");
			ResultSet rs1 = pstm1.executeQuery();
			int sqLast = 0;

			ArrayList<Integer> tids = new ArrayList<Integer>();
			ArrayList<Integer> tempS = new ArrayList<Integer>();
			ArrayList<Integer> tempE = new ArrayList<Integer>();

			boolean first = true;
			while(rs1.next()){
				x++;
				if (x%1000 == 0){
					System.out.print("Processed " + x +"\n");
				}
				int sqNow = rs1.getInt("sequenceid");

				if (first){
					sqLast = sqNow;
					first = false;
				}

				if (sqNow != sqLast){
					// the set of tms for this protein is complete
					// add sqlast and the refresh all
					//if (tids.size() == TmNoToChose){
					//sequenceIds.add(sqLast);
					TMS temp = new TMS();
					temp.start = tempS;
					temp.end = tempE;


					tms.put(sqLast, temp);
					//}
					sqLast = sqNow;
					tids = new ArrayList<Integer>();
					tempS = new ArrayList<Integer>();
					tempE = new ArrayList<Integer>();
				}
				int tid = rs1.getInt("tms_id");
				int S = rs1.getInt("begin");
				int E = rs1.getInt("end");

				tids.add(tid);
				tempS.add(S);
				tempE.add(E);

			}
			pstm1.close();
			rs1.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void calculate_tmCoverage(int query_begin, int query_end,
			int hit_begin, int hit_end, int hitSequenceID, int queryID) throws SQLException {
		// TODO Auto-generated method stub

		//get coordinates of query tms from table
		try{
			ArrayList<Integer> startArray_Q = new ArrayList<Integer>();
			ArrayList<Integer> endArray_Q = new ArrayList<Integer>();
			TMS t = tms.get(queryID);
			startArray_Q = t.start;
			endArray_Q = t.end;
			t =null;
			thisNumberofTmQuery = startArray_Q.size();

			//get coordinates of hit tms from table

			// get hit from mapped tms for every tms

			ArrayList<Integer> startArray_hit = new ArrayList<Integer>();
			ArrayList<Integer> endArray_hit = new ArrayList<Integer>();
			t = new TMS();
			t = tms.get(hitSequenceID);
			startArray_hit = t.start;
			endArray_hit = t.end;
			t =null;
			thisNumberofTmHit = startArray_hit.size();
			//Calculate now

			int index = 0;

			for (Integer tms : startArray_Q) {
				int tms_start = startArray_Q.get(index);
				int tms_end = endArray_Q.get(index);
				index++;

				if ((tms_start >= (query_begin - 5))
						&& (tms_end <= (query_end + 5))) {
					covered_tms_query++;
				}
			}

			covered_tms_perc_query = (covered_tms_query * 100)
					/ ((double) startArray_Q.size());

			index = 0;
			for (Integer tms : startArray_hit) {
				int tms_start = startArray_hit.get(index);
				int tms_end = endArray_hit.get(index);
				index++;
				if ((tms_start >= (hit_begin - 5))
						&& (tms_end <= (hit_end + 5))) {
					covered_tms_hit++;
				}
			}
			covered_tms_perc_hit = (covered_tms_hit * 100)
					/ ((double) startArray_hit.size());

			startArray_hit = null;
			endArray_hit = null ;
			startArray_Q = null;
			endArray_Q = null;
		}
		catch(Exception e){

			Initialize();
			e.printStackTrace();
		}
	}

}
