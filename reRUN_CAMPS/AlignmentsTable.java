package reRUN_CAMPS;

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

import extract_proteins.TMS;

import utils.DBAdaptor;

public class AlignmentsTable {

	/**
	 * @param args
	 * populates the new ALignments table -- alignments2
	 *  
	 */
	private static final String similarityFile= "/localscratch/CampsSimilarityScores/camps_seq_file.matrix";
	private static final String outFile = "/localscratch/CampsSimilarityScores/camps_seq_file_filtered.matrix";

	//private static final String similarityFile= "F:/Scratch/missingGenomes/similarityscore/tempfile";
	//private static final String outFile = "F:/Scratch/missingGenomes/similarityscore/Outtempfile";

	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");

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

	private static ArrayList<Integer> exception = new ArrayList<Integer>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Populating TMS...");
		populate_tms();
		System.out.println("Processsing similarity File");
		ProcessSimilarityFile();
		System.out.println("Exception sequences: ");
		for(int i =0;i<=exception.size()-1;i++){
			System.out.println(exception.get(i));
		}
	}
	private static void ProcessSimilarityFile() {
		// TODO Auto-generated method stub
		try{
			StringBuilder sb = new StringBuilder();
			HashMap<Integer,Integer> sequences = new HashMap<Integer,Integer>();
			long proceesedLines = 0;
			int batchCounter1 = 0;
			BufferedReader br = new BufferedReader(new FileReader(new File(similarityFile)));
			String sCurrentLine = "";
			BufferedWriter bw = new BufferedWriter (new FileWriter(new File(outFile)));

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
						// query
						if (thisNumberofTmQuery>=3){
							if(covered_tms_query < 2 || covered_tms_perc_query < 40) {
								showHit = false;
							}
						}
						else{
							if(covered_tms_query < 1 ) {	// TM numbers are 1, 2 
								showHit = false;			// then at least 1 should be covered
							}
						}
						//hit
						if (thisNumberofTmHit>=3){
							if(covered_tms_hit < 2 || covered_tms_perc_hit < 40) {
								showHit = false;
							}
						}
						else{
							if(covered_tms_hit < 1 ) {		// TM numbers are 1, 2
								showHit = false;			// then at least 1 should be covered
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
				}
				if(proceesedLines%100000 == 0){
					System.out.println("Lines Processsed:" + proceesedLines);
				}
			}
			//pstm1.executeBatch();
			//pstm1.clearBatch();
			//pstm1.close();
			//pstm1 = null;
			String result = sb.toString();
			bw.write(result);
			bw.close();

			CAMPS_CONNECTION.close();
			CAMPS_CONNECTION = null;

			br.close();
			System.out.print("Total Sequences: "+ sequences.size());

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void Initialize(){
		covered_tms_perc_query = 0 ;
		covered_tms_perc_hit = 0 ;
		covered_tms_query = 0 ;
		covered_tms_hit = 0 ;
		thisNumberofTmQuery = 0;
		thisNumberofTmHit = 0;
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
			exception.add(hitSequenceID);
			exception.add(queryID);
			Initialize();
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
}
