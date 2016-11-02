package extract_proteins;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import utils.DBAdaptor;

public class InsertInAlign3_Thread extends Thread{
	private Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	private Connection CAMPS_CONNECTION_Insert = DBAdaptor.getConnection("camps4");
	//private static HashMap <Integer, TMS> tms = new HashMap <Integer, TMS>();  

	// number of records for insertion of multiple rows at once

	private double covered_tms_perc_query;
	private double covered_tms_perc_hit;
	private int covered_tms_query;
	private int covered_tms_hit;
	private int thisNumberofTmQuery;
	private int thisNumberofTmHit;
	private float minLengthRatio;
	private int threadNo;

	public String f = "";
	InsertInAlign3_Thread(){

	}
	InsertInAlign3_Thread(String a,int x){
		this.f = a;
		this.threadNo = x;
	}

	//private double minBitScore;


	public void InsertInAlign2_initialize(){
		//this.seqid = 0 ;
		this.covered_tms_perc_query = 0 ;
		this.covered_tms_perc_hit = 0 ;
		this.covered_tms_query = 0 ;
		this.covered_tms_hit = 0 ;
		this.thisNumberofTmQuery = 0;
		this.thisNumberofTmHit = 0;

		this.minLengthRatio = 0.5f;
		//this.minBitScore = 0d;

	}
	public void run(){
		int batchCounter1 = 0;
		int batchSize = 500;
		BufferedReader br = null;
		long counter = 0;

		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(this.f));
			PreparedStatement pstm1 = null;
			//int i = 0;
			//56789227 processed till here

			while ((sCurrentLine = br.readLine()) != null ) {
				counter++;
				if (counter %100000 == 0){
					System.out.print("\nlines processed " + counter + " in thread "+ this.threadNo +"\n");
				}
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

				// CALCULATE
				// PARAMS************************************

				if(evalue < 1E-5 && bitscore > 10){		// *****************SET IT*******************
					InsertInAlign2_initialize();
					calculate_tmCoverage(query_begin,query_end, hit_begin, hit_end,seqidhit,seqidquery);

					// all calculated just put in table
					boolean showHit = true;
					if (alignment_coverage_query < minLengthRatio && alignment_coverage_hit< minLengthRatio) {
						showHit = false;
					}	
					// query
					if (this.thisNumberofTmQuery>=3){
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
					if (this.thisNumberofTmHit>=3){
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
						pstm1 = CAMPS_CONNECTION_Insert.prepareStatement(
								"INSERT INTO alignments2 " +
										"(seqid_query,seqid_hit,query_begin,query_end,hit_begin,hit_end,bit_score, " +
										"sw_score, evalue, identity, positives, covered_tms_query, perc_covered_tms_query, " +
										"covered_tms_hit, perc_covered_tms_hit, similarity, overlap, alignment_coverage_query," +
										"alignment_coverage_hit, selfscoreRatio_query, selfscoreRatio_hit) VALUES " +
								"(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");	

						pstm1.setInt(1, seqidquery);
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
						pstm1.setInt(12, this.covered_tms_query);
						pstm1.setDouble(13, this.covered_tms_perc_query);
						pstm1.setInt(14, this.covered_tms_hit);
						pstm1.setDouble(15, this.covered_tms_perc_hit);

						pstm1.setFloat(16, similarity);
						pstm1.setInt(17, overlap);
						pstm1.setFloat(18, alignment_coverage_query);
						pstm1.setFloat(19, alignment_coverage_hit);
						pstm1.setFloat(20, selfscoreRatio_query);
						pstm1.setFloat(21, selfscoreRatio_hit);
						pstm1.addBatch();

						batchCounter1++;

						if(batchCounter1 % batchSize == 0) {
							synchronized(CAMPS_CONNECTION_Insert) {
								pstm1.executeBatch();
								pstm1.clearBatch();
								pstm1.close();
							}
						}
					}
				}
			}
			synchronized(CAMPS_CONNECTION_Insert) {
				pstm1.executeBatch();
				pstm1.clearBatch();
			}
			br.close();


		} catch (IOException e) {
			System.out.print("\nThreadNumber"+this.threadNo+"\n");
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.print("\nThreadNumber"+this.threadNo+"\n");
			e.printStackTrace();
		}

	}
	private void calculate_tmCoverage(int query_begin, int query_end,
			int hit_begin, int hit_end, int hitSequenceID, int queryID) throws SQLException {
		// TODO Auto-generated method stub

		//get coordinates of query tms from table
		ResultSet rs = null;

		PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("SELECT begin,end FROM tms WHERE sequenceid=?");
		pstm.setInt(1,queryID); //pick begin and end from mapped_tms table for this sequenceid 
		rs = pstm.executeQuery();

		ArrayList<Integer> startArray_Q = new ArrayList<Integer>();
		ArrayList<Integer> endArray_Q = new ArrayList<Integer>();
		int j =0;

		while(rs.next()) {
			startArray_Q.add(j, rs.getInt("begin"));
			endArray_Q.add(j, rs.getInt("end"));		
			j++;
			//making an array with start and another array with end... at same index of both we have a start and end of the tm

			//so...can i make a temp aray of starts and end..and send it with seq id to read thread
			// in that function retreive the start end of the hit and calculate and submit in alignment table.
		}

		//we now have here..the seqidQuery tms coordinates of query..we can now send it for calculation of blast ad so on!
		//rs.close();
		rs = null;
		pstm.close();
		pstm = null;

		//get coordinates of hit tms from table

		// get hit from mapped tms for every tms

		PreparedStatement pstm_find_hit = this.CAMPS_CONNECTION
				.prepareStatement("SELECT begin,end FROM tms WHERE sequenceid=?");
		pstm_find_hit.setInt(1, hitSequenceID); // pick begin
		// and end from
		// mapped_tms
		// table for
		// this
		// sequenceid
		ResultSet rs_get_hit = pstm_find_hit.executeQuery();

		ArrayList<Integer> startArray_hit = new ArrayList<Integer>();
		ArrayList<Integer> endArray_hit = new ArrayList<Integer>();

		int k = 0;
		while (rs_get_hit.next()) {
			startArray_hit.add(k, rs_get_hit.getInt("begin"));
			endArray_hit.add(k, rs_get_hit.getInt("end"));
			k++;
		}
		rs_get_hit.close();
		rs_get_hit = null;

		pstm_find_hit.close();
		pstm_find_hit = null;

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

		this.covered_tms_perc_query = (covered_tms_query * 100)
				/ ((double) startArray_Q.size());

		//int covered_tms_hit = 0;
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
		this.covered_tms_perc_hit = (covered_tms_hit * 100)
				/ ((double) startArray_hit.size());
		
		startArray_hit = null;
		endArray_hit = null ;
		startArray_Q = null;
		endArray_Q = null;

	}

}
