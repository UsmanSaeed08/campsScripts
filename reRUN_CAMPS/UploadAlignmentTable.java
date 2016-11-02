package reRUN_CAMPS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;

import utils.DBAdaptor;

public class UploadAlignmentTable {

	/**
	 * @param args
	 * Reads the filtered alignments file and puts it in the alignments table in Db
	 */
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4"); //changed to test_camps3
	private static String similarityFile = "/localscratch/CampsSimilarityScores/camps_seq_file_filtered.matrix";
	private static final int MULTIROW_INSERT_SIZE = 10000;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Running...");
		run();
	}
	private static void run(){
		try{
			//StringBuilder sb = new StringBuilder();
			HashMap<Integer,Integer> sequences = new HashMap<Integer,Integer>();
			long batchCounter1 = 0;
			BufferedReader br = new BufferedReader(new FileReader(new File(similarityFile)));
			String sCurrentLine = "";

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO alignments2 " +
							"(seqid_query,seqid_hit,query_begin,query_end,hit_begin,hit_end,bit_score, " +
							"sw_score, evalue, identity, positives, covered_tms_query, perc_covered_tms_query, " +
							"covered_tms_hit, perc_covered_tms_hit, similarity, overlap, alignment_coverage_query," +
							"alignment_coverage_hit, selfscoreRatio_query, selfscoreRatio_hit) VALUES " +
					"(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");	
			while((sCurrentLine=br.readLine())!=null){
				
				String q2[] = sCurrentLine.split("\t");

				int seqidquery = Integer.parseInt(q2[0]);
				int seqidhit = Integer.parseInt(q2[1]);
				int query_begin = Integer.parseInt(q2[2]);
				int query_end = Integer.parseInt(q2[3]);
				int hit_begin = Integer.parseInt(q2[4]);
				int hit_end = Integer.parseInt(q2[5]);

				double bitscore = Double.parseDouble(q2[6]);
				int sw_score = Integer.parseInt(q2[7]);
				double evalue= Double.parseDouble(q2[8]);

				float identity= Float.parseFloat(q2[9]);
				float positives= Float.parseFloat(q2[10]);
				int covered_tms_query = Integer.parseInt(q2[11]);
				double covered_tms_perc_query = Double.parseDouble(q2[12]);
				int covered_tms_hit = Integer.parseInt(q2[13]);
				double covered_tms_perc_hit = Double.parseDouble(q2[14]);


				float similarity = Float.parseFloat(q2[15]);
				int overlap = Integer.parseInt(q2[16]);

				float alignment_coverage_query = Float.parseFloat(q2[17]);
				float alignment_coverage_hit = Float.parseFloat(q2[18]);
				float selfscoreRatio_query = Float.parseFloat(q2[19]);
				float selfscoreRatio_hit = Float.parseFloat(q2[20]);


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

				pstm1.addBatch();

				batchCounter1 ++;


				if(!sequences.containsKey(seqidquery)){
					sequences.put(seqidquery, null);
				}
				if(!sequences.containsKey(seqidhit)){
					sequences.put(seqidhit, null);
				}
				if (batchCounter1 % MULTIROW_INSERT_SIZE == 0){
					pstm1.executeBatch();
					pstm1.clearBatch();
					System.out.println("Lines Processsed:" + batchCounter1);
				}
			}
			pstm1.executeBatch();
			pstm1.clearBatch();
			pstm1.close();

			CAMPS_CONNECTION.close();

			br.close();
			System.out.print("Total Sequences: "+ sequences.size());
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
