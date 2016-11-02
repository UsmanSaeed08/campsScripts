package extract_proteins;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import mips.gsf.de.simapclient.client.SimapAccessWebService;
import mips.gsf.de.simapclient.datatypes.HitSet;
import mips.gsf.de.simapclient.datatypes.ResultParser;

public class ReadHitsThreadNew4 {

	private int seqid;
	private Connection connection;

	private int Query_Len;
	private int Hit_len;

	private int minScore;
	private double minBitScore = 0.0; // default
	private double maxEvalue;
	private float minLengthRatio;

	// number of records for insertion of multiple rows at once
	private static final int MULTIROW_INSERT_SIZE = 5;
	private ArrayList<Integer> startArray_Q;
	private ArrayList<Integer> endArray_Q;

	public ReadHitsThreadNew4(int seqid, ArrayList<Integer> startArray_Query,
			ArrayList<Integer> endArray_Query, Connection conn, double maxE,
			float minLenR) throws Exception {

		this.connection = conn;
		this.maxEvalue = maxE;
		this.minLengthRatio = minLenR;

		this.minScore = 95; // minsw_score
		this.Hit_len = 0;
		this.Query_Len = 0;
		this.seqid = seqid;

		this.startArray_Q = startArray_Query;
		this.endArray_Q = endArray_Query;

		run3();
	}

	private void run3() throws Exception {
		// establish connection with simap
		// get hit id
		// find hit in camps
		// if exists then calc parameters
		// else put in un mapped hits

		String sql = "INSERT INTO alignments_test "
				+ " (sequences_sequenceid_query,sequences_sequenceid_hit,query_begin,query_end,hit_begin,hit_end,bit_score,sw_score,evalue,identity,positives,covered_tms_query,perc_covered_tms_query,covered_tms_hit,perc_covered_tms_hit)"
				+ " VALUES ";

		PreparedStatement pstm_hitInsert;
		try {

			int currentmultirowInsertSize = 0;
			StringBuffer multirowInsert = new StringBuffer();
			Statement stmImport = connection.createStatement();

			// ********************** 1 ESTABLISH CONNECTION WITH SIMAP AND GET
			// HIT ID *********************************

			String seq = new String();
			String md5 = new String();

			SimapAccessWebService simap = new SimapAccessWebService();

			seq = simap.getSequence(this.seqid);
			this.Query_Len = seq.length();

			md5 = simap.computeMD5(seq);
			simap.setMd5(md5);

			simap.setMin_swscore(minScore);
			simap.setMax_evalue(maxEvalue);
			// simap.setMax_number_hits(50); ?? sHOULD i HAVE SOME SET NUMBER OF
			// MAX HITS? 100 PERHAPS?

			simap.alignments(true);
			simap.sequences(true);

			ArrayList<HitSet> result = ResultParser.parseResult(simap
					.getHitsXML());
			HitSet second;

			for (int j = 0; j <= result.size() - 1; j++) { // get each hit for
															// the seqid

				second = (HitSet) result.get(j);

				int hitSequenceID = second.getHitData().getSequence_id();
				// System.out.print("Hit: "+second.getHitData().getProteins().get(j)+"\n");
				this.Hit_len = second.getHitData().getSequence().length();

				int query_alignmentstart = second.getHitAlignment()
						.getQuery_start();
				int query_alignmentend = second.getHitAlignment()
						.getQuery_stop();

				int match_alignmentstart = second.getHitAlignment()
						.getHit_start();
				int match_alignmentend = second.getHitAlignment().getHit_stop();
				int score = second.getHitAlignment().getScore();
				int bitScore = (int) second.getHitAlignment().getBits();
				double eValue = second.getHitAlignment().getEvalue();

				float identity = (float) second.getHitAlignment().getIdentity();
				float positives = (float) second.getHitAlignment()
						.getPositives();

				if (eValue < this.maxEvalue && bitScore > this.minBitScore) { // if
																				// hit
																				// is
																				// greater
																				// than
																				// the
																				// required
																				// move
																				// in..else
																				// get
																				// next
																				// hit

					// find hit in camps ..initial step to calculate the percent
					// covered etc

					// search hitid in sequences, if exists find in mapped_tms
					// and process else populate in other table

					PreparedStatement pstm_find_hit_sequences = this.connection
							.prepareStatement("SELECT length FROM sequences WHERE sequenceid=?");
					pstm_find_hit_sequences.setInt(1, hitSequenceID); // pick
																		// begin
																		// and
																		// end
																		// from
																		// mapped_tms
																		// table
																		// for
																		// this
																		// sequenceid
					ResultSet rs_get_hit_sequences = pstm_find_hit_sequences
							.executeQuery();
					int flag = 0; // mapped or not 1 if mapped

					// ************************MAPPED
					// PROTEINS***********************
					if (rs_get_hit_sequences.next()) { // mapped proteins
						flag = 1;
						// get hit from mapped tms for every tms
						PreparedStatement pstm_find_hit = this.connection
								.prepareStatement("SELECT begin,end FROM mapped_tms WHERE campsId=?");
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

						// **********************TMS EXTRACTION COMPLETE now
						// CALCULATE
						// PARAMS************************************//

						int covered_tms_query = 0;
						int index = 0;

						for (Integer tms : this.startArray_Q) {
							int tms_start = this.startArray_Q.get(index);
							int tms_end = this.endArray_Q.get(index);
							index++;

							if ((tms_start >= (query_alignmentstart - 5))
									&& (tms_end <= (query_alignmentend + 5))) {
								covered_tms_query++;
							}
						}

						double covered_tms_perc_query = (covered_tms_query * 100)
								/ ((double) this.startArray_Q.size());

						int covered_tms_hit = 0;
						index = 0;
						// calculate tms perc hit covered
						for (Integer tms : startArray_hit) {
							int tms_start = startArray_hit.get(index);
							int tms_end = endArray_hit.get(index);
							index++;
							if ((tms_start >= (match_alignmentstart - 5))
									&& (tms_end <= (match_alignmentend + 5))) {
								covered_tms_hit++;
							}
						}
						double covered_tms_perc_hit = (covered_tms_hit * 100)
								/ ((double) startArray_hit.size());

						// *****************ALL PARAMS CALCULATED********NOW
						// POPULATE TABLE************//

						float alignmentLengthRatioSubject = (float) (query_alignmentend - query_alignmentstart)
								/ (float) this.Query_Len;
						// int hitsequencelength =
						// sequencelengths[hitSequenceID];
						float alignmentLengthRatioHit = (float) (match_alignmentend - match_alignmentstart)
								/ (float) this.Hit_len;

						boolean showHit = true;
						if (eValue > maxEvalue) {
							showHit = false;
						}
						if (alignmentLengthRatioSubject < minLengthRatio
								&& alignmentLengthRatioHit < minLengthRatio) {
							showHit = false;
						}
						if (bitScore < minBitScore) {
							showHit = false;
						}
						if (covered_tms_query < 1
								|| covered_tms_perc_query < 40
								|| covered_tms_hit < 1
								|| covered_tms_perc_hit < 40) {
							showHit = false;
						}

						if (showHit) {
							currentmultirowInsertSize++;

							String insert = "(" + this.seqid + ","
									+ hitSequenceID + ","
									+ query_alignmentstart + ","
									+ query_alignmentend + ","
									+ match_alignmentstart + ","
									+ match_alignmentend + "," + bitScore + ","
									+ score + "," + eValue + "," + identity
									+ "," + positives + "," + covered_tms_query
									+ "," + covered_tms_perc_query + ","
									+ covered_tms_hit + ","
									+ covered_tms_perc_hit + ")";

							multirowInsert.append("," + insert);

							if (currentmultirowInsertSize
									% MULTIROW_INSERT_SIZE == 0) {
								String fullInsertStatement = sql
										+ multirowInsert.toString()
												.substring(1);
								stmImport.executeUpdate(fullInsertStatement);

								// reset all data
								multirowInsert = new StringBuffer();
							}

						}

					}

				}
			}

			System.out.println("." + this.seqid + ".");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
