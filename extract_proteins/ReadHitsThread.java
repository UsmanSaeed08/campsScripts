package extract_proteins;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Hashtable;

import datastructures.TMS;
import de.gsf.mips.simap.lib.database.SimapHitfile;

public class ReadHitsThread implements Runnable{

	// values for BLOSUM50
	private final double MATRIX_K = 0.109;
	private final double MATRIX_LAMBDA = 0.242;	
	
		
	private static Integer threadCount = new Integer(0);
	
	//SIMAP sequence id for query sequence
	private int sequenceID;
	
	private BitSet selectedSequences;
	
	//mapping of CAMPS to SIMAP sequence ids (index: SIMAP id, value: CAMPS id)
	private int[] mapping;
	
	private long searchspaceSize;
	
	private int[] sequencelengths;
	
	private Connection connection;	
	
	
	private int minScore;
	
	private double minBitScore = 0.0;	//default
	
	private double maxEvalue;
	
	private float minLengthRatio;
	
	
	private Hashtable<Integer,ArrayList<TMS>> campsid2tms;
	
	
	//number of records for insertion of multiple rows at once
	private static final int MULTIROW_INSERT_SIZE = 1000;
	
	
	
	
			
	public ReadHitsThread(int sequenceID, BitSet selectedSequences, int[] mapping, long searchspaceSize,int[] sequencelengths,Connection connection, double maxEvalue, float minLengthRatio, Hashtable<Integer,ArrayList<TMS>> campsid2tms) {
		changeThreadCount(1);
		
			
		this.sequenceID = sequenceID;
		this.selectedSequences = selectedSequences;
		this.mapping = mapping;
		this.searchspaceSize = searchspaceSize;
		this.sequencelengths = sequencelengths;		
		this.connection = connection;		
		this.maxEvalue = maxEvalue;
		this.minLengthRatio = minLengthRatio;
		this.campsid2tms = campsid2tms;
		
		Thread t = new Thread(this);
		t.start();
	}
	
	public void run() {
		
		try {
			
			//statement template for multirow insert			
			String sql = "INSERT INTO alignments " +
					" (sequences_sequenceid_query,sequences_sequenceid_hit,query_begin,query_end,hit_begin,hit_end,bit_score,sw_score,evalue,identity,positives,covered_tms_query,perc_covered_tms_query,covered_tms_hit,perc_covered_tms_hit)" +
					" VALUES ";
			
			StringBuffer multirowInsert = new StringBuffer(); 
			
			Statement stmImport = connection.createStatement();
			
			
			// Calculate minimum score from max Evalue and adapt minScore if necessary
			minScore = (int) Math.round((minBitScore * 0.693147181f + Math.log(MATRIX_K)) / MATRIX_LAMBDA);
			//System.out.println("\n\t\t[INFO]: calculated score="+minScore+" from minBitScore="+minBitScore);
			
			if (maxEvalue > 0) {
				// shortest protein assumed of length 50
				int minScoreFromMaxEvalue = (int) Math.floor((-Math.log( maxEvalue/ (50 * searchspaceSize)) + Math.log(MATRIX_K)) / MATRIX_LAMBDA);
				if (minScore == 0) {
					minScore = minScoreFromMaxEvalue;	
					//System.out.println("\n\t\t[INFO]: Using minimal score of "+minScore+" according to the given maximal E-Value.");
				} else {
					minScore = Math.max(minScore, minScoreFromMaxEvalue);	
					//System.out.println("\n\t\t[INFO]: Using minimal score of "+minScore+" according to the given minimal score and maximal E-Value.");
				}
			}
			
						
			//get transmembrane positions for query sequence
			int queryCampsSequenceid = mapping[sequenceID];
			ArrayList<TMS> tms_arr_query = campsid2tms.get(Integer.valueOf(queryCampsSequenceid));
			
			
			int currentmultirowInsertSize = 0;
			
			SimapHitfile hitfile = SimapHitfile.getInstance();//?????
			//getHits(int x,int y,BitSet bs,int z): z=0 for max_number_hits parameter => option is disabled
			String[] hits = hitfile.hits2string(sequenceID, hitfile.getHits(sequenceID, minScore, selectedSequences, 0)).split("\t");
			for (int i = hits.length-1; i >=0; i --) {
				String hit = hits[i];
				String[] hitparts = hit.split(" ");
				if (hitparts.length >1) {
					try {
						
						int hitSequenceID = Integer.parseInt(hitparts[0]);
						
						if(sequenceID < hitSequenceID) {					//get hits from triangle matrix
							
							//get transmembrane positions for hit sequence	                		
	                		int hitCampsSequenceid = mapping[hitSequenceID];
	                		ArrayList<TMS> tms_arr_hit = campsid2tms.get(Integer.valueOf(hitCampsSequenceid));
	        																			        				
	        				int score = Integer.parseInt(hitparts[1]);
	        				
	        				int query_alignmentstart = Integer.parseInt(hitparts[5]);
							int query_alignmentend = Integer.parseInt(hitparts[6]);
	        				
							int match_alignmentstart = Integer.parseInt(hitparts[7]);
							int match_alignmentend = Integer.parseInt(hitparts[8]);
	        				
							int querysequencelength = sequencelengths[sequenceID];
							float alignmentLengthRatioSubject = (float) (query_alignmentend-query_alignmentstart)/(float) querysequencelength;
							int hitsequencelength = sequencelengths[hitSequenceID];
							float alignmentLengthRatioHit = (float) (match_alignmentend-match_alignmentstart)/(float) hitsequencelength;
	        				
							double bitScore = ((float) score * MATRIX_LAMBDA - Math.log(MATRIX_K)) / 0.693147181f;
							double eValue = Math.max((double) Math.max(querysequencelength, hitsequencelength) * 
										searchspaceSize * Math.pow(2, -bitScore),1e-300) ;
	        										
							
							float identity = (float) Integer.parseInt(hitparts[2]) / 1000f;
							float positives = (float) Integer.parseInt(hitparts[3]) / 1000f;
							
							//int overlap = Integer.parseInt(hitparts[4]);
							
							int covered_tms_query = 0;
            				for(TMS tms:tms_arr_query) {
            					int tms_start = tms.get_start();
            					int tms_end = tms.get_end();
            					if((tms_start >= (query_alignmentstart - 5)) && (tms_end <= (query_alignmentend + 5))) {
            						covered_tms_query++;
            					}
            				}
            				double covered_tms_perc_query = (covered_tms_query*100)/((double) tms_arr_query.size());
            				
            				int covered_tms_hit = 0;
            				for(TMS tms:tms_arr_hit) {
            					int tms_start = tms.get_start();
            					int tms_end = tms.get_end();
            					if((tms_start >= (match_alignmentstart - 5)) && (tms_end <= (match_alignmentend + 5))) {
            						covered_tms_hit++;
            					}
            				}
            				double covered_tms_perc_hit = (covered_tms_hit*100)/((double) tms_arr_hit.size());

							boolean showHit = true;
							if (eValue > maxEvalue) {
								showHit = false;
							}								
							if (alignmentLengthRatioSubject < minLengthRatio && alignmentLengthRatioHit< minLengthRatio) {
								showHit = false;
							}								
							if (bitScore < minBitScore) {
								showHit = false;
							}							
							if(covered_tms_query < 2 || covered_tms_perc_query < 40 || covered_tms_hit < 2 || covered_tms_perc_hit < 40) {
								showHit = false;
							}
            				
							
							synchronized (connection) {
								
								if(showHit) {
									
									currentmultirowInsertSize++;
									
									int mappedSequenceidQuery = mapping[sequenceID];
									int mappedSequenceidHit = mapping[hitSequenceID];
									
									String insert = "("+mappedSequenceidQuery+","+mappedSequenceidHit+","+
									query_alignmentstart+","+query_alignmentend+","+
									match_alignmentstart+","+match_alignmentend+","+
									bitScore+","+score+","+
									eValue+","+identity+","+positives+","+
									covered_tms_query+","+covered_tms_perc_query+","+
									covered_tms_hit+","+covered_tms_perc_hit+")";
																	
									multirowInsert.append(","+insert);
									
									if(currentmultirowInsertSize % MULTIROW_INSERT_SIZE == 0) {
										String fullInsertStatement = sql+multirowInsert.toString().substring(1);
										stmImport.executeUpdate(fullInsertStatement);
										
										//reset all data
										multirowInsert = new StringBuffer(); 
									}									
										
								}							
							}
						}
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
				}
			}
			
			//insert remaining records
			if(multirowInsert.length() != 0) {
				String fullInsertStatement = sql+multirowInsert.toString().substring(1);
				stmImport.executeUpdate(fullInsertStatement);
				multirowInsert = new StringBuffer(); 
			}
			
			stmImport.close();
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}	
		changeThreadCount(-1);
	}	
	
	public static int getThreadCount() {
		return threadCount.intValue();
	}
	
	private static void changeThreadCount(int value) {
		synchronized(threadCount) {
			threadCount = new Integer(threadCount.intValue() + value);
		}
	}
	
}
