package extract_proteins;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Hashtable;

import mips.gsf.de.simapclient.client.SimapAccessWebService;
import mips.gsf.de.simapclient.datatypes.HitSet;
import mips.gsf.de.simapclient.datatypes.ResultParser;

import datastructures.TMS;
//import de.gsf.mips.simap.lib.database.SimapHitfile;

public class ReadHitsThreadNew implements Runnable{

	// values for BLOSUM50
	private final double MATRIX_K = 0.109;
	private final double MATRIX_LAMBDA = 0.242;	
	
		
	private static Integer threadCount = new Integer(0);
	
	//SIMAP sequence id for query sequence
	private int sequenceID;
	
	private BitSet selectedSequences;
	
	//mapping of CAMPS to SIMAP sequence ids (index: SIMAP id, value: CAMPS id)
	private int[] mapping;
	
	//private long searchspaceSize;
	
	//private int[] sequencelengths;
	
	private Connection connection;	
	
	private int Query_Len;
	private int Hit_len;
	
	private int minScore;
	
	private double minBitScore = 0.0;	//default
	
	private double maxEvalue;
	
	private float minLengthRatio;
	
	
	private Hashtable<Integer,ArrayList<TMS>> campsid2tms;
	
	
	//number of records for insertion of multiple rows at once
	private static final int MULTIROW_INSERT_SIZE = 1000;
	
	
	
	
			
	public ReadHitsThreadNew(int sequenceID, BitSet selectedSequences, int[] mapping,Connection connection, double maxEvalue, float minLengthRatio, Hashtable<Integer,ArrayList<TMS>> campsid2tms) {
		changeThreadCount(1);
		
			
		this.sequenceID = sequenceID;
		this.selectedSequences = selectedSequences;
		this.mapping = mapping;
		//this.searchspaceSize = searchspaceSize;
		//this.sequencelengths = sequencelengths;	
		
		//since older version of simap depricated, the sequence length is simple calculated by string.len therefore
		//using below integers
		
		this.Hit_len = 0;
		this.Query_Len = 0;
		
		this.connection = connection;		
		this.maxEvalue = maxEvalue;
		this.minLengthRatio = minLengthRatio;
		this.campsid2tms = campsid2tms;
		this.minScore = 95;	//minsw_score
		Thread t = new Thread(this);
		t.start();
	}
	
	public void run() {
		
		try {
			
			//statement template for multirow insert			
			String sql = "INSERT INTO alignments " +
					" (sequences_sequenceid_query,sequences_sequenceid_hit,query_begin,query_end,hit_begin,hit_end,bit_score,sw_score,evalue,identity,positives,covered_tms_query,perc_covered_tms_query,covered_tms_hit,perc_covered_tms_hit)" +
					" VALUES ";
			PreparedStatement pstm = connection.prepareStatement(
					"INSERT INTO alignments_nonTMS " +
					"(sequences_sequenceid_query," +
					"sequences_sequenceid_hit," +
					"query_begin," +
					"query_end," +
					"hit_begin," +
					"hit_end," +
					"bit_score," +
					"sw_score," +
					"evalue," +
					"identity," +
					"positives," +
					"covered_tms_query," +
					"perc_covered_tms_query," +
					"covered_tms_hit," +
					"perc_covered_tms_hit)" +
					"VALUES " +
					"(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			
			
			StringBuffer multirowInsert = new StringBuffer(); 
			
			Statement stmImport = connection.createStatement();
				
			//get transmembrane positions for query sequence
			int queryCampsSequenceid = mapping[sequenceID];
			ArrayList<TMS> tms_arr_query = campsid2tms.get(Integer.valueOf(queryCampsSequenceid));
			
			
			int currentmultirowInsertSize = 0;
			
			
			//****************************************************************************************************
			//Set max number of hits or not?
			//set max evalue chaned from now...set the value and sw score..for now just implementing code
			
			String seq = new String();
			String md5 = new String();
			//int sequenceid4 = 0;
			//int sequenceid5 = 0;
			
			//double maxEvalue = 1E-5;
			//float minLengthRatio = 0.5f;
				
			SimapAccessWebService simap=new SimapAccessWebService();
			
				seq = simap.getSequence(sequenceID);
				this.Query_Len = seq.length();
				//System.out.print(simap.getSequence(sequenceid[i])+"\n\n\n");
				
				md5 = simap.computeMD5(seq);
				simap.setMd5(md5);
				//System.out.println(simap.getProtein(md5));
				
				
				simap.setMin_swscore(minScore);
				simap.setMax_evalue(maxEvalue);
				//simap.setMax_number_hits(50);
				
				simap.alignments(true);
				simap.sequences(true);
				
				//simap.getMax_number_hits();
				ArrayList<HitSet> result = ResultParser.parseResult(simap.getHitsXML());
				HitSet second=(HitSet) result.get(result.size()-1); //Brings an exception!!! More over this isnt really required here...
				//Exception seems to be because there might be some sequences with no hits? and so second has null in it
				//and so the exception comes at index zero and size zero
				//HitSet second ; 
								
				//System.out.print("Hit Number: "+second.getHitData().getNumber_hits()+"\n");
				
			
			
				//****************************************************************************************************
			
			
			
			
			for (int j =result.size()-1; j>= 0;j--){
				
				second=(HitSet) result.get(j);
				
				int hitSequenceID = second.getHitData().getSequence_id();
				//System.out.print("Hit: "+second.getHitData().getProteins().get(j)+"\n");
				this.Hit_len = second.getHitData().getSequence().length();
				
				if(sequenceID < hitSequenceID) {
				
				int mappedSequenceidQuery = mapping[sequenceID];
				int mappedSequenceidHit = mapping[hitSequenceID];
				ArrayList<TMS> tms_arr_hit = campsid2tms.get(Integer.valueOf(mappedSequenceidHit));
				
				int query_alignmentstart = second.getHitAlignment().getQuery_start();
				int query_alignmentend = second.getHitAlignment().getQuery_stop();
				
				int match_alignmentstart = second.getHitAlignment().getHit_start();
				int match_alignmentend = second.getHitAlignment().getHit_stop();
				int score = second.getHitAlignment().getScore();
				int bitScore = (int) second.getHitAlignment().getBits();
				double eValue = second.getHitAlignment().getEvalue();
				
				float identity = (float) second.getHitAlignment().getIdentity();
				float positives = (float) second.getHitAlignment().getPositives();
				
				//int hitCampsSequenceid = mapping[hitSequenceID];
        		
				// Calculating the covered tms query and covered tms hit etc
				
				
				
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
				
				//calculate tms perc hit covered
				
				if(tms_arr_hit != null){ //if array is not empty then do below
				for(TMS tms:tms_arr_hit) {
					int tms_start = tms.get_start();
					int tms_end = tms.get_end();
					if((tms_start >= (match_alignmentstart - 5)) && (tms_end <= (match_alignmentend + 5))) {
						covered_tms_hit++;
					}
				}
				}
				else{
					//currentmultirowInsertSize++;
					
					//commented out below cuz repeatition
					//,,,,,,,,,,,perc_covered_tms_hit	
					if (eValue <=1E-5 && identity >= 40f){
					mappedSequenceidQuery = mapping[sequenceID];
					mappedSequenceidHit = mapping[hitSequenceID];
				
					pstm.setInt(1, mappedSequenceidQuery);
					pstm.setInt(2, mappedSequenceidHit);
					pstm.setInt(3, query_alignmentstart);
					pstm.setInt(4, query_alignmentend);
					pstm.setInt(5, match_alignmentstart);
					pstm.setInt(6, match_alignmentend);
					pstm.setInt(7, bitScore);
					pstm.setInt(8, score);
					pstm.setDouble(9, eValue);
					pstm.setFloat(10, identity);
					pstm.setFloat(11, positives);
					pstm.setInt(12, covered_tms_query);
					pstm.setDouble(13, covered_tms_perc_query);					
					pstm.setDouble(14, covered_tms_hit);
					pstm.setDouble(15, 0);
					
					pstm.executeUpdate();
					pstm.clearParameters();
					}
					continue;
				
					
				}
				double covered_tms_perc_hit = (covered_tms_hit*100)/((double) tms_arr_hit.size());
				
				//calculate the show hit stuff
				
				//int querysequencelength = sequencelengths[sequenceID];
				float alignmentLengthRatioSubject = (float) (query_alignmentend-query_alignmentstart)/(float) this.Query_Len;
				//int hitsequencelength = sequencelengths[hitSequenceID];
				float alignmentLengthRatioHit = (float) (match_alignmentend-match_alignmentstart)/(float) this.Hit_len;
				
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
				if(covered_tms_query < 1 || covered_tms_perc_query < 40 || covered_tms_hit < 1 || covered_tms_perc_hit < 40) {
					showHit = false;
				}
				
				
				synchronized (connection) {
					
					if(showHit) {
						
						currentmultirowInsertSize++;
						
						//commented out below cuz repeatition
						mappedSequenceidQuery = mapping[sequenceID];
						mappedSequenceidHit = mapping[hitSequenceID];
						
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
				
				/*
				System.out.print("Hit id: "+second.getHitData().getSequence_id());
				System.out.print("Hit SelfScore: "+second.getHitData().getSelfscore());
				
				System.out.print("Query begin: "+second.getHitAlignment().getQuery_start()+"\n");
				System.out.print("Query end: "+second.getHitAlignment().getQuery_stop()+"\n");
				
				
				System.out.print("Hit begin: "+second.getHitAlignment().getHit_start()+"\n");
				System.out.print("Hit end: "+second.getHitAlignment().getHit_stop()+"\n");
				
				System.out.print("Bit Score: " +second.getHitAlignment().getBits()+"\n");
				System.out.print("Sw Score: "+second.getHitAlignment().getScore()+"\n"); //????????
				
				System.out.print("Evalue: "+second.getHitAlignment().getEvalue()+"\n");
				System.out.print("Identity: "+second.getHitAlignment().getIdentity()+"\n");
				System.out.print("Positives: "+second.getHitAlignment().getPositives()+"\n");
				
				System.out.print("Covered tms Query: "+second.getHitAlignment().getQuery_coverage()+"\n");
				System.out.print("Perc Covered tms Query: "+second.getHitAlignment().getCoverageRatio()+"\n"); //???????
				
				System.out.print("Covered tms Hit: "+second.getHitAlignment().getHit_coverage()+"\n");
				System.out.print("Perc Covered tms Hit: "+second.getHitAlignment().getCoverageRatio()+"\n"); //????????
				
				System.out.print("\n \n \n \n");
				*/
			}

			
			//*******************************************************************************
			
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
