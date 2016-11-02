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

import utils.DBAdaptor;

import extract_proteins.TMS;

public class TmDistribution5 {

	/**
	 * @param args
	 */
	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");

	private static final String similarityFile= "/localscratch/CampsSimilarityScores/camps_seq_file.matrix";
	//private static final String similarityFile= "F:/similarityScoresTem.matrix";

	private static final String outFileAllHits = "/localscratch/CampsSimilarityScores/AllHitsFound.matrix";
	private static final String outFileRejectedHits = "/localscratch/CampsSimilarityScores/RejectedHitsFound_30232302.matrix";
	private static final String outFileRejectedHits2 = "/localscratch/CampsSimilarityScores/RejectedHitsFound_all_proteins.matrix";

	private static final String outFileRejectedHits_IndivisualReasons = "/localscratch/CampsSimilarityScores/RejectedHits_all_proteins_IndividualReasons.matrix";

	//private static final String outFileAllHits = "F:/AllHitsFound.matrix";
	//private static final String outFileRejectedHits = "F:/RejectedHitsFound_30232302.matrix";
	//private static final String outFileRejectedHits2 = "F:/RejectedHitsFound_all_proteins.matrix";


	private static HashMap <Integer, TMS> tms = new HashMap <Integer, TMS>();
	private static HashMap <Integer, String> seqid2uniprot = new HashMap <Integer, String>();

	private static HashMap <Integer, Integer> evaReject = new HashMap <Integer, Integer>(); // key: protein and val is hits rejected because of high eval
	private static HashMap <Integer, Integer> AlnCoverageReject = new HashMap <Integer, Integer>();
	private static HashMap <Integer, Integer> TmCoverageQueryReject = new HashMap <Integer, Integer>();
	private static HashMap <Integer, Integer> TmCoverageHitReject = new HashMap <Integer, Integer>();
	private static HashMap <Integer, Integer> hitCount = new HashMap <Integer, Integer>();


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

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Running..");
		//RunAlignmentStuff();
		RunAlignmentStuff2();
	}
	public static void Initialize(){
		covered_tms_perc_query = 0 ;
		covered_tms_perc_hit = 0 ;
		covered_tms_query = 0 ;
		covered_tms_hit = 0 ;
		thisNumberofTmQuery = 0;
		thisNumberofTmHit = 0;
	}

	private static ArrayList<Integer> proteinList = new ArrayList<Integer>();
	private static HashMap<Integer,Integer> proteinListMap = new HashMap<Integer,Integer>();

	private static void RunAlignmentStuff2(){
		// have to create the file
		// with proteins ids and and each column show the number of total hits and then the number of hits failed for each reason
		try{
			// 1. get all the rejected proteins.
			getRejectedProteins(); // fetches the proteins rejected between initial and in alignments
			initializeCountingMaps();
			populate_tms(); // used in calculating the tm coverage
			populateSeqid2uniprot();
			//ProcessSimilarityFileFor2();

			ProcessSimilarityFileFor3(); // to calculate only th indivisual cause of rejection for a hit. 
			// NOTE: ABOVE FUNCTION DOESNT WRITEs outFileRejectedHits_IndivisualReasons
			int tm = 1;
			//String outfile = "/localscratch/CampsSimilarityScores/RejectedHitsFound_"+tm+"_TMProteins.matrix";
			String outfile3 = "/localscratch/CampsSimilarityScores/RejectedHitsFound_Individual_"+tm+"_TMProteins_UniprotNames.matrix";

			//writeResultFile(tm,outfile); // writes the output file for the proteins with specified tm number
			writeResultFile(tm,outfile3); // writes the output file for the proteins with specified tm number

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void populateSeqid2uniprot() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("" +
					"select sequenceid,name from proteins_merged");
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				int id = rs1.getInt(1);
				String name = rs1.getString(2);
				if(seqid2uniprot.containsKey(id)){
					//String tmp = seqid2uniprot.get(id);
					//seqid2uniprot.put(id, tmp+","+name);
				}
				else{
					seqid2uniprot.put(id, name);
				}
			}
			rs1.close();
			pstm1.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}
	private static void writeResultFile(int tm,String o){
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(o)));
			bw.write("#PleaseNote: TotalHits may not correspond to the sum of all rejection reasons");
			bw.newLine();
			bw.write("#because, all possible rejection reasons were counted for a hit.");
			bw.newLine();
			bw.write("# ProteinId \t UniProt \t TotalHits \t RejectByEvalue \t RejectByAlignmentCoverage \t RejectByTMCoverageQuery \t RejectByTMCoverageHit");
			bw.newLine();

			for(int i =0;i<=proteinList.size()-1;i++){
				int id = proteinList.get(i);
				String name = "xxxxx";
				if(seqid2uniprot.containsKey(id)){
					name = seqid2uniprot.get(id);
				}
				if(!name.contains("xxxxx")){
					if(tms.get(id).start.size() == tm){
						bw.write(id+"\t"+name+"\t"+hitCount.get(id)+"\t"+evaReject.get(id)+"\t"+AlnCoverageReject.get(id)+"\t"+TmCoverageQueryReject.get(id)+"\t"+TmCoverageHitReject.get(id));
						bw.newLine();
					}
				}
				else{
					System.out.println("Rejected..."+id);
				}
			}
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void initializeCountingMaps() {
		// TODO Auto-generated method stub
		try{
			// this is done, because we are only interested in those proteins which were rejected
			for(int i =0;i<=proteinList.size()-1;i++){
				Integer id = proteinList.get(i);

				evaReject.put(id, 0);
				AlnCoverageReject.put(id, 0);
				TmCoverageQueryReject.put(id, 0);
				TmCoverageHitReject.put(id, 0);
				hitCount.put(id, 0);
			}
			System.out.println("Ev Size: "+evaReject.size());
			System.out.println("Aln Size: "+AlnCoverageReject.size());
			System.out.println("TMq Size: "+TmCoverageQueryReject.size());
			System.out.println("TMh Size: "+TmCoverageHitReject.size());
			System.out.println("Total Size: "+hitCount.size());
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	// the difference from 2 is that it counts individual cases for rejection of hits
	// i.e. only one reason for rejection of hit
	private static void ProcessSimilarityFileFor3() { // this is the seqid to check for its hits
		// TODO Auto-generated method stub
		try{

			StringBuilder sb = new StringBuilder();
			HashMap<Integer,Integer> sequences = new HashMap<Integer,Integer>();
			long proceesedLines = 0;
			int batchCounter1 = 0;
			BufferedReader br = new BufferedReader(new FileReader(new File(similarityFile)));
			String sCurrentLine = "";
			BufferedWriter bw = new BufferedWriter (new FileWriter(new File(outFileAllHits)));

			BufferedWriter bwReject = new BufferedWriter (new FileWriter(new File(outFileRejectedHits_IndivisualReasons)));

			while((sCurrentLine=br.readLine())!=null){
				proceesedLines++;
				String q2[] = sCurrentLine.split("\t");

				int seqidquery = Integer.parseInt(q2[0]);
				int seqidhit = Integer.parseInt(q2[1]);

				if(proteinListMap.containsKey(seqidquery) || proteinListMap.containsKey(seqidhit)){

					if(proceesedLines%100000 == 0){
						System.out.println("Lines Processsed:" + proceesedLines);
					}

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

						if(hitCount.containsKey(seqidquery)){ // counted for both query and hit
							int c = hitCount.get(seqidquery);
							c++;
							hitCount.put(seqidquery, c);
						}
						if(hitCount.containsKey(seqidhit)){ // counted for both query and hit
							int c = hitCount.get(seqidhit);
							c++;
							hitCount.put(seqidhit, c);
						}

						if(evalue < maxEvalue && bitscore > minBitScore){		
							Initialize();
							calculate_tmCoverage(query_begin,query_end, hit_begin, hit_end,seqidhit,seqidquery);
							// all calculated just put in table
							boolean showHit = true;
							if (alignment_coverage_query < minLengthRatio && alignment_coverage_hit< minLengthRatio) {
								showHit = false;
								bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
										+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
										+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
										+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-LowAlignmentCoverage-");
								bwReject.newLine();
								if(AlnCoverageReject.containsKey(seqidquery)){ // counted for both query and hit
									int c = AlnCoverageReject.get(seqidquery);
									c++;
									AlnCoverageReject.put(seqidquery, c);
								}
								if(AlnCoverageReject.containsKey(seqidhit)){ // counted for both query and hit
									int c = AlnCoverageReject.get(seqidhit);
									c++;
									AlnCoverageReject.put(seqidhit, c);
								}
								continue;
							}

							// query
							if (thisNumberofTmQuery>=3){
								if(covered_tms_query < 2 || covered_tms_perc_query < 40) {
									showHit = false;
									bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
											+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
											+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
											+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredQuery-");
									bwReject.newLine();

									if(TmCoverageQueryReject.containsKey(seqidquery)){ // 
										int c = TmCoverageQueryReject.get(seqidquery);
										c++;
										TmCoverageQueryReject.put(seqidquery, c);
									}
									continue;
								}
							}
							else{
								if(covered_tms_query < 1 ) {	// TM numbers are 1, 2 
									showHit = false;			// then at least 1 should be covered
									bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
											+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
											+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
											+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredQuery-");
									bwReject.newLine();
									if(TmCoverageQueryReject.containsKey(seqidquery)){ // 
										int c = TmCoverageQueryReject.get(seqidquery);
										c++;
										TmCoverageQueryReject.put(seqidquery, c);
									}
									continue;
								}
							}
							//hit
							if (thisNumberofTmHit>=3){
								if(covered_tms_hit < 2 || covered_tms_perc_hit < 40) {
									showHit = false;
									bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
											+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
											+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
											+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredHit-");
									bwReject.newLine();

									if(TmCoverageHitReject.containsKey(seqidhit)){ // 
										int c = TmCoverageHitReject.get(seqidhit);
										c++;
										TmCoverageHitReject.put(seqidhit, c);
									}
								}
								continue;
							}
							else{
								if(covered_tms_hit < 1 ) {		// TM numbers are 1, 2
									showHit = false;			// then at least 1 should be covered
									bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
											+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
											+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
											+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredHit-");
									bwReject.newLine();

									if(TmCoverageHitReject.containsKey(seqidhit)){ // 
										int c = TmCoverageHitReject.get(seqidhit);
										c++;
										TmCoverageHitReject.put(seqidhit, c);
									}
									continue;
								}
							}
							if(showHit == true){

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
							if(evaReject.containsKey(seqidquery)){
								int c = evaReject.get(seqidquery);
								c++;
								evaReject.put(seqidquery, c);
							}
							if(evaReject.containsKey(seqidhit)){
								int c = evaReject.get(seqidhit);
								c++;
								evaReject.put(seqidhit, c);
							}
						}
					}
				}
			}

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
	private static void ProcessSimilarityFileFor2() { // this is the seqid to check for its hits
		// TODO Auto-generated method stub
		try{

			StringBuilder sb = new StringBuilder();
			HashMap<Integer,Integer> sequences = new HashMap<Integer,Integer>();
			long proceesedLines = 0;
			int batchCounter1 = 0;
			BufferedReader br = new BufferedReader(new FileReader(new File(similarityFile)));
			String sCurrentLine = "";
			BufferedWriter bw = new BufferedWriter (new FileWriter(new File(outFileAllHits)));

			BufferedWriter bwReject = new BufferedWriter (new FileWriter(new File(outFileRejectedHits2)));

			while((sCurrentLine=br.readLine())!=null){
				proceesedLines++;
				String q2[] = sCurrentLine.split("\t");

				int seqidquery = Integer.parseInt(q2[0]);
				int seqidhit = Integer.parseInt(q2[1]);

				if(proteinListMap.containsKey(seqidquery) || proteinListMap.containsKey(seqidhit)){

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

						if(hitCount.containsKey(seqidquery)){ // counted for both query and hit
							int c = hitCount.get(seqidquery);
							c++;
							hitCount.put(seqidquery, c);
						}
						if(hitCount.containsKey(seqidhit)){ // counted for both query and hit
							int c = hitCount.get(seqidhit);
							c++;
							hitCount.put(seqidhit, c);
						}

						// in both cases of evalue accept or reject, calculate further. 
						if(evalue < maxEvalue && bitscore > minBitScore){
							// correct
						}	
						else{ // low eval OR bitscore reject
							Initialize();
							bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
									+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+"-"+"\t"+"-"
									+"\t"+"-"+"\t"+"-"+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
									+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-LowIdentityOrEval-");
							bwReject.newLine();
							if(evaReject.containsKey(seqidquery)){
								int c = evaReject.get(seqidquery);
								c++;
								evaReject.put(seqidquery, c);
							}
							if(evaReject.containsKey(seqidhit)){
								int c = evaReject.get(seqidhit);
								c++;
								evaReject.put(seqidhit, c);
							}
						}
						Initialize();
						calculate_tmCoverage(query_begin,query_end, hit_begin, hit_end,seqidhit,seqidquery);
						// all calculated just put in table
						boolean showHit = true;
						if (alignment_coverage_query < minLengthRatio && alignment_coverage_hit< minLengthRatio) {
							showHit = false;
							bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
									+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
									+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
									+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-LowAlignmentCoverage-");
							bwReject.newLine();
							if(AlnCoverageReject.containsKey(seqidquery)){ // counted for both query and hit
								int c = AlnCoverageReject.get(seqidquery);
								c++;
								AlnCoverageReject.put(seqidquery, c);
							}
							if(AlnCoverageReject.containsKey(seqidhit)){ // counted for both query and hit
								int c = AlnCoverageReject.get(seqidhit);
								c++;
								AlnCoverageReject.put(seqidhit, c);
							}
						}

						// query
						if (thisNumberofTmQuery>=3){
							if(covered_tms_query < 2 || covered_tms_perc_query < 40) {
								showHit = false;
								bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
										+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
										+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
										+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredQuery-");
								bwReject.newLine();

								if(TmCoverageQueryReject.containsKey(seqidquery)){ // 
									int c = TmCoverageQueryReject.get(seqidquery);
									c++;
									TmCoverageQueryReject.put(seqidquery, c);
								}
							}
						}
						else{
							if(covered_tms_query < 1 ) {	// TM numbers are 1, 2 
								showHit = false;			// then at least 1 should be covered
								bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
										+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
										+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
										+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredQuery-");
								bwReject.newLine();
								if(TmCoverageQueryReject.containsKey(seqidquery)){ // 
									int c = TmCoverageQueryReject.get(seqidquery);
									c++;
									TmCoverageQueryReject.put(seqidquery, c);
								}
							}
						}
						//hit
						if (thisNumberofTmHit>=3){
							if(covered_tms_hit < 2 || covered_tms_perc_hit < 40) {
								showHit = false;
								bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
										+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
										+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
										+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredHit-");
								bwReject.newLine();

								if(TmCoverageHitReject.containsKey(seqidhit)){ // 
									int c = TmCoverageHitReject.get(seqidhit);
									c++;
									TmCoverageHitReject.put(seqidhit, c);
								}
							}
						}
						else{
							if(covered_tms_hit < 1 ) {		// TM numbers are 1, 2
								showHit = false;			// then at least 1 should be covered
								bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
										+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
										+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
										+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredHit-");
								bwReject.newLine();
								if(TmCoverageHitReject.containsKey(seqidhit)){ // 
									int c = TmCoverageHitReject.get(seqidhit);
									c++;
									TmCoverageHitReject.put(seqidhit, c);
								}
							}
						}
						if(showHit == true){

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
							}
						}


					}
					if(proceesedLines%100000 == 0){
						System.out.println("Lines Processsed:" + proceesedLines);
					}
				}
			}

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

	private static void getRejectedProteins() {
		// TODO Auto-generated method stub
		try{
			// get initial set
			System.out.println("Waiting for query...");
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid from sequences2 where NOT sequenceid in (select distinct(sequenceid) from clusters_mcl2 where cluster_threshold=5)");
			ResultSet rs = pstm.executeQuery();
			System.out.println("Processing query results...");
			while(rs.next()){
				Integer id = rs.getInt(1);
				if(id != null ){
					proteinListMap.put(id, null);
					proteinList.add(id);
				}
			}

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void RunAlignmentStuff() { // runs the alignment stuff for one specified sequence and write all the rejected hits and the reasons of rejection correctly. 
		// the last time there was a problem in distribution 3
		// TODO Auto-generated method stub

		int seqid = 30232302; // the one to check for hits

		System.out.println("Populating TMS...");
		populate_tms();
		System.out.println("Processsing similarity File");
		ProcessSimilarityFile(seqid);
		//System.out.println("Exception sequences: ");
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

			System.out.println("Exception Query: "+queryID);
			System.out.println("Exception Hit: "+hitSequenceID);


			Initialize();
			e.printStackTrace();
			System.exit(0);
		}
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
								bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
										+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
										+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
										+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-LowAlignmentCoverage-");
								bwReject.newLine();
								continue;
							}

							// query
							if (thisNumberofTmQuery>=3){
								if(covered_tms_query < 2 || covered_tms_perc_query < 40) {
									showHit = false;
									bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
											+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
											+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
											+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredQuery-");
									bwReject.newLine();
									continue;
								}
							}
							else{
								if(covered_tms_query < 1 ) {	// TM numbers are 1, 2 
									showHit = false;			// then at least 1 should be covered
									bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
											+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
											+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
											+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredQuery-");
									bwReject.newLine();
									continue;
								}
							}
							//hit
							if (thisNumberofTmHit>=3){
								if(covered_tms_hit < 2 || covered_tms_perc_hit < 40) {
									showHit = false;
									bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
											+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
											+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
											+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredHit-");
									bwReject.newLine();
									continue;
								}
							}
							else{
								if(covered_tms_hit < 1 ) {		// TM numbers are 1, 2
									showHit = false;			// then at least 1 should be covered
									bwReject.write(seqidquery + "\t"+ seqidhit+"\t"+query_begin+"\t"+query_end+"\t"+hit_begin+"\t"+hit_end
											+"\t"+bitscore+"\t"+sw_score+"\t"+evalue+"\t"+identity+"\t"+positives+"\t"+covered_tms_query+"\t"+covered_tms_perc_query
											+"\t"+covered_tms_hit+"\t"+covered_tms_perc_hit+"\t"+similarity+"\t"+overlap+"\t"+alignment_coverage_query+"\t"+alignment_coverage_hit
											+"\t"+selfscoreRatio_query+"\t"+selfscoreRatio_hit+"\t"+"-TMSegmentNotCoveredHit-");
									bwReject.newLine();
									continue;
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

				if(sqNow == 60326602){
					int yy = 60326602;
					System.out.println(yy);
				}

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
			// for the last entry
			TMS temp = new TMS();
			temp.start = tempS;
			temp.end = tempE;

			tms.put(sqLast, temp);

			System.out.print("Total Seqids attained for tms " + tms.size() +"\n");

			pstm1.close();
			rs1.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
