package manuscriptIssues;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import extract_proteins.TMS;

import utils.DBAdaptor;
import workflow.Dictionary;

public class FungiDetailReport {

	/**
	 * @param args
	 * Makes the complete detailed report of number of hits rejected at each filter
	 * using fungal similarity score matrix -- subset of camps similarity scores matrix
	 * 
	 */
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4"); //changed to test_camps3

	private static HashMap<Integer,Integer> seq2taxidFungi = new HashMap<Integer,Integer>();
	private static ArrayList<Integer> alltaxidsFungi = new ArrayList<Integer>();
	private static ArrayList<Integer> missingTaxidsArray = new ArrayList<Integer>();
	//	private static ArrayList<Integer> allSeqidsArray = new ArrayList<Integer>();
	private static HashMap<Integer,Integer> missingTax2seqInAlignments = new HashMap<Integer,Integer>(); // taxid key to number of sequences in alignments table 

	private static HashMap <Integer, TMS> tms = new HashMap <Integer, TMS>();  

	private static double covered_tms_perc_query;
	private static double covered_tms_perc_hit;
	private static int covered_tms_query;
	private static int covered_tms_hit;
	private static int thisNumberofTmQuery;
	private static int thisNumberofTmHit;
	private static float minLengthRatio;

	public static void Initialize(){
		covered_tms_perc_query = 0 ;
		covered_tms_perc_hit = 0 ;
		covered_tms_query = 0 ;
		covered_tms_hit = 0 ;
		thisNumberofTmQuery = 0;
		thisNumberofTmHit = 0;

		minLengthRatio = 0.5f;

	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Populating TMS...");
		populate_tms();

		System.out.println("Making Detailed Report...");

		//String misingGenomeFile = "F:/Scratch/missingGenomes/FilteredEuTaxIds.txt";
		String misingGenomeFile = "/home/users/saeed/scratch/runForFilteredOutGenomes/FilteredEuTaxIds.txt";

		getFungalTax2(misingGenomeFile);

		//String CAMPSSimilarityFile = "F:/Scratch/missingGenomes/similarityscore/tempfile";
		//String CAMPSSimilarityFile = "/localscratch/CampsSimilarityScores/camps_seq_file.matrix";
		String CAMPSSimilarityFile = "/localscratch/CampsSimilarityScores/fungi_seq_file.matrix";
		//
		//String FungalSimilarityFileHits = "F:/Scratch/missingGenomes/similarityscore/hitsfile";
		missingTax2seqInAlignments = populatemissingTax2seqInAlignments();
		ProcessSimilarityFileofFungi(CAMPSSimilarityFile);

	}

	private static HashMap<Integer, Integer> populatemissingTax2seqInAlignments() {
		// TODO Auto-generated method stub
		try{
			HashMap<Integer,Integer> taxids2Seqids = new HashMap<Integer,Integer>();

			HashMap<Integer,Integer> ProcessedSeqids = new HashMap<Integer,Integer>();
			// initialize with missing genomes... cuz only their count has to be checked
			for(int i =0;i<=alltaxidsFungi.size()-1;i++){
				int t = alltaxidsFungi.get(i);
				taxids2Seqids.put(t, 0);
			}

			PreparedStatement pstm = null;
			int idx =1;
			for (int i=1; i<=17;i++){				
				pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit FROM alignments_initial limit "+idx+","+10000000);
				//pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial");
				idx = i*10000000;
				System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
				ResultSet rs = pstm.executeQuery();
				while(rs.next()) {
					int idhit = rs.getInt("seqid_hit");
					int query = rs.getInt ("seqid_query");
					if(seq2taxidFungi.containsKey(idhit)){
						int temptax = seq2taxidFungi.get(idhit);
						if(taxids2Seqids.containsKey(temptax)){
							if(!ProcessedSeqids.containsKey(idhit)){
								int count = taxids2Seqids.get(temptax);
								count = count + 1;
								taxids2Seqids.put(temptax, count);
								ProcessedSeqids.put(idhit, null);
							}
						}
					}
					if(seq2taxidFungi.containsKey(query)){
						int temptax = seq2taxidFungi.get(query);
						if(taxids2Seqids.containsKey(temptax)){
							if(!ProcessedSeqids.containsKey(query)){
								int count = taxids2Seqids.get(temptax);
								count = count + 1;
								taxids2Seqids.put(temptax, count);
								ProcessedSeqids.put(query, null);
							}
						}	
					}

				}
				// dict population complete
				rs.close();
			}
			pstm.close();
			return taxids2Seqids;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	private static void ProcessSimilarityFileofFungi(String fungalSimilarityFile) {
		// TODO Auto-generated method stub
		try{
			// go through the file -- each line
			// get the taxid for query
			// get taxid for hit
			// process the relationship... 
			// 				-- count hit in hashmap of taxidtoCountEvalBSRejected -- evalue and bitscore
			//				-- count hit in hashmap of taxidtoCountRejected -- minLengthRatio
			//				-- count hit in hashmap of taxidtoCountRejected -- TM Coverage -- query or hit

			// for all the hashmaps below --> they are used to count hit stats of each respective filter case..i.e evalue&bitScore or tm coverage			
			// key taxid ... vale hit count
			HashMap <Integer,Integer> totalCount = new HashMap<Integer,Integer>();
			HashMap <Integer,Integer> EvalRejectCount = new HashMap<Integer,Integer>();
			HashMap <Integer,Integer> IdentityRejectCount = new HashMap<Integer,Integer>();
			HashMap <Integer,Integer> swRejectCount = new HashMap<Integer,Integer>();
			HashMap <Integer,Integer> MinLenRejectCount = new HashMap<Integer,Integer>();
			HashMap <Integer,Integer> TMCoverRejectCount = new HashMap<Integer,Integer>();
			HashMap <Integer,Integer> PassedCount = new HashMap<Integer,Integer>();
			HashMap <Integer,HashMap <Integer,Integer>> PassedCountSequences = new HashMap<Integer,HashMap <Integer,Integer>>(); // --key taxid --val map(key seqid, val null)
			// initialize all hashmaps -- so that dont have to check if the key exists or not ;)
			int n = 0;
			for(int i=0;i<=alltaxidsFungi.size()-1;i++){
				int taxa = alltaxidsFungi.get(i);
				totalCount.put(taxa, n);
				EvalRejectCount.put(taxa, n);
				IdentityRejectCount.put(taxa, n);
				swRejectCount.put(taxa, n);
				MinLenRejectCount.put(taxa, n);
				TMCoverRejectCount.put(taxa, n);
				PassedCount.put(taxa,n);
				PassedCountSequences.put(taxa,new HashMap<Integer,Integer>());
			}
			BufferedReader br = new BufferedReader(new FileReader(new File(fungalSimilarityFile)));
			String sCurrentLine = "";
			while((sCurrentLine=br.readLine())!=null){
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
				float alignment_coverage_query = Float.parseFloat(q2[8]);
				float alignment_coverage_hit = Float.parseFloat(q2[9]);


				q2 = null;

				int taxquery = seq2taxidFungi.get(seqidquery);
				int taxhit = seq2taxidFungi.get(seqidhit);

				// add to total count
				int temp = totalCount.get(taxquery);
				temp = temp + 1;
				totalCount.put(taxquery, temp);
				temp = totalCount.get(taxhit);
				temp = temp + 1;
				totalCount.put(taxhit, temp);
				temp = 0;
				if(evalue < 1E-5 && bitscore > 50){
					if(sw_score > 136){
						if(identity>40f){
							Initialize();
							calculate_tmCoverage(query_begin,query_end, hit_begin, hit_end,seqidhit,seqidquery);
							if (alignment_coverage_query < minLengthRatio && alignment_coverage_hit< minLengthRatio) {
								//count reject min length ratio
								temp = MinLenRejectCount.get(taxquery);
								temp = temp + 1;
								MinLenRejectCount.put(taxquery, temp);

								temp = 0;
								temp = MinLenRejectCount.get(taxhit);
								temp = temp + 1;
								MinLenRejectCount.put(taxhit, temp);
								temp = 0;
							}
							else{
								/*
								boolean showHit = true;
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
								if(!showHit){
									*/
									//}
									if(covered_tms_query < 1 || covered_tms_perc_query < 40 || covered_tms_hit < 1 || covered_tms_perc_hit < 40) {
									// count tm coverage reject
									temp = TMCoverRejectCount.get(taxquery);
									temp = temp + 1;
									TMCoverRejectCount.put(taxquery, temp);

									temp = 0;
									temp = TMCoverRejectCount.get(taxhit);
									temp = temp + 1;
									TMCoverRejectCount.put(taxhit, temp);
									temp = 0; 
								}
								else{
									// count passed
									temp = PassedCount.get(taxquery);
									temp = temp + 1;
									PassedCount.put(taxquery, temp);

									temp = 0;
									temp = PassedCount.get(taxhit);
									temp = temp + 1;
									PassedCount.put(taxhit, temp);
									temp = 0; 
									// count passed sequences
									PassedCountSequences = CountPassedSequences(taxquery,taxhit,seqidquery,seqidhit,PassedCountSequences);
								}						
							}
						}
						else{
							// count identity thresh reject
							temp = IdentityRejectCount.get(taxquery);
							temp = temp + 1;
							IdentityRejectCount.put(taxquery, temp);

							temp = 0;
							temp = IdentityRejectCount.get(taxhit);
							temp = temp + 1;
							IdentityRejectCount.put(taxhit, temp);
							temp = 0; 
						}
					}
					else{
						//count sw reject
						temp = swRejectCount.get(taxquery);
						temp = temp + 1;
						swRejectCount.put(taxquery, temp);

						temp = 0;
						temp = swRejectCount.get(taxhit);
						temp = temp + 1;
						swRejectCount.put(taxhit, temp);
						temp = 0;
					}
				}
				else{
					// count reject min Eval
					temp = EvalRejectCount.get(taxquery);
					temp = temp + 1;
					EvalRejectCount.put(taxquery, temp);

					temp = 0;
					temp = EvalRejectCount.get(taxhit);
					temp = temp + 1;
					EvalRejectCount.put(taxhit, temp);
					temp = 0;
				}
			}
			br.close();
			printReport(totalCount,EvalRejectCount,MinLenRejectCount,TMCoverRejectCount,PassedCount,PassedCountSequences,IdentityRejectCount);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static HashMap<Integer, HashMap<Integer, Integer>> CountPassedSequences(
			int taxquery, int taxhit, int seqidquery, int seqidhit,
			HashMap<Integer, HashMap<Integer, Integer>> passedCountSequences) {
		// TODO Auto-generated method stub
		HashMap<Integer,Integer> temp = passedCountSequences.get(taxquery);
		if (!temp.containsKey(seqidquery)){
			temp.put(seqidquery, null);
		}
		passedCountSequences.put(taxquery,temp);


		HashMap<Integer,Integer> temp2 = passedCountSequences.get(taxhit);
		if (!temp2.containsKey(seqidhit)){
			temp2.put(seqidhit, null);
		}
		passedCountSequences.put(taxhit,temp2);

		return passedCountSequences;
	}
	private static void printReport(HashMap<Integer, Integer> totalCount,
			HashMap<Integer, Integer> evalRejectCount,
			HashMap<Integer, Integer> minLenRejectCount,
			HashMap<Integer, Integer> tMCoverRejectCount,
			HashMap<Integer, Integer> passedCount, 
			HashMap<Integer, HashMap<Integer, Integer>> passedCountSequences,
			HashMap<Integer, Integer> identityRejectCount) {
		// TODO Auto-generated method stub
		try{
			System.out.println();
			for(int i=0;i<=alltaxidsFungi.size()-1;i++){
				int taxa = alltaxidsFungi.get(i);
				int total = totalCount.get(taxa);
				int eval = evalRejectCount.get(taxa);
				int minLen = minLenRejectCount.get(taxa);
				int tmCoverage = tMCoverRejectCount.get(taxa);
				int passed = passedCount.get(taxa);
				int seq = passedCountSequences.get(taxa).size();
				int ident = identityRejectCount.get(taxa);

				if(missingTaxidsArray.contains(taxa)){
					System.out.println("---MISSING TAXA BELOW---");
				}
				System.out.println("Taxa: "+ taxa);
				System.out.println("Total Hits: "+ total);
				System.out.println("Hits With Low Eval: "+ eval);
				System.out.println("Hits with low identity: "+ ident);
				System.out.println("Hits With Low min LengthRatio: "+ minLen);
				System.out.println("Hits With coverage issue (TM not covered): "+ tmCoverage);
				System.out.println("Passed Hits: "+ passed);
				System.out.println("Passed Sequences in this Taxa: "+ seq);
				System.out.println("Sequences of this taxa in AlignmentsTable: "+ missingTax2seqInAlignments.get(taxa));
				System.out.println("*********************************************************************");
				System.out.println();
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}
	private static void calculate_tmCoverage(int query_begin, int query_end,
			int hit_begin, int hit_end, int hitSequenceID, int queryID) throws SQLException {
		// TODO Auto-generated method stub

		//get coordinates of query tms from table

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

	// this function collects all fungal genomes and makes seqid to taxid
	// then array of all taxids
	// array of all missing taxids


	private static void getFungalTax2(String misingGenomeFile) {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
					"where taxonomyid in (select taxonomyid from taxonomies2 where kingdom=\"Fungi\" or taxonomyid=164328)"); // taxid 164328 is an exception.. it is fungi like

			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				Integer seqid = rs1.getInt(1);
				Integer taxid = rs1.getInt(2);
				if(!seq2taxidFungi.containsKey(seqid)){
					seq2taxidFungi.put(seqid, taxid);
					//allSeqidsArray.add(seqid);
				}
				if(!alltaxidsFungi.contains(taxid)){
					alltaxidsFungi.add(taxid);
				}
			}
			rs1.close();
			pstm1.close();

			BufferedReader br = new BufferedReader(new FileReader(new File(misingGenomeFile)));
			String l = "";
			while((l=br.readLine())!=null){
				missingTaxidsArray.add(Integer.parseInt(l.trim()));
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
