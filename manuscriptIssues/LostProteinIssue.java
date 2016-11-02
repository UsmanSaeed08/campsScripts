package manuscriptIssues;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import utils.DBAdaptor;
import workflow.Dictionary2;

public class LostProteinIssue {

	/**
	 * @param args
	 * The idea is to get sequenceids in each genomes... 
	 * Now at each successive step.. update this list of sequence ids and report if any genome is removed.. 
	 * 
	 */

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4"); //changed to test_camps3

	private static HashMap <Integer, String> taxIds2Detail = new HashMap <Integer, String>(); //TaxonomyId to detail taxonomy
	private static ArrayList<Integer> taxIdsMissing = new ArrayList<Integer> ();
	private static HashMap <Integer, ArrayList<Integer>> taxIdsMissingMap = new HashMap <Integer, ArrayList<Integer>> (); // key is missing taxid and value is arraylist of seqIds in each genome

	private static HashMap <Integer, Integer> SeqIdtoTaxIdMissing = new HashMap <Integer, Integer> (); // key is missing sequenceid and value is the taxid

	private static ArrayList<Integer> FlagedTaxIds = new ArrayList<Integer>(); // ids which were not found during sucessive steps
	private static ArrayList<Integer> FlagedSeqIds = new ArrayList<Integer>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// *************** DATA EXTRACTION ********************************
		System.out.println("Running...");
		// 2 is called for checking after reRun process is complete
		getMissingSequencesAndTaxids2(); // got the sequences not in final data
		//getTaxaDetail();
		System.out.println("Displaying All filtered out data");
		display();
		System.out.println("Total Number of sequences: " + SeqIdtoTaxIdMissing.size());
		System.exit(0);

		// *************** Remove sequences filtered during Similarity SCORES check ********************************

		HashMap<Integer,Integer> SeqIdsMissing = RunThroughAlignmentsTable(SeqIdtoTaxIdMissing);	// returns a list of sequence-taxid map not in alignments table
		updateMissingMap(); // based on flaged taxids and flaged seids remove the sequences from Maps
		System.out.println("Displaying After checking similarity scores");
		display();


		// *************** Remove sequences filtered during TMS Core extraction ********************************		
		System.out.println();
		System.out.println("Total number of sequences that passed similarity score: "+ SeqIdsMissing.size());
		SeqIdsMissing = RunThroughTMSClusterCores(SeqIdsMissing);
		updateMissingMap(); // based on flaged taxids and flaged seids remove the sequences from Maps
		System.out.println("Displaying After checking TMS CORES");
		display();

		// *************** Remove sequences filtered during HMM generation ********************************		
		System.out.println();
		System.out.println("Total number of sequences that passed Core extraction: "+ SeqIdsMissing.size());
		SeqIdsMissing = RunThroughHMMs(SeqIdsMissing);
		updateMissingMap(); // based on flaged taxids and flaged seids remove the sequences from Maps
		System.out.println("Displaying After checking HMMs");
		display();


	}


	private static HashMap<Integer, Integer> RunThroughHMMs(
			HashMap<Integer, Integer> seqIdsMissing) {
		// TODO Auto-generated method stub
		try{
			HashMap<Integer,Integer> remainingSeq = new HashMap<Integer,Integer>();

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid from sequences2 where in_SC=\"Yes\"");
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				int s = rs1.getInt(1);
				if(seqIdsMissing.containsKey(s)){
					int taxid = seqIdsMissing.get(s);
					seqIdsMissing.remove(s);
					remainingSeq.put(s, taxid);
				}
			}
			rs1.close();
			pstm1.close();
			Iterator<Entry<Integer, Integer>> it = seqIdsMissing.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry)it.next();
				//System.out.println(pair.getKey() + " = " + pair.getValue());
				Integer seqid = (Integer) pair.getKey();
				Integer taxid = (Integer) pair.getValue();
				FlagedTaxIds.add(taxid);
				FlagedSeqIds.add(seqid);
				it.remove(); // avoids a ConcurrentModificationException
			}

			return remainingSeq;	
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}


	private static HashMap<Integer, Integer> RunThroughTMSClusterCores(
			HashMap<Integer, Integer> seqIdsMissing) {
		// TODO Auto-generated method stub
		try{
			HashMap<Integer,Integer> remainingSeq = new HashMap<Integer,Integer>();

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select distinct(sequenceid) from tms_cores");
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				int s = rs1.getInt(1);
				if(seqIdsMissing.containsKey(s)){
					int taxid = seqIdsMissing.get(s);
					seqIdsMissing.remove(s);

					remainingSeq.put(s, taxid);
				}
			}
			rs1.close();
			pstm1.close();

			// now remove sequences which were not found
			Iterator<Entry<Integer, Integer>> it = seqIdsMissing.entrySet().iterator();
			int i =0;
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry)it.next();
				//System.out.println(pair.getKey() + " = " + pair.getValue());
				Integer seqid = (Integer) pair.getKey();
				Integer taxid = (Integer) pair.getValue();
				FlagedTaxIds.add(i,taxid);
				FlagedSeqIds.add(i,seqid);
				i++;
				it.remove(); // avoids a ConcurrentModificationException
			}
			return remainingSeq;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	private static void getTaxaDetail(){
		try{
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select taxonomyid," +
					"species,genus,family,order_,class,phylum,kingdom,superkingdom from taxonomies2");
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				Integer taxId = rs1.getInt(1);
				/*String species = rs1.getString(2);
				String genus = rs1.getString(3);
				String family = rs1.getString(4);
				String order_ = rs1.getString(5);
				String classs = rs1.getString(6);
				String phylum = rs1.getString(7);
				String kingdom = rs1.getString(8);
				String superkingdom = rs1.getString(9);*/
				String detail = rs1.getString(2)+"-"+rs1.getString(3)+"-"+rs1.getString(4)+"-"+rs1.getString(5)+"-"+rs1.getString(6)+"-"+rs1.getString(7)+"-"+rs1.getString(8)+"-"+rs1.getString(9);
				taxIds2Detail.put(taxId, detail);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void display(){
		for(int i =0;i<=taxIdsMissing.size()-1;i++){
			Integer thisTaxa = taxIdsMissing.get(i);
			//System.out.println(thisTaxa + "\t"+ taxIds2Detail.get(thisTaxa) + "\t # of sequences of this taxa in initial \t" + taxIdsMissingMap.get(thisTaxa.toString()));
			if(taxIdsMissingMap.containsKey(thisTaxa)){
				ArrayList<Integer> temp = taxIdsMissingMap.get(thisTaxa);
				System.out.println(thisTaxa + "\t" + temp.size());
			}
			else{
				System.out.println(thisTaxa + "\t" + 0);
			}

		}
	}

	private static void updateMissingMap() {
		// TODO Auto-generated method stub
		try{
			if(FlagedTaxIds.size()!=FlagedSeqIds.size()){
				System.err.print("Critical Error... Length of TaxId array does not match SeqId Array");
				System.exit(0);
			}
			for(int i=0; i<=FlagedTaxIds.size()-1;i++){
				int t = FlagedTaxIds.get(i);
				int s = FlagedSeqIds.get(i);
				if(taxIdsMissingMap.containsKey(t)){
					ArrayList<Integer> temp = taxIdsMissingMap.get(t);
					temp.remove(new Integer(s)); // new Integer is to remove this value and not the index

					if(temp.size()>0){
						taxIdsMissingMap.put(t,temp);
					}
					else{
						taxIdsMissingMap.remove(t);
					}
				}

			}
			FlagedTaxIds = new ArrayList<Integer>();
			FlagedSeqIds = new ArrayList<Integer>();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	// the function goes through complete alignments table and sees how many seqids are present here or not
	private static HashMap<Integer,Integer> RunThroughAlignmentsTable(HashMap<Integer,Integer> SeqIdtoTaxIdMissingLocal) {
		// TODO Auto-generated method stub
		try{
			HashMap<Integer,Integer> remainingSeq = new HashMap<Integer,Integer>();

			PreparedStatement pstm1 = null;
			int idx =1;
			for (int i=1; i<=17;i++){				
				pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit FROM alignments_initial limit "+idx+","+10000000);
				//pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial");
				idx = i*10000000;
				System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
				System.out.flush();
				ResultSet rs1 = pstm1.executeQuery();

				while(rs1.next()){
					int q = rs1.getInt(1);
					int h = rs1.getInt(2);
					if(SeqIdtoTaxIdMissingLocal.containsKey(q)){
						int taxid = SeqIdtoTaxIdMissingLocal.get(q);
						SeqIdtoTaxIdMissingLocal.remove(q);						
						remainingSeq.put(q, taxid);
					}
					if(SeqIdtoTaxIdMissingLocal.containsKey(h)){
						int taxid = SeqIdtoTaxIdMissingLocal.get(h);
						SeqIdtoTaxIdMissingLocal.remove(h);
						remainingSeq.put(h, taxid);
					}
				}
				rs1.close();
			}
			pstm1.close();
			// now remove the ids which were not found
			// the remaining seqids in seqIdstoTaxIdMissingLocal are those which were not found in alignments table
			// therefore, these sequences were filterd out in this step
			Iterator<Entry<Integer, Integer>> it = SeqIdtoTaxIdMissingLocal.entrySet().iterator();
			int i=0;
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry)it.next();
				//System.out.println(pair.getKey() + " = " + pair.getValue());
				Integer seqid = (Integer) pair.getKey();
				Integer taxid = (Integer) pair.getValue();

				FlagedTaxIds.add(i,taxid);
				FlagedSeqIds.add(i,seqid);
				i++;
				it.remove(); // avoids a ConcurrentModificationException
			}
			System.out.println("At end of Alignments.. Sequences removed in total: "+FlagedSeqIds.size());

			return remainingSeq;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;

	}

	private static void getMissingSequencesAndTaxids(){
		try{
			HashMap <Integer, ArrayList<Integer>> taxIds2seq = new HashMap <Integer, ArrayList<Integer>>(); //Complete Initial Data
			HashMap <Integer, ArrayList<Integer>> taxIds2seqFinal = new HashMap <Integer, ArrayList<Integer>>(); //Complete Final Data

			HashMap <Integer, Integer> processedSeq = new HashMap <Integer, Integer>(); //Complete Final Data

			ArrayList<Integer> taxIdsInitial = new ArrayList<Integer> ();
			ArrayList<Integer> taxIdsFinal = new ArrayList<Integer> ();

			// get all taxids for eukaryotes 
			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
			//		"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Eukaryota\")");

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
					"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Archaea\" or superkingdom=\"Bacteria\")");

			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				Integer seqid = rs1.getInt(1);
				Integer taxid = rs1.getInt(2);
				if(!processedSeq.containsKey(seqid)){
					if(taxIds2seq.containsKey(taxid)){
						ArrayList<Integer> temp = taxIds2seq.get(taxid);
						if (!temp.contains(seqid)){
							temp.add(seqid);
							taxIds2seq.put(taxid, temp);
							processedSeq.put(seqid, null);
						}
					}
					else{
						if(!taxIdsInitial.contains(taxid)){
							taxIdsInitial.add(taxid);
						}
						ArrayList<Integer> temp = new ArrayList<Integer>();
						temp.add(seqid);
						taxIds2seq.put(taxid, temp);
						processedSeq.put(seqid, null);
					}
				}
			}
			rs1.close();
			pstm1.close();
			processedSeq.clear();


			//PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
			//		"where sequenceid in (select sequenceid from sequences2 where in_SC=\"Yes\") and " +
			//		"taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Eukaryota\")");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
					"where sequenceid in (select sequenceid from sequences2 where in_SC=\"Yes\") and " +
					"taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Archaea\" or superkingdom=\"Bacteria\")");

			ResultSet rs2 = pstm2.executeQuery();
			while(rs2.next()){
				Integer seqid = rs2.getInt(1);
				Integer taxid = rs2.getInt(2);

				if(taxIds2seqFinal.containsKey(taxid)){
					ArrayList<Integer> temp = taxIds2seqFinal.get(taxid);
					if (!temp.contains(seqid)){
						temp.add(seqid);
						taxIds2seqFinal.put(taxid, temp);	
					}
				}
				else{
					if(!taxIdsFinal.contains(taxid)){
						taxIdsFinal.add(taxid);
					}
					ArrayList<Integer> temp = new ArrayList<Integer>();
					temp.add(seqid);
					taxIds2seqFinal.put(taxid, temp);
				}

			}
			rs2.close();
			pstm2.close();


			// Now Have obtained initial and final data -- we can get missing data

			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/Scratch/missingGenomes/filteredGenomePro.fasta")));
			PreparedStatement pgetSeq = CAMPS_CONNECTION.prepareStatement("select sequence from sequences2 where sequenceid=?");

			System.out.println("Initial Taxa:" + taxIdsInitial.size());
			System.out.println("Final Taxa:" + taxIdsFinal.size());
			ArrayList<Integer> taxxx = new ArrayList<Integer>();
			for(int i=0;i<=taxIdsInitial.size()-1;i++){
				Integer thisTaxa = taxIdsInitial.get(i);
				if(!taxIdsFinal.contains(thisTaxa)){
					taxIdsMissing.add(thisTaxa);
					taxIdsMissingMap.put(thisTaxa,taxIds2seq.get(thisTaxa));
					// now add sequenceids to map... SeqIdtoTaxIdMissing
					ArrayList<Integer> seqids = taxIds2seq.get(thisTaxa);
					//System.out.println("ThisTaxa: "+ thisTaxa);
					for(int j =0; j<=seqids.size()-1;j++){
						if(SeqIdtoTaxIdMissing.containsKey(seqids.get(j))){
							//System.err.println("Multiple sequenceId key....for "+seqids.get(j) + "and taxa"+SeqIdtoTaxIdMissing.get(seqids.get(j)));
							//System.err.println("ThisTaxa: "+ thisTaxa);
							//System.out.flush();
							if(!taxxx.contains(thisTaxa)){
								taxxx.add(thisTaxa);
							}
							if(!taxxx.contains(SeqIdtoTaxIdMissing.get(seqids.get(j)))){
								taxxx.add(SeqIdtoTaxIdMissing.get(seqids.get(j)));
							}
							//System.exit(0);
							// if already has this seqid - it means the sequence has multiple taxonomies 
							// since both taxas are basically the same.. we ignore one of them
							// and the sequence is already represented in the existing taxa...
							// so now, just have to remove the occurance of seqids in the other taxa...
							// Remove the sequence from thisTaxa because SeqIdtoTaxIdMissing.get(seqids.get(j)) is already in hashmap
							// instead of removing now.. it is better to ignore the cases at time of populating array and map


						}
						else{
							Integer sid = seqids.get(j);
							pgetSeq.setInt(1, sid);
							ResultSet rget = pgetSeq.executeQuery();
							String seq = "";
							while(rget.next()){
								seq = rget.getString("sequence");
							}
							rget.close();
							if(seq.length()<5000){ // ignore sequences with length over 5k
								bw.write(">"+sid.toString()+"-"+thisTaxa);
								bw.newLine();
								bw.write(seq);
								bw.newLine();
							}
							SeqIdtoTaxIdMissing.put(seqids.get(j), thisTaxa);
						}
					}
				}
			}
			bw.close();
			System.out.println("Sequences with multiple taxas: ");
			System.out.println(taxxx);
			taxIdsInitial.clear();
			taxIds2seq.clear();
			taxIdsFinal.clear();
			taxIds2seqFinal.clear();

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Made for the checking after the re Run Process
	 */
	private static void getMissingSequencesAndTaxids2(){
		try{
			HashMap <Integer, ArrayList<Integer>> taxIds2seq = new HashMap <Integer, ArrayList<Integer>>(); //Complete Initial Data
			HashMap <Integer, ArrayList<Integer>> taxIds2seqFinal = new HashMap <Integer, ArrayList<Integer>>(); //Complete Final Data

			HashMap <Integer, Integer> processedSeq = new HashMap <Integer, Integer>(); //Complete Final Data

			ArrayList<Integer> taxIdsInitial = new ArrayList<Integer> ();
			ArrayList<Integer> taxIdsFinal = new ArrayList<Integer> ();

			// get all taxids for eukaryotes 
			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
			//		"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Viruses\")");
			
			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
			//		"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Eukaryota\")");

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
					"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Archaea\" or superkingdom=\"Bacteria\")");

			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				Integer seqid = rs1.getInt(1);
				Integer taxid = rs1.getInt(2);
				if(!processedSeq.containsKey(seqid)){
					if(taxIds2seq.containsKey(taxid)){
						ArrayList<Integer> temp = taxIds2seq.get(taxid);
						if (!temp.contains(seqid)){
							temp.add(seqid);
							taxIds2seq.put(taxid, temp);
							processedSeq.put(seqid, null);
						}
					}
					else{
						if(!taxIdsInitial.contains(taxid)){
							taxIdsInitial.add(taxid);
						}
						ArrayList<Integer> temp = new ArrayList<Integer>();
						temp.add(seqid);
						taxIds2seq.put(taxid, temp);
						processedSeq.put(seqid, null);
					}
				}
			}
			rs1.close();
			pstm1.close();
			processedSeq.clear();


			//PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
			//		"where sequenceid in (select distinct(sequenceid) from tms_cores2) and " +
			//		"taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Viruses\")");
			
			//PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
			//		"where sequenceid in (select distinct(sequenceid) from tms_cores2) and " +
			//		"taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Eukaryota\")");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
					"where sequenceid in (select distinct(sequenceid) from tms_cores2) and " +
					"taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Archaea\" or superkingdom=\"Bacteria\")");

			ResultSet rs2 = pstm2.executeQuery();
			while(rs2.next()){
				Integer seqid = rs2.getInt(1);
				Integer taxid = rs2.getInt(2);

				if(taxIds2seqFinal.containsKey(taxid)){
					ArrayList<Integer> temp = taxIds2seqFinal.get(taxid);
					if (!temp.contains(seqid)){
						temp.add(seqid);
						taxIds2seqFinal.put(taxid, temp);	
					}
				}
				else{
					if(!taxIdsFinal.contains(taxid)){
						taxIdsFinal.add(taxid);
					}
					ArrayList<Integer> temp = new ArrayList<Integer>();
					temp.add(seqid);
					taxIds2seqFinal.put(taxid, temp);
				}

			}
			rs2.close();
			pstm2.close();


			// Now Have obtained initial and final data -- we can get missing data

			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/Scratch/missingGenomes/filteredGenomePro.fasta")));
			PreparedStatement pgetSeq = CAMPS_CONNECTION.prepareStatement("select sequence from sequences2 where sequenceid=?");

			System.out.println("Initial Taxa:" + taxIdsInitial.size());
			System.out.println("Final Taxa:" + taxIdsFinal.size());
			ArrayList<Integer> taxxx = new ArrayList<Integer>();
			for(int i=0;i<=taxIdsInitial.size()-1;i++){
				Integer thisTaxa = taxIdsInitial.get(i);
				if(!taxIdsFinal.contains(thisTaxa)){
					taxIdsMissing.add(thisTaxa);
					taxIdsMissingMap.put(thisTaxa,taxIds2seq.get(thisTaxa));
					// now add sequenceids to map... SeqIdtoTaxIdMissing
					ArrayList<Integer> seqids = taxIds2seq.get(thisTaxa);
					//System.out.println("ThisTaxa: "+ thisTaxa);
					for(int j =0; j<=seqids.size()-1;j++){
						if(SeqIdtoTaxIdMissing.containsKey(seqids.get(j))){
							//System.err.println("Multiple sequenceId key....for "+seqids.get(j) + "and taxa"+SeqIdtoTaxIdMissing.get(seqids.get(j)));
							//System.err.println("ThisTaxa: "+ thisTaxa);
							//System.out.flush();
							if(!taxxx.contains(thisTaxa)){
								taxxx.add(thisTaxa);
							}
							if(!taxxx.contains(SeqIdtoTaxIdMissing.get(seqids.get(j)))){
								taxxx.add(SeqIdtoTaxIdMissing.get(seqids.get(j)));
							}
							//System.exit(0);
							// if already has this seqid - it means the sequence has multiple taxonomies 
							// since both taxas are basically the same.. we ignore one of them
							// and the sequence is already represented in the existing taxa...
							// so now, just have to remove the occurance of seqids in the other taxa...
							// Remove the sequence from thisTaxa because SeqIdtoTaxIdMissing.get(seqids.get(j)) is already in hashmap
							// instead of removing now.. it is better to ignore the cases at time of populating array and map


						}
						else{
							Integer sid = seqids.get(j);
							pgetSeq.setInt(1, sid);
							ResultSet rget = pgetSeq.executeQuery();
							String seq = "";
							while(rget.next()){
								seq = rget.getString("sequence");
							}
							rget.close();
							if(seq.length()<5000){ // ignore sequences with length over 5k
								bw.write(">"+sid.toString()+"-"+thisTaxa);
								bw.newLine();
								bw.write(seq);
								bw.newLine();
							}
							SeqIdtoTaxIdMissing.put(seqids.get(j), thisTaxa);
						}
					}
				}
			}
			bw.close();
			System.out.println("Sequences with multiple taxas: ");
			System.out.println(taxxx);
			taxIdsInitial.clear();
			taxIds2seq.clear();
			taxIdsFinal.clear();
			taxIds2seqFinal.clear();

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
