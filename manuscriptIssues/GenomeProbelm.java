package manuscriptIssues;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

import utils.DBAdaptor;

public class GenomeProbelm {

	/**
	 * @param args
	 * Although there are other modules which deal with different kinds of issues i have been dealing with
	 * Cant even remember how many i have faced... 
	 * THIS one particularly deals with the issue that there are 148 eukaryotes in initial data, but 138
	 * in the final one... so how does this happen and what are these genomes
	 * 
	 *  1. names and taxids for these genomes
	 *  2. number of sequences in each genome
	 */

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4"); //changed to test_camps3

	private static HashMap <Integer, ArrayList<Integer>> taxIds2seq = new HashMap <Integer, ArrayList<Integer>>(); //Complete Initial Data
	private static HashMap <Integer, ArrayList<Integer>> taxIds2seqFinal = new HashMap <Integer, ArrayList<Integer>>(); //Complete Final Data
	private static HashMap <Integer, String> taxIds2Detail = new HashMap <Integer, String>(); //Complete Final Data


	private static ArrayList<Integer> taxIdsInitial = new ArrayList<Integer> ();
	private static ArrayList<Integer> taxIdsFinal = new ArrayList<Integer> ();

	private static ArrayList<Integer> taxIdsMissing = new ArrayList<Integer> ();
	private static HashMap<String,String> taxIdsMissingMap = new HashMap<String,String>(); // key is missing taxid and true or false if present in refSeq or not

	private static HashMap<Integer,Integer> seqIdsFinal = new HashMap<Integer,Integer>();
	private static ArrayList<String> files = new ArrayList<String>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Running...");

		// WARNING: TO USE THE v1 OF FUNCTION BELOW, COMMENT OUT THE FUNCTION getFinalSeqIdsFromFile();
		//getSequencesAndTaxids_v1(); // final set is taken by getting sequences with column in_SC=Yes


		String path = "F:/SC_Clust_postHmm/MetaModelsJune2016/HMMs/CAMPS4_1/";
		getFinalSeqIdsFromFile(path);
		// to use the v2 of below function, must use getFinalSeqIdsFromFile
		getSequencesAndTaxids_v2();	// as this version is used to get proteins after rerunning the CAMPS pipeline
		// therefore, the final set is attained by getting clusterIds and thresh from file names and then getting seqids.
		
		// the function fetches the taxas of all the viral sequences where were added
		// i.e. after adding missing genomes. This would get us the prospective count of 
		// viral genomes to be in CAMPS
		String hmmOutFileMissingGenomes = "F:/Scratch/addMissingGenomes/run/hmmOut.txt";
		
		System.out.println("Adding missed genomes... ");
		getViralTaxaFromFile(hmmOutFileMissingGenomes);

		System.out.println("Number of Taxids Initial: "+ taxIdsInitial.size());
		System.out.println("Number of Taxids Final: "+ taxIdsFinal.size());
		System.out.println("Number of final SeqIds: "+seqIdsFinal.size());
		// below is temporarily commented out
		//getTaxaDetail();
		//getMissingTaxa();
		//inRefSeq();
		//display();

		closeConnection();
	}


	private static void getViralTaxaFromFile(String hmmOutFile) {
		// TODO Auto-generated method stub
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(hmmOutFile)));
			String l = "";
			while((l=br.readLine())!=null){
				if(!l.startsWith("#")&&!l.contains("NO_HMM_FOUND")){
					String[] p = l.split("\t");
					int seq = Integer.parseInt(p[0].trim());
					int tax = Integer.parseInt(p[1].trim());
					
					if(!taxIdsFinal.contains(tax)){
						taxIdsFinal.add(tax);
					}
					if(!seqIdsFinal.containsKey(seq)){
						seqIdsFinal.put(seq,0);
					}
					
				}
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}


	// populates the seqIdsFinal in sc clusters... by using cluster_ids and thresh by parsing filenames... used after reRunning CAMPS
	private static void getFinalSeqIdsFromFile(String s) {
		// TODO Auto-generated method stub
		try{
			//seqIdsFinal
			File folder = new File(s);
			File[] listOfFiles = folder.listFiles();

			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					//System.out.println("File " + listOfFiles[i].getName());
					//cluster_5.0_0.hmm.serialized
					if (!listOfFiles[i].getName().endsWith(".serialized")){
						files.add(listOfFiles[i].getName());
						//System.out.println("File " + listOfFiles[i].getName());
					}
				} else if (listOfFiles[i].isDirectory()) {
					System.out.println("Directory " + listOfFiles[i].getName());
				}
			}



			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_threshold = ? and cluster_id = ?");
			for (int i =0; i <=files.size()-1;i++){
				String f = files.get(i);
				//cluster_5.0_0.hmm
				//cluster_5.0_46.hmm
				int idx = f.indexOf(".hmm");
				f = f.substring(0,idx);
				System.out.print(f+"\n");
				String Fp[] = f.split("_");
				String thresh = Fp[1];
				String clusid = Fp[2];
				String key = thresh + "_" + clusid;
				// no get clus members from sql and populate
				//int clus = Integer.parseInt(clusid);


				pstm.setFloat(1, Float.parseFloat(thresh));
				pstm.setInt(2, Integer.parseInt(clusid));

				ResultSet rs = pstm.executeQuery();
				//ArrayList<Integer> sqids= new ArrayList<Integer>();
				while(rs.next()){
					//sqids.add(rs.getInt("sequenceid"));
					int seqid = rs.getInt("sequenceid");
					if(!seqIdsFinal.containsKey(seqid)){
						seqIdsFinal.put(seqid, 0);
					}
				}
				rs.close();
				//SC.put(key, sqids);
				//sc_keys.add(key);
			}
			pstm.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}


	private static void getSequencesAndTaxids_v2() {
		// TODO Auto-generated method stub
		try{
			// get all taxids for eukaryotes 
			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
			//		"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Eukaryota\")");
			
			// superkingdom="Archaea" or superkingdom="Bacteria
			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
			//		"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Archaea\" or superkingdom=\"Bacteria\")");
			
			//Virusese: Viruses
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
					"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Viruses\")");

			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				Integer seqid = rs1.getInt(1);
				Integer taxid = rs1.getInt(2);
				if(taxIds2seq.containsKey(taxid)){
					ArrayList<Integer> temp = taxIds2seq.get(taxid);
					if (!temp.contains(seqid)){
						temp.add(seqid);
						taxIds2seq.put(taxid, temp);
					}
				}
				else{
					taxIdsInitial.add(taxid);

					ArrayList<Integer> temp = new ArrayList<Integer>();
					temp.add(seqid);
					taxIds2seq.put(taxid, temp);
				}
			}
			rs1.close();
			pstm1.close();

			//PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
			//		"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Eukaryota\")");

			//PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
			//		"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Archaea\" or superkingdom=\"Bacteria\")");
			
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
					"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Viruses\")");

			ResultSet rs2 = pstm2.executeQuery();
			while(rs2.next()){
				Integer seqid = rs2.getInt(1);
				Integer taxid = rs2.getInt(2);
				if(taxIds2seqFinal.containsKey(taxid)){
					ArrayList<Integer> temp = taxIds2seqFinal.get(taxid);
					if(seqIdsFinal.containsKey(seqid)){ // exists in final Set
						if (!temp.contains(seqid)){
							temp.add(seqid);
							taxIds2seqFinal.put(taxid, temp);
						}
					}
				}
				else{
					if(seqIdsFinal.containsKey(seqid)){ // exists in final Set
						taxIdsFinal.add(taxid);

						ArrayList<Integer> temp = new ArrayList<Integer>();
						temp.add(seqid);
						taxIds2seqFinal.put(taxid, temp);
					}
				}
			}
			rs2.close();
			pstm2.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

	private static void display(){
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/missingGenomesReportAll.txt")));
			bw.write("Taxid \t # of sequences of this taxa in initial \t SizeInitial \t TaxaDetail \t RefSeqDetail");
			bw.newLine();
			bw.newLine();
		for(int i =0;i<=taxIdsMissing.size()-1;i++){
			Integer thisTaxa = taxIdsMissing.get(i);
			System.out.println(thisTaxa + "\t # of sequences of this taxa in initial \t"
					+ taxIds2seq.get(thisTaxa).size()+"\t"+ taxIds2Detail.get(thisTaxa) + "\t" + taxIdsMissingMap.get(thisTaxa.toString()));
			//if(taxIds2seq.get(thisTaxa).size()>50){
			bw.write(thisTaxa + "\t # of sequences of this taxa in initial \t"
					+ taxIds2seq.get(thisTaxa).size()+"\t"+ taxIds2Detail.get(thisTaxa) + "\t" + taxIdsMissingMap.get(thisTaxa.toString()));
			bw.newLine();}
		//}
		bw.close();
		}
		catch(Exception e ){
			e.printStackTrace();
		}
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

	private static void getMissingTaxa(){
		try{
			for(int i=0;i<=taxIdsInitial.size()-1;i++){
				Integer thisTaxa = taxIdsInitial.get(i);
				if(!taxIdsFinal.contains(thisTaxa)){
					taxIdsMissing.add(thisTaxa);
					taxIdsMissingMap.put(thisTaxa.toString(), "");
					//boolean ref = inRefSeq(thisTaxa);
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void inRefSeq() {// checks in the refseq file if these taxas exist there or not
		// TODO Auto-generated method stub
		try{
			String pathToRefSeqFile = "F:/Scratch/RefSeq-release60.catalog";
			BufferedReader br = new BufferedReader(new FileReader(new File(pathToRefSeqFile)));
			String line = "";
			while((line = br.readLine())!= null){
				String[] parts = line.split("\t");
				if (taxIdsMissingMap.containsKey(parts[0].trim())){
					taxIdsMissingMap.put(parts[0], line);
				}				
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	/**
	 * v1
	 * Gets the sequence and taxonomyids in the initial set and in the final set
	 * the final set is fetched through getting only those sequences from table where in_SC="Yes";
	 * 
	 */
	private static void getSequencesAndTaxids_v1(){
		try{
			// get all taxids for eukaryotes 
			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
			//		"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Eukaryota\")");
			// superkingdom="Archaea" or superkingdom="Bacteria
			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
			//		"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Archaea\" or superkingdom=\"Bacteria\")");
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
							"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Viruses\")");
			

			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				Integer seqid = rs1.getInt(1);
				Integer taxid = rs1.getInt(2);
				if(taxIds2seq.containsKey(taxid)){
					ArrayList<Integer> temp = taxIds2seq.get(taxid);
					if (!temp.contains(seqid)){
						temp.add(seqid);
						taxIds2seq.put(taxid, temp);
					}
				}
				else{
					taxIdsInitial.add(taxid);

					ArrayList<Integer> temp = new ArrayList<Integer>();
					temp.add(seqid);
					taxIds2seq.put(taxid, temp);
				}
			}
			rs1.close();
			pstm1.close();

			//PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
			//		"where sequenceid in (select sequenceid from sequences2 where in_SC=\"Yes\") and " +
			//		"taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Eukaryota\")");

			//PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
					//"where sequenceid in (select sequenceid from sequences2 where in_SC=\"Yes\") and " +
					//"taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Archaea\" or superkingdom=\"Bacteria\")");
			
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
							"where sequenceid in (select sequenceid from sequences2 where in_SC=\"Yes\") and " +
							"taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Viruses\")");

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
					taxIdsFinal.add(taxid);

					ArrayList<Integer> temp = new ArrayList<Integer>();
					temp.add(seqid);
					taxIds2seqFinal.put(taxid, temp);
				}
			}
			rs2.close();
			pstm2.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void closeConnection() {
		// TODO Auto-generated method stub
		try{
			CAMPS_CONNECTION.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}


}
