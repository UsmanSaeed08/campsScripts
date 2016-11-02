package addMissingGenomes;

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

public class GetGenomesToAdd {

	/**
	 * @param args
	 * This is the main class to add the missing viral genomes. 
	 * The idea is to get viral genomes which were present before before the reRun.
	 * Once we have those genomes which were present before but not present now are to be added. 
	 * One have attained these genomes, write their sequenceIds, taxids and tm Range to a file
	 * 
	 * Further, these sequences would then later be scored against all existing HMMs of the tms range and assigned to respective clusters
	 *
	 * FILE WITH PROBABLE HMMS AND SEQUENCES TO ADD IS MADE HERE BUT 
	 * SCORING IS DONE USING XXX IN MetaModelHmm2 PROJECT. ONCE THE SCORING IS DONE, THEY ARE ASSIGNED TO DB USING THIS CLASS
	 * 
	 * The final running over the HMMs is done by Project TestMMWeb and class MetaModelClassification4.
	 * 
	 * 
	 */
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4"); //changed to test_camps3
	private static ArrayList<String> files = new ArrayList<String>();
	private static HashMap<Integer,Integer> VirseqIdsFinalMap = new HashMap<Integer,Integer>(); // Final means after reRun
	private static ArrayList<Integer> VirseqIdsFinal = new ArrayList<Integer>();

	private static HashMap<Integer,Integer> AllViral = new HashMap<Integer,Integer>();
	
	private static HashMap<Integer,Integer> VirseqIdsOldMap = new HashMap<Integer,Integer>(); // old means before reRun 
	private static ArrayList<Integer> VirseqIdsOld = new ArrayList<Integer>();
	
	private static ArrayList<Integer> Sequences2Add = new ArrayList<Integer>();


	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("Running...");
		
		String hmmTMrangeFile = "F:/Scratch/addMissingGenomes/run/scClustersAndTMSRangeReRun.txt";

		String path = "F:/SC_Clust_postHmm/MetaModelsJune2016/HMMs/CAMPS4_1/";
		getVirFinalSeqIdsFromFile(path); // The function populates all viral sequences in from 1013102 sequences
		// it then populates the final set of sequences in camps after reRun.
		
		getSequencesAndTaxids_v1();
		
		getSequences2Add();
		
		System.out.println("Sequences ReRun: "+ VirseqIdsFinal.size());
		System.out.println("Sequences Old: "+ VirseqIdsOld.size());
		System.out.println("Sequences to Add: "+ Sequences2Add.size());
		
		String outfile = "F:/Scratch/addMissingGenomes/run/";
		String filename = "ToAddSequences.txt";
		writeToFileAndAddTmNo(outfile,filename,hmmTMrangeFile);
		//System.out.println("Number of Taxids Initial: "+ taxIdsInitial.size());
		//System.out.println("Number of Taxids Final: "+ taxIdsFinal.size());
		//System.out.println("Number of final SeqIds: "+AllseqIdsFinal.size());
	}

	private static void writeToFileAndAddTmNo(String outfile,String fnm,String hmmTMrangeF) {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select max(tms_id) from tms where sequenceid=?");
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outfile+fnm)));
			bw.write("# File contains sequences of Viruses to be added after reRun of CAMPS and the HMMs they need to be tested on");
			bw.newLine();
			bw.write("#SeqId \t TaxId \t Number of TMs \t Comma separated probable Hmms");
			bw.newLine();
			for(int i =0; i<=Sequences2Add.size()-1;i++){
				int id = Sequences2Add.get(i);
				int tmNo = -1;
				
				pstm1.setInt(1, id);
				ResultSet rs = pstm1.executeQuery();
				while(rs.next()){
					tmNo = rs.getInt(1);
				}
				rs.close();
				pstm1.clearParameters();
				
				if(tmNo != -1){
					
					String hmms = getProbableHMMs(tmNo,hmmTMrangeF);
					
					bw.write(id+"\t"+AllViral.get(id)+"\t"+tmNo+"\t"+hmms);
					bw.newLine();
				}
				else{
					System.err.print("NO TM Number...");
				}
			}
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static String getProbableHMMs(int tmNo,String hmmTMrangeFile) {
		// TODO Auto-generated method stub
		try{
			String hmms = "";
			BufferedReader br = new BufferedReader(new FileReader(new File(hmmTMrangeFile)));
			String line = "";
			while((line = br.readLine())!=null){
				if(!line.startsWith("#")){
					String[] parts = line.split("\t");
					String tmr = parts[1].trim();
					int range_begin = Integer.parseInt(tmr.split("-")[0].trim());
					int range_end = Integer.parseInt(tmr.split("-")[1].trim());
					if (tmNo>=range_begin && tmNo<=range_end){
						hmms = hmms + parts[0].trim()+",";
					}
				}
			}
			br.close();
			if(hmms.isEmpty())
				return "NO_HMM_FOUND";
			return hmms.substring(0, hmms.length()-1);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	private static void getSequences2Add() {
		// TODO Auto-generated method stub
		try{
			for(int i =0;i<=VirseqIdsOld.size()-1;i++){
				int id = VirseqIdsOld.get(i);
				if(!VirseqIdsFinalMap.containsKey(id)){
					if(!Sequences2Add.contains(id)){
						Sequences2Add.add(id);
					}
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	// populates the seqIdsFinal in sc clusters... by using cluster_ids and thresh by parsing filenames... used after reRunning CAMPS
	private static void getVirFinalSeqIdsFromFile(String s) {
		// TODO Auto-generated method stub
		try{
			// get all virals
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
					"where taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Viruses\")");
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				Integer seqid = rs1.getInt(1);
				Integer taxid = rs1.getInt(2);
				if(!AllViral.containsKey(seqid)){ // check if sequence is a viralSequence
					AllViral.put(seqid, taxid);
				}
			}
			rs1.close();
			pstm1.close();

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
					if(!VirseqIdsFinalMap.containsKey(seqid)){
						if(AllViral.containsKey(seqid)){ // check if sequence is a viralSequence
							VirseqIdsFinalMap.put(seqid, 0);
							VirseqIdsFinal.add(seqid);
						}
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
	
	/**
	 * v1
	 * Gets the Viral SequenceIds in the SC clusters before reRun
	 * 
	 */
	private static void getSequencesAndTaxids_v1(){
		try{
			
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
							"where sequenceid in (select sequenceid from sequences2 where in_SC=\"Yes\") and " +
							"taxonomyid in (select taxonomyid from taxonomies2 where superkingdom=\"Viruses\")");

			ResultSet rs2 = pstm2.executeQuery();
			while(rs2.next()){
				Integer seqid = rs2.getInt(1);
				Integer taxid = rs2.getInt(2);
				
				if(!VirseqIdsOldMap.containsKey(seqid)){
					if(AllViral.containsKey(seqid)){ // check if sequence is a viralSequence
						VirseqIdsOldMap.put(seqid, 0);
						VirseqIdsOld.add(seqid);
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

}
