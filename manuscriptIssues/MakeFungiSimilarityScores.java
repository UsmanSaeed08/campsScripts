package manuscriptIssues;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import utils.DBAdaptor;

public class MakeFungiSimilarityScores {

	/**
	 * @param args
	 * Makes similarity matrix of hits for each genome against the other genome -- no filters
	 */
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4"); //changed to test_camps3

	private static HashMap<Integer,Integer> seq2taxidFungi = new HashMap<Integer,Integer>();
	private static ArrayList<Integer> alltaxidsFungi = new ArrayList<Integer>();
	private static ArrayList<Integer> missingTaxidsArray = new ArrayList<Integer>();
	private static ArrayList<Integer> allSeqidsArray = new ArrayList<Integer>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// phase 1 ---- Make the only fungi similarity score file
		/*
		System.out.println("xxx");
		String xistingFile = "/localscratch/CampsSimilarityScores/camps_seq_file.matrix";
		String outFile = "/localscratch/CampsSimilarityScores/fungi_seq_file.matrix";
		HashMap<Integer,Integer> fugalSeq = getFungalTax();
		makeFungalSimilarityFile(xistingFile,outFile,fugalSeq);
		 */
		// phase 2 --- Make the similarity scores Matrix
		//String misingGenomeFile = "F:/Scratch/missingGenomes/FilteredEuTaxIds.txt";
		String misingGenomeFile = "/home/users/saeed/scratch/runForFilteredOutGenomes/FilteredEuTaxIds.txt";
		getFungalTax2(misingGenomeFile);
		//String FungalSimilarityFile = "F:/Scratch/missingGenomes/similarityscore/tempfile";
		String FungalSimilarityFile = "/localscratch/CampsSimilarityScores/fungi_seq_file.matrix";
		//String FungalSimilarityFileHits = "F:/Scratch/missingGenomes/similarityscore/hitsfile";
		String FungalSimilarityFileHits = "/localscratch/CampsSimilarityScores/hitsfile";
		makeSimilarityFileofHits(FungalSimilarityFile,FungalSimilarityFileHits);
		System.out.println("Hits Similarity matrix made at: "+FungalSimilarityFileHits);
	}

	private static void makeSimilarityFileofHits(String fungalSimilarityFile,String outf) {
		// TODO Auto-generated method stub
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outf)));
			for(int i =0;i<=alltaxidsFungi.size()-1;i++){ // for each genome -taxId
				Integer thisTaxid = alltaxidsFungi.get(i);
				HashMap <Integer,Integer> genometoHitCount = new HashMap<Integer,Integer>(); // key is taxid value is hits against thisTaxid--declared in upper line
				ArrayList<Integer> keys = new ArrayList<Integer>();
				BufferedReader br = new BufferedReader(new FileReader(new File(fungalSimilarityFile)));
				String sCurrentLine = "";
				while((sCurrentLine=br.readLine())!=null){
					//missingTaxidsArray.add(Integer.parseInt(l.trim()));
					String q2[] = sCurrentLine.split("\t");
					int seqidquery = Integer.parseInt(q2[0]);
					int seqidhit = Integer.parseInt(q2[1]);

					int taxquery = seq2taxidFungi.get(seqidquery);
					int taxhit = seq2taxidFungi.get(seqidhit);

					if(thisTaxid.equals(taxquery)){
						if(genometoHitCount.containsKey(taxhit)){
							int temp = genometoHitCount.get(taxhit);
							temp = temp + 1 ;
							genometoHitCount.put(taxhit, temp);
						}
						else{
							int n = 1;
							genometoHitCount.put(taxhit, n);
							if(!keys.contains(taxhit)){
								keys.add(taxhit);
							}
						}
					}
					else if (thisTaxid.equals(taxhit)){
						if(genometoHitCount.containsKey(taxquery)){
							int temp = genometoHitCount.get(taxquery);
							temp = temp + 1 ;
							genometoHitCount.put(taxquery, temp);
						}
						else{
							int n = 1;
							genometoHitCount.put(taxquery, n);
							if(!keys.contains(taxquery)){
								keys.add(taxquery);
							}
						}
					}
				}
				br.close();
				if(i==0){
					printHeader(bw);
				}
				compileResult(keys,genometoHitCount,thisTaxid,bw);
			}
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}


	private static void printHeader(BufferedWriter bw) {
		// TODO Auto-generated method stub
		try{
			for(int i =0;i<=alltaxidsFungi.size()-1;i++){
				System.out.print("\t"+alltaxidsFungi.get(i));
				bw.write("\t"+alltaxidsFungi.get(i));
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	// this function collects all fungal genomes and makes seqid to taxid
	// then array of all taxids
	// array of all missing taxids

	private static void compileResult(ArrayList<Integer> keys,
			HashMap<Integer, Integer> genometoHitCount,Integer thistaxa, BufferedWriter bw) {
		// TODO Auto-generated method stub
		try{
			bw.newLine();
			System.out.println();
			bw.write(thistaxa.toString());
			System.out.print(thistaxa);
			for(int i =0;i<=alltaxidsFungi.size()-1;i++){
				int taxa = alltaxidsFungi.get(i);
				if(taxa==thistaxa){
					System.out.print("\t"+"-");
					bw.write("\t"+"-");
				}
				else{
					if(genometoHitCount.containsKey(taxa)){
						System.out.print("\t"+genometoHitCount.get(taxa).toString());
						bw.write("\t"+genometoHitCount.get(taxa).toString());
					}
					else{
						System.out.print("\t"+"-");
						bw.write("\t"+"-");
					}
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

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
					allSeqidsArray.add(seqid);
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

	private static void makeFungalSimilarityFile(String xistingFile,
			String outFile,HashMap<Integer,Integer> fugalSeq) {
		// TODO Auto-generated method stub
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outFile)));
			FileInputStream inputStream = null;
			Scanner sc = null;
			String sCurrentLine;
			inputStream = new FileInputStream(xistingFile);
			sc = new Scanner(inputStream, "UTF-8");
			long counter =0;
			long hitcounter =0;
			while (sc.hasNextLine() ) {
				sCurrentLine = sc.nextLine();
				counter++;
				if (counter %1000000 == 0){
					System.out.print("\nlines processed " + counter +"\n");
				}
				String q2[] = sCurrentLine.split("\t");
				Integer seqidquery = Integer.parseInt(q2[0].trim());
				Integer seqidhit = Integer.parseInt(q2[1].trim());

				if(!seqidquery.equals(seqidhit)){
					if(fugalSeq.containsKey(seqidhit) && fugalSeq.containsKey(seqidquery)){
						hitcounter ++;
						bw.write(sCurrentLine);
						bw.newLine();
					}
				}
			}
			bw.close();
			sc.close();
			System.out.println("Processed hits: "+ hitcounter);
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

	private static HashMap<Integer,Integer> getFungalTax() {
		// TODO Auto-generated method stub
		try{
			HashMap<Integer,Integer> seq2taxid = new HashMap<Integer,Integer>();

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
					"where taxonomyid in (select taxonomyid from taxonomies2 where kingdom=\"Fungi\")");

			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				Integer seqid = rs1.getInt(1);
				Integer taxid = rs1.getInt(2);
				if(!seq2taxid.containsKey(seqid)){
					seq2taxid.put(seqid, taxid);
				}
			}
			rs1.close();
			pstm1.close();
			return seq2taxid;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
}
