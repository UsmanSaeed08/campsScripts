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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import utils.DBAdaptor;

public class SimilarityScoreMissingGenomes {

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4"); //changed to test_camps3

	private static HashMap <Integer, Long> passedSeq = new HashMap <Integer, Long>(); // passed sequenceids to count number of hits
	private static ArrayList<Integer> passedSeqArray = new ArrayList<Integer>();

	private static HashMap <Integer, Long> FilteredSeq = new HashMap <Integer, Long>(); // filtered seqid to count of number of hits
	private static ArrayList<Integer> FilteredSeqArray = new ArrayList<Integer>();

	private static HashMap <Integer, Integer> FilteredSeqtoTaxId = new HashMap <Integer, Integer>(); //filteres seqid to taxonomyid
	private static ArrayList<Integer> FilteredTaxIdArray = new ArrayList<Integer>();
	private static HashMap <Integer, Long> FilteredTaxIdtoCount = new HashMap <Integer, Long>(); //filteres taxonomyid to their number of hits

	private static HashMap<Integer,Long> FilteredTaxIdtoNumberofSeq =new HashMap <Integer,Long>();
	/**
	 * @param args
	 * Tasks - 
	 * Get stats for the missing and passed genomes. 
	 * 1. get All seqids and taxids for missing genomes
	 * 2. get All seqids and taxids for passed genomes
	 * 3. read through the scoresfile and simply count the occurance(hits) 
	 * 4. sum up the hits for missing and passed genomes respectively
	 * 5. normalize acc to the number of total sequences. 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("CHECK!!!");
		// get all passed sequences
		passedSeq = passedSeq();
		// Now get the filtered out seq - using files... using taxids of filtered out genomes in files;
		//String taxidFile = "F:/Scratch/missingGenomes/filteredGenomeEu.fasta";
		String taxidFile = "/home/users/saeed/scratch/runForFilteredOutGenomes/filteredGenomeEu.fasta";
		getFilteredGenomesFromFile(taxidFile);
		String scoreFilePath = "/localscratch/CampsSimilarityScores/camps_seq_file.matrix";
		readScoreFile(scoreFilePath);
		compileResults();

	}

	private static void compileResults() {
		// TODO Auto-generated method stub
		try{
			Long hitsSumPassed = 0l;
			Integer size = passedSeqArray.size()-1;
			for(int i=0;i<=size;i++){
				Integer seqid = passedSeqArray.get(i);
				hitsSumPassed = hitsSumPassed + passedSeq.get(seqid);
			}
			Double normalizedPassed = ((double) hitsSumPassed/(double)size);

			System.out.println("Passed Sequences: "+size);
			System.out.println("Passed Sequences hits: "+hitsSumPassed);
			System.out.println("Passed Normalized hits: "+normalizedPassed);


			size = FilteredSeqArray.size()-1;
			Long totalhits = 0l;
			for(int i=0;i<=size;i++){
				Integer id = FilteredSeqArray.get(i);
				Long seqhits = FilteredSeq.get(id);
				totalhits = totalhits + seqhits;
				int taxid = FilteredSeqtoTaxId.get(id);
				if(FilteredTaxIdtoCount.containsKey(taxid)){
					Long c = FilteredTaxIdtoCount.get(taxid);
					c = c + seqhits;
					FilteredTaxIdtoCount.put(taxid, c);

					Long x = FilteredTaxIdtoNumberofSeq.get(taxid);
					x = x + 1;
					FilteredTaxIdtoNumberofSeq.put(taxid,x);
				}
				else{
					FilteredTaxIdtoCount.put(taxid, seqhits);
					FilteredTaxIdtoNumberofSeq.put(taxid,1l);
				}
			}
			Float normalizedfilteredHits = (float)totalhits/(float)size;

			System.out.println("Filtered Sequences: "+size);
			System.out.println("Filtered Sequences hits: "+totalhits);
			System.out.println("Filtered Normalized hits: "+normalizedfilteredHits);

			System.out.println();
			System.out.println("Taxa\tNoOfSeq\tHits\tNormalized");
			for(int i =0;i<=FilteredTaxIdArray.size()-1;i++){
				Integer taxa = FilteredTaxIdArray.get(i);
				Long taxahits = FilteredTaxIdtoCount.get(taxa);
				Long noOfSeq = FilteredTaxIdtoNumberofSeq.get(taxa); 
				float taxanorm = (float)taxahits/(float)noOfSeq;
				System.out.println(taxa+"\t"+noOfSeq+"\t"+taxahits+"\t"+taxanorm);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void readScoreFile(String scoreFilePath) {
		// TODO Auto-generated method stub
		try{
			FileInputStream inputStream = null;
			Scanner sc = null;
			String sCurrentLine;
			inputStream = new FileInputStream(scoreFilePath);
			sc = new Scanner(inputStream, "UTF-8");
			long counter =0;
			while (sc.hasNextLine() ) {
				sCurrentLine = sc.nextLine();
				counter++;
				if (counter %100000 == 0){
					System.out.print("\nlines processed " + counter +"\n");
				}
				String q2[] = sCurrentLine.split("\t");
				Integer seqidquery = Integer.parseInt(q2[0]);
				Integer seqidhit = Integer.parseInt(q2[1]);
				if(seqidquery!=seqidhit){
					double evalue= Double.parseDouble(q2[4]);
					double bitscore = Double.parseDouble(q2[3]);
					if(evalue < 1E-5){
						if(bitscore>50){
							if(FilteredSeq.containsKey(seqidquery)){
								//if(bitscore>50){
								long count = FilteredSeq.get(seqidquery);
								count = count +1 ;
								FilteredSeq.put(seqidquery, count);
							}
							if(FilteredSeq.containsKey(seqidhit)){
								long count = FilteredSeq.get(seqidhit);
								count = count +1 ;
								FilteredSeq.put(seqidhit, count);
							}
						}
						if(passedSeq.containsKey(seqidquery)){
							long count = passedSeq.get(seqidquery);
							count = count +1 ;
							passedSeq.put(seqidquery, count);
						}
						if(passedSeq.containsKey(seqidhit)){
							long count = passedSeq.get(seqidhit);
							count = count +1 ;
							passedSeq.put(seqidhit, count);
						}
					}
				}
			}
			sc.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void getFilteredGenomesFromFile(String File) {
		// TODO Auto-generated method stub
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(File)));
			String l ="";
			while((l=br.readLine())!=null){
				if(l.startsWith(">")){
					l = l.substring(1);
					String [] part = l.split("-");
					Integer seqid = Integer.parseInt(part[0].trim());
					Integer taxId = Integer.parseInt(part[1].trim());

					if(!FilteredSeq.containsKey(seqid)){
						FilteredSeq.put(seqid, 0l);
						FilteredSeqArray.add(seqid);

						FilteredSeqtoTaxId.put(seqid, taxId);
					}
					if(!FilteredTaxIdArray.contains(taxId)){
						FilteredTaxIdArray.add(taxId);
					}
				}
			}
			System.out.println("Number of sequences of filtered genomes: "+FilteredSeq.size());
			System.out.println("Number of filtered genomes: "+FilteredSeqtoTaxId.size());
			System.out.println("Number of filtered genomes: "+FilteredTaxIdArray.size());

			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	public static HashMap<Integer,Long> passedSeq(){
		try{
			HashMap <Integer, Long> passedSeq1 = new HashMap <Integer, Long>(); //All the passed seqids from alignments_initial
			//ArrayList<Integer> passedSeqArray = new ArrayList<Integer>();
			System.out.println("Awaiting Query...");
			int idx =1;
			PreparedStatement pstm1 = null;
			for (int i=1; i<=17;i++){	
				pstm1 = CAMPS_CONNECTION.prepareStatement("select seqid_query,seqid_hit from alignments_initial where not seqid_query=seqid_hit limit "+idx+","+10000000);
				idx = i*10000000;
				System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
				System.out.flush();
				ResultSet rs1 = pstm1.executeQuery();
				System.out.println("Processing...");
				while(rs1.next()){
					Integer seqidq = rs1.getInt(1);
					Integer seqidh = rs1.getInt(2);

					if(!passedSeq1.containsKey(seqidq)){
						passedSeq1.put(seqidq, 0l);
						passedSeqArray.add(seqidq);
					}
					if(!passedSeq1.containsKey(seqidh)){
						passedSeq1.put(seqidh, 0l);
						passedSeqArray.add(seqidh);
					}
				}
				rs1.close();
			}
			pstm1.close();

			System.out.println("The number of passed sequences extracted is: " + passedSeq1.size());
			return passedSeq1;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}




}
