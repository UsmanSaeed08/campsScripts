package computeResults;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Hashtable;

import utils.DBAdaptor;

public class SuperkingdomsInSCcluster {

	/**
	 * @param args
	 * This class makes table for protein sequences in camps SC cluters and checks their tm no and superkingdom
	 * so basically a table with tmNo and its number of proteins in that particular superkingdom
	 * 
	 *  		tmNo	Bacteria	Archaea	Virus	Eukaryotes
	 *  	
	 */

	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");
	private static Hashtable<Integer,Integer> sequences2taxids = new Hashtable<Integer,Integer>();	// id -> Key is seqid and Value is taxid 

	private static Hashtable<Integer,String> sequences2superKingdoms = new Hashtable<Integer,String>();	// id -> sequenceId and value is taxonomyId

	private static ArrayList<Integer> Bacteria2Seq = new ArrayList<Integer>();	// array Of bacteria sequences in SC
	private static ArrayList<Integer> Arch2Seq = new ArrayList<Integer>();	// array Of Arch sequences in SC
	private static ArrayList<Integer> Virus2Seq = new ArrayList<Integer>();	// array Of Virus sequences in SC
	private static ArrayList<Integer> Eu2Seq = new ArrayList<Integer>();	// array Of Eu sequences in SC
	private static ArrayList<Integer> Exception = new ArrayList<Integer>(); // not in any superkingdoms... missed record
	private static ArrayList<Integer> taxonomyNotFound = new ArrayList<Integer>();
	private static Hashtable<Integer,Integer> SeqId2Exception = new Hashtable<Integer,Integer>(); // used to map sequenceids for the missing taxonomyids
	
	private static ArrayList<Integer> ssRnaSequences = new ArrayList<Integer>();
	private static ArrayList<String> ssRnaFamilies = new ArrayList<String>(); 


	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//tryFindingExceptionSeq();
		//System.exit(0);

		System.out.println("Fetching the sequences and their tmNo...");
		TMDistribution.getInitialSet(true); // true so that only those proteins are fetched which are in SC
		//TMDistribution.getInitialSet(false); // false so all proteins are fetched
		System.out.println("Fetching the taxIds...");
		getssRnaFamiliesFromFile();
		getTaxIds(true);
		//getTaxIds(false);
		System.out.println("Fetching the superKingdoms...");
		//fetchSuperkingdoms(true);
		fetchSuperkingdoms(true); //-- keep always true as its good to have all the taxonomies... avoid errors
		System.out.println("Bacterial Proeins in SC: " + Bacteria2Seq.size());
		System.out.println("Archaea Proeins in SC: " + Arch2Seq.size());
		System.out.println("Virus Proeins in SC: " + Virus2Seq.size());
		System.out.println("Eukaryote Proeins in SC: " + Eu2Seq.size());
		System.out.println("Number of Sequences whose Taxonomyid is Not Found: " + Exception.size());
		System.out.println("Number of Taxonomyids Not Found: " + taxonomyNotFound.size());
		//System.exit(0);
		//tryFindingExceptionSeq(); -- have used it once.. now can use the generated file from this function
		//addExceptionsequences();
		System.out.println();
		System.out.println("After Adding lost taxonomies.. the stats are: ");
		System.out.println("Bacterial Proeins in SC: " + Bacteria2Seq.size());
		System.out.println("Archaea Proeins in SC: " + Arch2Seq.size());
		System.out.println("Virus Proeins in SC: " + Virus2Seq.size());
		System.out.println("Eukaryote Proeins in SC: " + Eu2Seq.size());
		System.out.println("Number of ssRna Sequences: "+ ssRnaSequences.size());

		getTmDistributionAndSuperKingdom();
		
		getTmDistributionssRna();

	}
	private static void getTmDistributionssRna() {
		// TODO Auto-generated method stub
		ArrayList<Integer> vir = new ArrayList<Integer>();
		// initialize arrays
		for(int i =0;i<=200;i++){
			vir.add(i, 0);
		}
		System.out.println();
		System.out.println(ssRnaSequences.size());
		for(int i =0;i<=ssRnaSequences.size()-1;i++){
			int seq = ssRnaSequences.get(i);
			int tmNos = TMDistribution.Initial_sequences.get(seq);
			if(tmNos < 8){
				int temp = vir.get(tmNos);
				vir.set(tmNos, temp+1);
			}
			else{
				/*
				 * Not working right for some reason
				 */
				int temp = vir.get(tmNos);
				System.out.println(tmNos + " number of helices in "+ temp +" proteins");
				vir.set(8, temp+1);
			}
		}
		System.out.println();
		System.out.println();
		System.out.println("\t"+"TmNo"+"\t"+"Virus");
		for(int i =0;i<=8;i++){
			if(i<8){
				System.out.println("\t"+i+"\t"+vir.get(i));
			}
			else{
				System.out.println("\t"+"Other"+"\t"+vir.get(i));
			}
		}
	}
	private static void getssRnaFamiliesFromFile() {
		// TODO Auto-generated method stub
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File("/home/users/saeed/AllssRnaViruses.txt")));
			String line = "";
			while((line = br.readLine())!=null){
				//ssRnaFamilies
				if(!ssRnaFamilies.contains(line.trim())){
					ssRnaFamilies.add(line.trim());
				}
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
	/**
	 * The function makes 4 different arrays for each superkingdom.. 
	 * the index is the number of TM in protein and the value is the frequency/occurance of this type of protein
	 * in the given superkingdom 
	 */
	private static void getTmDistributionAndSuperKingdom() {
		// TODO Auto-generated method stub
		ArrayList<Integer> bacteria = new ArrayList<Integer>();
		ArrayList<Integer> arch = new ArrayList<Integer>();
		ArrayList<Integer> eu = new ArrayList<Integer>();
		ArrayList<Integer> vir = new ArrayList<Integer>();
		// initialize arrays
		for(int i =0;i<=200;i++){
			bacteria.add(i, 0);
			arch.add(i, 0);
			eu.add(i, 0);
			vir.add(i, 0);
		}

		// first process bacterial seq
		for(int i =0;i<=Bacteria2Seq.size()-1;i++){
			int seq = Bacteria2Seq.get(i);
			int tmNos = TMDistribution.Initial_sequences.get(seq);
			if(tmNos < 160){
				int temp = bacteria.get(tmNos);
				bacteria.set(tmNos, temp+1);
				
			}
		}
		// then archea
		for(int i =0;i<=Arch2Seq.size()-1;i++){
			int seq = Arch2Seq.get(i);
			int tmNos = TMDistribution.Initial_sequences.get(seq);
			if(tmNos < 160){
				int temp = arch.get(tmNos);
				arch.set(tmNos, temp+1);
			}
		}
		// eu
		for(int i =0;i<=Eu2Seq.size()-1;i++){
			int seq = Eu2Seq.get(i);
			int tmNos = TMDistribution.Initial_sequences.get(seq);
			if(tmNos < 160){
				int temp = eu.get(tmNos);
				eu.set(tmNos, temp+1);
			}
		}
		// vir
		for(int i =0;i<=Virus2Seq.size()-1;i++){
			int seq = Virus2Seq.get(i);
			int tmNos = TMDistribution.Initial_sequences.get(seq);
			if(tmNos < 160){
				int temp = vir.get(tmNos);
				vir.set(tmNos, temp+1);
			}
		}
		printResults(bacteria,arch,eu,vir);
	}

	private static void printResults(ArrayList<Integer> bacteria,
			ArrayList<Integer> arch, ArrayList<Integer> eu, ArrayList<Integer> vir) {
		// TODO Auto-generated method stub
		System.out.println();
		System.out.println();
		System.out.println("\t"+"TmNo"+"\t"+"Bacteria"+"\t"+"Archea"+"\t"+"Eukaryote"+"\t"+"Virus");
		for(int i =0;i<=160;i++){
			System.out.println("\t"+i+"\t"+bacteria.get(i)+"\t"+arch.get(i)+"\t"+eu.get(i)+"\t"+vir.get(i));
		}
	}
	// reads the file generated by tryFinding Exception function and adds the number of sequences to Bacteria Archea Eu and Virus
	private static void addExceptionsequences() {
		// TODO Auto-generated method stub
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File("F:/Scratch/taxonomyInfo/taxonomy/missingTaxonomySC2.txt")));
			String line = "";
			// taxid \t superkingdom \t sequenceidinCAMPS
			while((line=br.readLine())!=null){
				String[] p=line.split("\t");
				if(p[1].trim().contains("Bacteria")){
					Bacteria2Seq.add(Integer.parseInt(p[2].trim()));
				}
				else if(p[1].trim().contains("Archaea")){
					Arch2Seq.add(Integer.parseInt(p[2].trim()));
				}
				else if(p[1].trim().contains("Viruses")){
					Virus2Seq.add(Integer.parseInt(p[2].trim()));
				}
				else if(p[1].trim().contains("Eukaryota")){
					Eu2Seq.add(Integer.parseInt(p[2].trim()));
				}
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

	// function only used to get superkingdoms from the embl website 
	// using the sequences  and taxonomy id in Exception array etc
	private static void tryFindingExceptionSeq() {
		// TODO Auto-generated method stub
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/Scratch/taxonomyInfo/taxonomy/missingTaxonomySC2.txt")));
			// taxid \t superkingdom \t sequenceidinCAMPS
			//
			Hashtable <Integer,String> processed = new Hashtable<Integer,String>(); //-- used to keep record of done taxids and their superkingdoms 
			for(int i =0;i<=Exception.size()-1;i++){
				//if (i ==10){
				//	break;
				//}
				Integer seqid = Exception.get(i);
				Integer taxid = SeqId2Exception.get(seqid);
				if(!processed.containsKey(taxid)){
					//Integer taxid = 1206782;
					//http://www.ebi.ac.uk/ena/data/view/Taxon:1206782&display=xml
					String weburl = "http://www.ebi.ac.uk/ena/data/view/Taxon:"+taxid.toString()+"&display=xml";
					URL oracle = new URL(weburl);
					BufferedReader in = new BufferedReader(new InputStreamReader(oracle.openStream()));
					String inputLine;
					while ((inputLine = in.readLine()) != null){
						//System.out.println(inputLine);
						if(inputLine.contains("Bacteria")){
							System.out.println("Found Bacteria: "+ taxid + "\t"+ i);
							processed.put(taxid, "Bacteria");
							bw.write(taxid+"\t"+"Bacteria"+"\t"+seqid);
							bw.newLine();
							break;
						}
						else if(inputLine.contains("Archaea")){
							System.out.println("Found Archaea: "+ taxid+ "\t"+ i);
							processed.put(taxid, "Archaea");
							bw.write(taxid+"\t"+"Archaea"+"\t"+seqid);
							bw.newLine();
							break;
						}
						else if(inputLine.contains("Viruses")){
							System.out.println("Found Viruses: "+ taxid+ "\t"+ i);
							processed.put(taxid, "Viruses");
							bw.write(taxid+"\t"+"Viruses"+"\t"+seqid);
							bw.newLine();
							break;
						}
						else if(inputLine.contains("Eukaryota")){
							System.out.println("Found Eukaryota: "+ taxid+ "\t"+ i);
							bw.write(taxid+"\t"+"Eukaryota"+"\t"+seqid);
							bw.newLine();
							processed.put(taxid, "Eukaryota");
							break;
						}
					}
					in.close();
				}
				else{ // already have this taxid -- so simply write it down in file
					String superkingdom = processed.get(taxid);
					bw.write(taxid+"\t"+superkingdom+"\t"+seqid);
					bw.newLine();
				}
			}
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void getTaxIds(boolean onlySc) {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm; //= CAMPS_CONNECTION.prepareStatement("select taxonomyid,sequenceid from sequences2names where in_SC=\"Yes\"");
			if(onlySc){
				pstm = CAMPS_CONNECTION.prepareStatement("select taxonomyid,sequenceid from sequences2names where in_SC=\"Yes\"");
			}
			else{
				pstm = CAMPS_CONNECTION.prepareStatement("select taxonomyid,sequenceid from sequences2names");
			}
			
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				Integer taxid = rs.getInt(1);
				Integer seqid = rs.getInt(2);
				sequences2taxids.put(seqid, taxid);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void fetchSuperkingdoms(boolean inSc){
		try{
			//PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select taxonomyid,superkingdom from taxonomies2 where in_SC=\"Yes\"");
			PreparedStatement pstm;
			if(inSc){
				pstm = CAMPS_CONNECTION.prepareStatement("select taxonomyid,superkingdom,family from taxonomies_merged where in_SC=\"Yes\"");
			}
			else{
				pstm = CAMPS_CONNECTION.prepareStatement("select taxonomyid,superkingdom,family from taxonomies_merged");
			}
			
			ResultSet rs = pstm.executeQuery();
			Hashtable <Integer,String> tempsuperkingdoms = new Hashtable<Integer,String>(); // key taxid and value is superkingdom
			Hashtable <Integer,String> tempfamily = new Hashtable<Integer,String>(); // key taxid and value is superkingdom
			while(rs.next()){
				Integer taxid = rs.getInt(1);
				String superkingdom = rs.getString(2);
				String family = rs.getString(3);
				if(!(superkingdom==null)){
					tempsuperkingdoms.put(taxid, superkingdom);
					if(family!=null){
						tempfamily.put(taxid, family);
					}
				}
			}
			rs.close();
			pstm.close();
			/*
			 * Bacteria2Seq = new ArrayList<Integer>();	// array Of bacteria sequences in SC
	private static ArrayList<Integer> Arch2Seq = new ArrayList<Integer>();	// array Of Arch sequences in SC
	private static ArrayList<Integer> Virus2Seq = new ArrayList<Integer>();	// array Of Virus sequences in SC
	private static ArrayList<Integer> Eu2Seq =
			 */
			for(int i =0;i<=TMDistribution.allProteins.size()-1;i++){
				int thisseq = TMDistribution.allProteins.get(i);
				int taxid = sequences2taxids.get(thisseq);

				if(tempsuperkingdoms.containsKey(taxid)){
					String superking = tempsuperkingdoms.get(taxid);
					String family = "xxxyyyzzz";
					if(tempfamily.containsKey(taxid)){
						family = tempfamily.get(taxid);
					}

					if(superking.contains("Bacteria")){
						Bacteria2Seq.add(thisseq);
					}
					else if(superking.contains("Archaea")){
						Arch2Seq.add(thisseq);
					}
					else if(superking.contains("Viruses")){
						Virus2Seq.add(thisseq);
						if(ssRnaFamilies.contains(family)){
							ssRnaSequences.add(thisseq);
						}
					}
					else if(superking.contains("Eukaryota")){
						Eu2Seq.add(thisseq);
					}
					sequences2superKingdoms.put(thisseq, superking);
				}
				else{
					Exception.add(thisseq);
					if(!taxonomyNotFound.contains(taxid)){
						taxonomyNotFound.add(taxid);
					}
					SeqId2Exception.put(thisseq, taxid);
				}
			}			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
