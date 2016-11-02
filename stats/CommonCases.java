package stats;

import java.awt.List;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import utils.DBAdaptor;

public class CommonCases {

	/**
	 * calculates the common cases between pfam and camps from file sc clusters
	 * @param args
	 */

	private static ArrayList<String> files = new ArrayList<String>();
	//key - thresh_clusterId ..... value - cluster_members = sequenceID
	// e.g. 5.0_30
	private static HashMap <String, ArrayList<Integer>> SC = new HashMap<String,ArrayList<Integer>>();
	private static HashMap <Integer, String> pdb = new HashMap<Integer, String>(); // seqid - pdbid
	private static HashMap <String, ArrayList<Integer>> pfm = new HashMap<String,ArrayList<Integer>>(); //pfam family and value are sequences in camps

	private static ArrayList<String> sc_keys = new ArrayList<String>();
	private static ArrayList<String> pfm_keys = new ArrayList<String>();

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//System.out.print("Word");
		//run("F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/HMMs/CAMPS4_1/");
		run("F:/SC_Clust_postHmm/MetaModelsJune2016/HMMs/CAMPS4_1/");
		//run("/home/proj/check/RunMetaModel_gef/HMMs/CAMPS4_1/");
		//
	}



	private static void run(String string) {
		// TODO Auto-generated method stub
		System.out.print("Getting files" + "\n");
		getfiles(string);
		System.out.print("Populating SC" + "\n");
		PopulategetSC(); // -- have removed the condition to populate only non redundant sequences
		// because why populate only nr sequences when mapping pdb.. pdb can be represented by any sequence
		int s =0;
		for (int i =0;i<=sc_keys.size()-1;i++){
			s = s + SC.get(sc_keys.get(i)).size(); 
		}
		System.out.print("Number of clusters " + sc_keys.size() + "\n");
		System.out.print("Number of Sequences " + s + "\n");
		///*
		System.out.print("Populating PFAM" + "\n");
		Populatepfm();
		//Populatepdb();
		Populatepdb2();
		System.out.print("Calculating..." + "\n");

		compare_scVspfm();
		//*/
	}
	/**
	 * 2nd version of function because camps2pdb is old and has less sequences. so now should use sequences
	 * with 30% identity threshold... 
	 */
	private static void Populatepdb2() {
		// TODO Auto-generated method stub
		try{
			// get pdbids in cpclusters first --
			/*
			 * commenting out, because the file which we are using now only contains CAMPS SC cluster sequences
			 * 
			ArrayList<String> temp = new ArrayList<String>();
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("" +
			"select distinct pdbid from clusters_mcl_structures2");
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				String struc = rs.getString(1);
				temp.add(struc);
			}
			 */

			//BufferedReader br = new BufferedReader(new FileReader(new File("F:/Scratch/mappingCampsToCATHandPDB/MappedPdbtoCampsSC"))); // old file... new mapping is below
			BufferedReader br = new BufferedReader(new FileReader(new File("F:/Scratch/reRunCAMPS_CATH_PDB/reRUNCAMPSSeqToPDBTMOnly.txt")));
			// the file contains same information as in camps2pdb30Iden and is based on 1013102 camps initial set of sequences 
			String line = "";
			while((line = br.readLine())!=null){
				line = line.trim();
				if(!line.isEmpty()){
					String parts[] = line.split("\t");
					String cid = parts[0].trim();
					String struct  = parts[1].trim();
					//String description = parts[7].trim();

					Float Identity = Float.parseFloat(parts[2].trim());
					//if(Identity > 29 && temp.contains(struct)){
					if(Identity > 29 ){
						int id = Integer.parseInt(cid);
						if (!pdb.containsKey(id)){
							pdb.put(id, struct);
						}
					}
				}
			}
			br.close();
		}
		catch(Exception e ){
			e.printStackTrace();
		}
	}

	private static void Populatepdb() {
		// TODO Auto-generated method stub
		try{
			//PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("" +
			//"select sequenceid, pdb_name from camps2pdb where sequenceid in ");
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("" +
					"select sequenceid, pdb_name from camps2pdb where sequenceid in " +
					"(select sequenceid from sequences2 where in_SC=\"Yes\")");
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				String struct = rs.getString("pdb_name");
				Integer id = rs.getInt("sequenceid");
				if (!pdb.containsKey(id)){
					pdb.put(id, struct);
				}
			}
		}
		catch(Exception e ){
			e.printStackTrace();
		}
	}



	private static void compare_scVspfm() {
		// TODO Auto-generated method stub
		//ok, so i have now pfam families and sc clusters
		// pfam families ---- seqids
		// sccluster ------ seqids
		// for each clusterId -> find all its sequences in each domain of pfam.
		// if all sequences are in this pfam domain count ++ to same domain, 
		// else if more than 30% of seq have accounted for diff then
		// count ++ to different domain than pfam
		try{
			ArrayList<Boolean> boolList = new ArrayList<Boolean>();
			ArrayList<String> scWithPfam = new ArrayList<String>();
			ArrayList<String> scWithPfamNegtv = new ArrayList<String>();
			ArrayList<String> positiveList = new ArrayList<String>();

			String outpath = "F:/Scratch/reRunPfam/new/";
			BufferedWriter bftbl = new BufferedWriter(new FileWriter(outpath+"positiveCaseTable"+"_"+"Pfam"+"_"+".txt"));
			// above is the file to write positive cases in table format
			bftbl.write("#clusterid_threshold\tsequenceId\tPDB\tPfamClassification");
			bftbl.newLine();
			//FileWriter f = new FileWriter("/home/users/saeed/positiveCasePfamnCamps.txt");
			//			
			BufferedWriter bf1tbl = new BufferedWriter(new FileWriter(outpath+"negativeCaseTable"+"_"+"Pfam"+"_"+".txt"));
			bf1tbl.write("#clusterid_threshold\tsequenceId\tPDB\tPfamClassification");
			bf1tbl.newLine();

			FileWriter f = new FileWriter(outpath+"positiveCasePfamnCamps2.txt"); // cut pasted to scrach reRunpfam
			//FileWriter f = new FileWriter("/home/users/saeed/positiveCasePfamnCamps.txt");
			//
			BufferedWriter brpositiv = new BufferedWriter(f);

			FileWriter f1 = new FileWriter(outpath+"negativeCasePfamnCamps2.txt");// cut pasted to scrach reRunpfam
			//FileWriter f1 = new FileWriter("/home/users/saeed/negativeCasePfamnCamps.txt");
			BufferedWriter brnegtv = new BufferedWriter(f1);

			int numberofClustersWithSingleClassification = 0;
			int numberofClustersWithMultipleClassification = 0;


			int numberofClustersWithSingleClassificationNegtv = 0;
			int numberofClustersWithMultipleClassificationNegtv = 0;

			int total = 0;
			int numberofProteinsClasssified = 0;
			int diffDomainCounter = 0;
			int diffDomainCounter2 = 0;
			
			int greater50Counter = 0;
			int greater60Counter = 0;
			int greater70Counter = 0;
			int greater80Counter = 0;
			int greater90Counter = 0;
			

			int numberofClustersWithClassification = 0;

			int lesser50Counter = 0;
			int greater85Counter = 0;
			int greater95Counter = 0;
			int greater99Counter = 0;


			int allmembersCounter = 0;
			int atLeastOneCounter = 0;
			int singlePfamNoComparison = 0;
			/*
		String positiveCase1 = "";	// same classification
		String positiveCase2 = "";	// same classification

		String negativeCase1 = ""; // Different Classification
		String negativeCase12 = ""; // Different Classification
			 */

			for (int i =0; i <= sc_keys.size()-1; i++){
				String clus_th = sc_keys.get(i);
				ArrayList <Integer> thisClusSequences = SC.get(sc_keys.get(i));
				numberofProteinsClasssified = numberofProteinsClasssified + thisClusSequences.size();

				boolean sameDomain = false;
				boolean standardDomainSet = false;

				//boolean diffDomain = false;
				ArrayList<String> standardDomains = new ArrayList<String>();
				int NumberOfSeqWithPfamClass = 0;

				ArrayList<String> table = new ArrayList<String>(); // to write in the file

				// 	set standard domain based on min size of cathfolds of a sequence
				ArrayList<String> dom = seStandardDomain(thisClusSequences); // returns the cath /scop classifications for given seqId
				if(dom.size()!=0 && !dom.isEmpty()){
					standardDomains = dom;
					standardDomainSet = true;
				}
				boolean sameDomainAllMembers = true;
				boolean first = true; 

				ArrayList<String> keys = new ArrayList<String>();

				for(int j =0; j<= thisClusSequences.size()-1; j++){
					int currentseqIdSC = thisClusSequences.get(j);

					//if(pfm.containsValue(currentseqIdSC)){
					// now search in the pfam
					// but one sequence can have multiple pfam domains
					if(j>0){
						first = false;
					}
					keys = getKeysByValue(1,currentseqIdSC);
					if (keys.size()!=0 && !keys.isEmpty()){
						if(!scWithPfam.contains(clus_th)){
							scWithPfam.add(clus_th);
						}
						NumberOfSeqWithPfamClass++;
						String splitkeySet = getStringKey(keys);

						if (standardDomainSet){
							// compare
							sameDomain = pairwisecompare(standardDomains, keys);
							if(sameDomain)
								boolList.add(true);
							else
								boolList.add(false);

						}
						else{
							// set standard
							standardDomains = keys;
							standardDomainSet = true;
							System.err.println("Not possible Case: line 372");
							System.exit(0);
						}

						String s = clus_th+"\t"+currentseqIdSC+"\t"+pdb.get(currentseqIdSC)+"\t"+splitkeySet;
						table.add(s);
						if(sameDomain){	// to count the percentage same domains
							System.out.print(keys.get(0)+"\n");
						}
						if (sameDomain== false && first==false){
							sameDomainAllMembers = false;
						}
					}
					//}
				}

				boolean any = false;
				boolean all = true;
				int comparisons = boolList.size();
				int countPost = 0;
				int countNegv = 0;

				if(boolList.size()>0){
					numberofClustersWithClassification++;
					//any = true; // initialize with true because we want to report negative if only one pairwise comparison was false
					for(int x =0;x<=boolList.size()-1;x++){
						boolean t = boolList.get(x);
						if(t){
							countPost++;
							any = true;
						}
						else{
							//any = false;
							all = false;
						}
						//countNegv++;
					}
					boolList = new ArrayList<Boolean>();
				}

				if(NumberOfSeqWithPfamClass == 1){ // if only one sequence was classified.. consider it positive
					any = true;
					// override the booLlist size to 1 because there were no pairwise comparisons
					comparisons = 1;
					countPost = 1;
					singlePfamNoComparison ++;
				}
				if(any){
					atLeastOneCounter ++;
					float perc = 0;
					perc = ((float)countPost/(float)comparisons)*100;
					if(perc <50){
						lesser50Counter++;
					}
					if(perc >=50){
						greater50Counter++;
					}
					if(perc >=60){
						greater60Counter++;
					}
					if(perc >=70){
						greater70Counter++;
					}
					if(perc >=80){
						greater80Counter++;
					}
					if(perc >=90){
						greater85Counter++;
					}
					if(perc >=95){
						greater95Counter++;
					}
					if(perc >=99){
						greater99Counter++;
						allmembersCounter++;
					}
					
					if(all && standardDomains.size() >=1){		//use all if want to check when each comparison was positive
						//write positive table
						total++;
						positiveList.add(clus_th);
						for(int x = 0;x<=table.size()-1;x++){
							bftbl.write(table.get(x));
							bftbl.newLine();
						}
						brpositiv.write(sc_keys.get(i));
						brpositiv.newLine();
						writeSameDomainCAMPSidAndPfam2(sc_keys.get(i),keys,thisClusSequences);
						//}
						//if(any){
						if(standardDomains.size() == 1){
							numberofClustersWithSingleClassification++;
						}
						else{
							numberofClustersWithMultipleClassification++;
						}
						// stats

						// the cluster had atleast 1 seq with a cath classification




					}
				}
				else{
					diffDomainCounter++;
					brnegtv.write(sc_keys.get(i));
					brnegtv.newLine();
					// write negative table
					for(int x = 0;x<=table.size()-1;x++){
						bf1tbl.write(table.get(x));
						bf1tbl.newLine();
					}
					if(standardDomains.size() == 1){
						numberofClustersWithSingleClassificationNegtv++;
					}
					else if(standardDomains.size() > 1){
						numberofClustersWithMultipleClassificationNegtv++;
					}
				}


				//*****

				//float perc = 0;
				//if(domainPercentCounter>0){
				//*perc = (thisClusSequences.size()/domainPercentCounter)*100;
				//float divident = thisClusSequences.size() -1;
				//perc = ((float)domainPercentCounter/divident)*100f;
				//}

				//if(perc >=70){
				//greater70Counter++;
				//}
				//if(perc >=90){
				//greater90Counter++;
				//}
				//if(sameDomainAllMembers){
				//	allmembersCounter++;
				//}
				//if(sameDomain){
				//	atLeastOneCounter ++;
				//}
				//else{
				//if (!sameDomain){
				//	diffDomainCounter2++;
				//brnegtv.write(sc_keys.get(i));
				//brnegtv.newLine();
				// negative is simple, as I can just write these sequences
				// were assigned differently in PFAM and have same fold in CAMPS
				// only have to see if they have pdb
				//}

				//if (!sameDomainAllMembers){
				/*
				if (!all){
					diffDomainCounter++;
					brnegtv.write(sc_keys.get(i));
					brnegtv.newLine();
					// negative is simple, as I can just write these sequences
					// were assigned differently in PFAM and have same fold in CAMPS
					// only have to see if they have pdb
				}
				else{
					brpositiv.write(sc_keys.get(i));
					brpositiv.newLine();
					writeSameDomainCAMPSidAndPfam2(sc_keys.get(i),keys,thisClusSequences);
				}*/

			}
			brpositiv.close();
			brnegtv.close();
			f.close();
			f1.close();
			bftbl.close();
			bf1tbl.close();


			// to make list of clusterIds which had CATH classification but are negative
			for(int i =0;i<=scWithPfam.size()-1;i++){
				String temp = scWithPfam.get(i);
				if (!positiveList.contains(temp)){
					scWithPfamNegtv.add(temp);
				}
			}
			System.out.print("Total Number of SC clusters " + sc_keys.size() + "\n");
			System.out.print("Total Number of Sequences within these SC clusters " + numberofProteinsClasssified + "\n");
			System.out.print("Exactly same folds in "+"Pfam"+" and Camps clusters " + (sc_keys.size()-diffDomainCounter) +"\n");
			System.out.print("Different folds in "+"Pfam"+" and Camps clusters " + diffDomainCounter +"\n");


			System.out.print("Camps clusters with lesser than 50 percent members having same fold " + lesser50Counter +"\n");
			System.out.print("Camps clusters with greater than 50 percent members having same fold " + greater50Counter +"\n");
			System.out.print("Camps clusters with greater than 60 percent members having same fold " + greater60Counter +"\n");
			System.out.print("Camps clusters with greater than 70 percent members having same fold " + greater70Counter +"\n");
			System.out.print("Camps clusters with greater than 80 percent members having same fold " + greater80Counter +"\n");
			System.out.print("Camps clusters with greater than 90 percent members having same fold " + greater85Counter +"\n");
			System.out.print("Camps clusters with greater than 95 percent members having same fold " + greater95Counter +"\n");
			System.out.print("Camps clusters with greater than 99 percent members having same fold " + greater99Counter +"\n");
			System.out.println("************************");
			System.out.println("Number of Clusters with pfam classification "+ numberofClustersWithClassification);
			System.out.println("Number of Clusters with pfam classification - second way "+ scWithPfam.size());
			System.out.println("Number of total Clusters in agreement with cath "+ total);
			System.out.println("Number of clusters eaching with only one sequence with a pfam fold, so no pairwise comparison" +
					" but considered positive: "+singlePfamNoComparison);
			System.out.println("Number of Clusters with all the members having only one pfam classification Positive"+ numberofClustersWithSingleClassification);
			System.out.println("Number of Clusters with multiple classifications and Positive"+ numberofClustersWithMultipleClassification);
			System.out.println("Number of Clusters with all the members having only one Pfam classification and Reported Negative "+ numberofClustersWithSingleClassificationNegtv);
			System.out.println("Number of Clusters with multiple classifications and reported Negative "+ numberofClustersWithMultipleClassificationNegtv);
			System.out.println("Number of Clusters with pfam classification and No agreement: "+ scWithPfamNegtv.size());			//
			System.out.println("these are: :"+ scWithPfamNegtv);			//
			//**

			System.out.print("Total Number of SC clusters " + sc_keys.size() + "\n");

			System.out.print("Total Number of Sequences within these SC clusters " + numberofProteinsClasssified + "\n");

			System.out.print("Same folds in Pfam and Camps clusters (positive if even one case of agreement)" + (sc_keys.size()-diffDomainCounter) +"\n");

			System.out.print("Different folds in Pfam and Camps clusters(any one member may be negative) " + diffDomainCounter +"\n");

			System.out.print("Different folds in Pfam and Camps clusters(all members negative) " + diffDomainCounter2 +"\n");

			System.out.print("Camps clusters with greater than 70 percent members having same fold " + greater70Counter +"\n");

			System.out.print("Camps clusters with greater than 90 percent members having same fold " + greater90Counter +"\n");

			System.out.print("Camps clusters with all members having same fold " + allmembersCounter +"\n");

			System.out.print("Camps clusters with at least one or any number of members having same fold " + atLeastOneCounter +"\n");

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static String getStringKey(ArrayList<String> keys) {
		// TODO Auto-generated method stub
		String r = "";
		for(int i =0;i<=keys.size()-1;i++){
			r = r + keys.get(i)+"#";
		}
		int x = r.length();
		return r.substring(0,x-1);
	}
	private static ArrayList<String> seStandardDomain(ArrayList<Integer> clus_seqIds) { // returns the standard domain based on the min size of folds	
		ArrayList<String> minDom = new ArrayList<String>();
		//keys = getKeysByValue(1,currentseqIdSC);
		for (int j =0; j<= clus_seqIds.size()-1;j++){
			ArrayList<String> keys = new ArrayList<String>();
			int currentseqIdSC = clus_seqIds.get(j);
			/*for (int i =0; i<=cath_keys.size()-1;i++){
				ArrayList<Integer> sqs = CATH.get(cath_keys.get(i));
				if(sqs.contains(currentseqIdSC)){
					keys.add(cath_keys.get(i));
					//System.out.print("True - CATH\n");
					//	break;
				}
			}*/
			keys = getKeysByValue(1,currentseqIdSC);

			if(keys.size()!=0 && !keys.isEmpty()){
				if (minDom.size()>keys.size() || minDom.size() == 0){
					minDom = keys;
				}
			}
		}
		return minDom;
	}

	/**
	 * 
	 * @param string
	 * @param keys
	 * uses the clusters_mcl_structures to get the structure for positive hit
	 * @param ClusSequences 
	 * 
	 */
	private static void writeSameDomainCAMPSidAndPfam2(String string, ArrayList<String> keys, ArrayList<Integer> ClusSequences) {
		// TODO Auto-generated method stub
		// ClusSequences --> the sequences of the sc cluster in consideration
		String struct = "";
		int id = 0;
		ArrayList<Integer> seqids_pfm = new ArrayList<Integer>();
		// keys are the pfam domains
		boolean found = false; 

		try{
			//PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select max(tms_id) from tms where sequenceid=?");

			for (int j =0; j <=keys.size()-1;j++){
				seqids_pfm = pfm.get(keys.get(j));
				for (int x =0; x<=seqids_pfm.size()-1;x++){
					if (pdb.containsKey(seqids_pfm.get(x))){
						if(ClusSequences.contains(seqids_pfm.get(x))){ // the sequence is also in this sc
							id = seqids_pfm.get(x);
							struct = pdb.get(id);
							//		pstm.setInt(1, id);
							//		ResultSet rs = pstm.executeQuery();
							//		int max =0;
							//		while(rs.next()){
							//			max = rs.getInt("max(tms_id)");
							//		}

							//		if(max<0){


							System.out.print("PDB Positive Case\n");
							System.out.print("CampsSeqId: " + id + "\n");
							System.out.print("CampsClusId&Theresh: " + string + "\n");
							System.out.print("PfamId: " + keys.get(j) + "\n");
							System.out.print("PdbId: " + struct + "\n\n");
							found = true;
							break;
							//}
						}
					}

				}
				if (found){
					break;
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void writeSameDomainCAMPSidAndPfam(String string, ArrayList<String> keys) {
		// TODO Auto-generated method stub
		String struct = "";
		int id = 0;
		ArrayList<Integer> seqids_pfm = new ArrayList<Integer>();
		// keys are the pfam domains
		boolean found = false; 

		try{
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select max(tms_id) from tms where sequenceid=?");

			for (int j =0; j <=keys.size()-1;j++){
				seqids_pfm = pfm.get(keys.get(j));
				for (int x =0; x<=seqids_pfm.size()-1;x++){
					if (pdb.containsKey(seqids_pfm.get(x))){
						id = seqids_pfm.get(x);
						struct = pdb.get(id);
						pstm.setInt(1, id);
						ResultSet rs = pstm.executeQuery();
						int max =0;
						while(rs.next()){
							max = rs.getInt("max(tms_id)");
						}

						if(max<0){


							System.out.print("PDB Positive Case\n");
							System.out.print("CampsSeqId: " + id + "\n");
							System.out.print("CampsClusId&Theresh: " + string + "\n");
							System.out.print("PfamId: " + keys.get(j) + "\n");
							System.out.print("PdbId: " + struct + "\n\n");
							found = true;
							break;
						}

					}

				}
				if (found){
					break;
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}



	private static boolean pairwisecompare(ArrayList<String> standardDomains,
			ArrayList<String> keys) {
		// TODO Auto-generated method stub
		boolean m = false;
		int countTrue = 0;
		for (int i =0; i<=keys.size()-1;i++){
			m = false;
			for (int j=0;j<=standardDomains.size()-1;j++){
				if (keys.get(i).contains(standardDomains.get(j))){
					m = true;
					countTrue++;
					break;
				}
			}
		}
		if(countTrue == standardDomains.size()){
			return true;
		}
		else{
			return false;
		}
		//return m;
	}



	public static ArrayList<String> getKeysByValue(int a, int currentseqIdSC) {
		ArrayList<String> keys = new ArrayList<String>();
		// a 1 -> pfm
		// a 2 -> sc
		if (a ==1){
			for (int i =0; i<=pfm_keys.size()-1;i++){
				ArrayList<Integer> sqs = pfm.get(pfm_keys.get(i));
				if(sqs.contains(currentseqIdSC)){
					keys.add(pfm_keys.get(i));
				}
			}
		}

		return keys;
	}

	private static void Populatepfm() {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		try{
			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select distinct(accession) from domains_pfam");
			//PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid,accession from domains_pfam ");
			// using da_clusterAssignment as it is more updated
			//  
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(" select distinct name from da_cluster_assignments where method=\"pfam\"");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid,name from da_cluster_assignments where method=\"pfam\"");

			ResultSet rs = pstm1.executeQuery();

			while(rs.next()){
				String ac = rs.getString(1);
				pfm_keys.add(ac);
			}
			rs.close();
			// make a hash map of all pfam then compare with keys
			ResultSet rsPm = pstm2.executeQuery();
			int i =0;
			while(rsPm.next()){
				i++;
				int sqid = rsPm.getInt("sequenceid");
				String ac = rsPm.getString("name");
				if(pfm.containsKey(ac)){
					ArrayList<Integer> temp = pfm.get(ac);
					temp.add(sqid);
					pfm.put(ac, temp);
				}
				else{
					ArrayList<Integer> temp = new ArrayList<Integer>();
					temp.add(sqid);
					pfm.put(ac, temp);
				}
				if (i%100 == 0){
					System.out.print("Processed Pfam domains entries "+i+"\n");
				}

			}


		}catch(Exception e){
			e.printStackTrace();
		}
	}

	/*
	private static void Populatepfm() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select distinct(accession) from domains_pfam");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from domains_pfam where accession = ?");
			ResultSet rs = pstm1.executeQuery();

			while(rs.next()){
				String ac = rs.getString("accession");
				pfm_keys.add(ac);
			}
			rs.close();
			for (int i =0; i<=pfm_keys.size()-1;i++){
				pstm2.setString(1, pfm_keys.get(i));
				ResultSet rs2 = pstm2.executeQuery();
				ArrayList<Integer> pfids= new ArrayList<Integer>();
				while(rs2.next()){
					pfids.add(rs2.getInt("sequenceid"));
				}
				rs2.close();
				pfm.put(pfm_keys.get(i), pfids);
				if (i%100 == 0){
					pstm2.close();
					pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from domains_pfam where accession = ?");
					System.out.print("Processed Pfam domains entries "+i+"\n");
				}
			}

		}catch(Exception e){
			e.printStackTrace();
		}
	}
	 */


	private static void PopulategetSC() {
		// TODO Auto-generated method stub
		try{
			//PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_threshold = ? and cluster_id = ?" +
			//		" and redundant=\"No\"");
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
				int clus = Integer.parseInt(clusid);


				pstm.setFloat(1, Float.parseFloat(thresh));
				pstm.setInt(2, Integer.parseInt(clusid));

				ResultSet rs = pstm.executeQuery();
				ArrayList<Integer> sqids= new ArrayList<Integer>();
				while(rs.next()){
					sqids.add(rs.getInt("sequenceid"));
				}
				rs.close();
				SC.put(key, sqids);
				sc_keys.add(key);
			}
			pstm.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}



	private static void getfiles(String string) {
		// TODO Auto-generated method stub
		File folder = new File(string);
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
	}


}
