package stats;

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

public class CathCamp_commonCases2 {

	/**
	 * @param args
	 */

	private static ArrayList<String> files = new ArrayList<String>();
	//key - clusterId ..... value - cluster_members = sequenceID
	private static HashMap <String, ArrayList<Integer>> SC = new HashMap<String,ArrayList<Integer>>();
	private static HashMap <Integer, Integer> SC_seqIds = new HashMap<Integer,Integer>();
	// key classification and value is seqId
	private static HashMap <String, ArrayList<Integer>> SCOP = new HashMap<String, ArrayList<Integer>>();
	private static HashMap <String, ArrayList<Integer>> CATH = new HashMap<String, ArrayList<Integer>>();
	private static HashMap <Integer, String> pdb = new HashMap<Integer, String>(); // seqid - pdbid
	//reverse key seqId and valu is cs
	private static HashMap <Integer, String> SCOP_rev = new HashMap<Integer, String>();
	private static HashMap <Integer, String> CATH_rev = new HashMap<Integer, String>();
	// for mcl cluster ids... key of hashmap sc_mcl
	//private static ArrayList<Integer> clusids_mcl = new ArrayList<Integer>();
	private static ArrayList<String> cath_keys = new ArrayList<String>();
	private static ArrayList<String> scop_keys = new ArrayList<String>();

	// sequences with structures and different clusters - to be checked in cathscop
	private static ArrayList <Integer> seqsToCheck = new ArrayList<Integer>(); // sequences

	// sequences with structures and may be same clusters - to be checked in cathscop
	private static ArrayList <Integer> seqsToCheckRed = new ArrayList<Integer>(); // sequences

	private static ArrayList<String> sc_keys = new ArrayList<String>();

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		//System.out.print("ok");
		//		run("F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/HMMs/CAMPS4_1/");
		//run("/home/proj/check/RunMetaModel_gef/HMMs/CAMPS4_1/");

		// after reRun
		//F:\SC_Clust_postHmm\MetaModelsJune2016\HMMs\CAMPS4_1
		String pdbmapFile = "F:/Scratch/reRunCAMPS_CATH_PDB/reRUNCAMPSSeqToPDBTMOnly.txt";
		String campCathMapFile = "F:/Scratch/reRunCAMPS_CATH_PDB/reRUNCamps2Cath_Min30Identity.txt";
		String outPath = "F:/Scratch/reRunCAMPS_CATH_PDB/";
		String hmms = "F:/SC_Clust_postHmm/MetaModelsJune2016/HMMs/CAMPS4_1/";
		run(hmms,pdbmapFile,campCathMapFile,outPath);
		// i think i should not count them as agreement cases where there is only one CATH fold
		// in the whole cluster -- CHECK -- DONE
		// set the standard fold by getting the fold present in maximum number of sequences - 
		// ^^^does this make any difference.. theoretically not - because we then check for 
		// exactly same folds -- hint -- change the code where checking pairwise agreement
		// and reject the cases in which standard domain is shorter than keys... this would also
		// eliminate the process to set up standard domain by max.
		// OK so, I tried rejecting standard domain where size != keys.size.. but it only seems to make the situation worse
		// as it increases true negatives and lowers agreement.
		// NOW:
		// Trying to set standard domain to min number of cath folds in any sequence in the cluster
		// because we consider the other hits as false positives - so this is being done to reduce the false positives
		// ^^^done above and it seems good
		/*
		 * 
			Number of clusters eaching with only one sequence with a CATH fold, so no pairwise comparison but considered positive: 56
			Number of Clusters with all the members having only one Cath classification 192
			Number of Clusters with multiple classifications 23
			Number of Clusters with all the members having only one Cath classification and Reported Negative 36
			Number of Clusters with CATH classification and No agreement: 36
			these are: :[100.0_19942, 14.0_18847, 14.0_21805, 16.0_15137, 17.0_14370, 17.0_4712, 18.0_11166, 19.0_17585, 22.0_21096, 22.0_3800, 28.0_18763, 30.0_24195, 30.0_2555, 35.0_32252, 35.0_364, 40.0_12342, 40.0_22875, 40.0_32486, 45.0_14383, 45.0_4747, 5.0_0, 5.0_101, 5.0_171, 5.0_295, 5.0_3, 5.0_33, 5.0_680, 5.0_784, 6.0_6458, 60.0_17838, 60.0_38285, 7.0_11407, 7.0_2208, 7.0_4631, 8.0_15393, 90.0_15171]
			
			The positive and negative cases are in main working directory

		 */


	}

	private static void run(String string,String pdbmapF,String campCathMapF,String outP){
		// get sc cluster sequenceids
		// use these seqids to get pdbids from camps2pdb
		// use these pdbids to get cath and scop domains separately
		System.out.print("Getting files" + "\n");
		getfiles(string);
		System.out.print("Populating SC" + "\n");
		PopulategetSC();
		System.out.print("Populating pdb" + "\n");
		//Populatepdb();
		ArrayList<Integer> sqids = PopulatepdbFile(pdbmapF);
		System.out.print("Getting CathScopIds" + "\n");
		//getCathIdsforSeqs();
		//getCathIdsforSeqsFile(sqids,campCathMapF,false);
		getCathIdsforSeqsFile(sqids,campCathMapF,true); // send true if you want to run the comparison at superfamily level. 
		//

		System.out.print("Compairing" + "\n");
		Compare(1,outP); // cath
		//Compare(2,outP); // scop
		ClusterComposition(1); // cath
		//ClusterComposition(2); // scop 
		//test();
		uniquefoldsforSeqsToCheck();
	}
	private static void ClusterComposition(int a){
		if(a==1	){// cath
			for(int i =0;i<=sc_keys.size()-1;i++){
				// for all the sc clusters
				ArrayList <Integer> temp = SC.get(sc_keys.get(i));
				// for each member of the cluster, if has a classification
				boolean reftoSet = true;
				String cs = "";
				String csRef = "";
				for (int j=0;j<=temp.size()-1;j++){
					cs = CATH_rev.get(temp.get(j)); 
					if (seqsToCheckRed.contains(temp.get(j))&& cs!=null){
						if(reftoSet){
							// first occurance of sequence with classification
							// therefore set the refernce
							csRef = cs;
							reftoSet = false;
						}
						else {
							// reference domain has been set and is only to be compared
							if(!cs.contains(csRef)){
								//System.out.print("\nReport Negative Case CATH\n");
								//System.out.print("SC Cluster: "+sc_keys.get(i)+"\n");
								//System.out.print("Classification: "+cs+"\n");
								//System.out.print("Classification Reference: "+csRef+"\n");

							}
						}
					}
				}

			}
		}
		else if(a==2	){// scop
			for(int i =0;i<=sc_keys.size()-1;i++){
				// for all the sc clusters
				ArrayList <Integer> temp = SC.get(sc_keys.get(i));
				// for each member of the cluster, if has a classification
				boolean reftoSet = true;
				String cs = "";
				String csRef = "";
				for (int j=0;j<=temp.size()-1;j++){
					cs = SCOP_rev.get(temp.get(j)); 
					if (seqsToCheckRed.contains(temp.get(j))&& cs!=null){
						if(reftoSet){
							// first occurance of sequence with classification
							// therefore set the refernce
							csRef = cs;
							reftoSet = false;
						}
						else {
							// reference domain has been set and is only to be compared
							if(!cs.contains(csRef)){
								System.out.print("\nReport Negative Case SCOP\n");
								System.out.print("SC Cluster: "+sc_keys.get(i)+"\n");
								System.out.print("Classification: "+cs+"\n");
								System.out.print("Classification Reference: "+csRef+"\n");

							}
						}
					}
				}

			}
		}
	}
	private static void uniquefoldsforSeqsToCheck() {
		// TODO Auto-generated method stub
		// to see that the SC clusters associated with pdbs have unique fold in
		// cath scop or not
		// seqsToCheck --> the arraylist of sequences with pdb and in camps sc
		ArrayList<String> counted = new ArrayList<String>();
		int associatedCath = 0;
		int associatedScop = 0;

		for(int x =0; x<=seqsToCheck.size()-1;x++){
			for (int i =0; i<=cath_keys.size()-1;i++){
				ArrayList<Integer> temp = CATH.get(cath_keys.get(i));
				if (temp.contains(seqsToCheck.get(x))){
					if (!counted.equals(cath_keys.get(i))){
						counted.add(cath_keys.get(i));
						associatedCath++;
					}

				}
			}
		}
		for(int x =0; x<=seqsToCheck.size()-1;x++){
			for (int i =0; i<=scop_keys.size()-1;i++){
				ArrayList<Integer> temp = SCOP.get(scop_keys.get(i));
				if (temp.contains(seqsToCheck.get(x))){
					if (!counted.contains(scop_keys.get(i))){
						counted.add(scop_keys.get(i));
						associatedScop++;
					}

				}
			}
		}
		System.out.print("Sequences in SC with pdb structus " + seqsToCheck.size()+"\n");
		System.out.print("Cath&SC Sequences with diff domains in both, shows consistency "+associatedCath+"\n");
		System.out.print("Scop&SC Sequences with diff domains in both "+associatedScop+"\n");



	}

	private static void test() {
		// TODO Auto-generated method stub
		int incath = 0;
		int inscop = 0;
		boolean map = true;
		for (int i =0;i<=cath_keys.size()-1;i++){
			ArrayList<Integer> id = CATH.get(cath_keys.get(i));
			for (int j =0;j<=id.size()-1;j++){
				// now for all the clusters in sc - cant use contains Value cuz its a list of ids
				for(int x = 0;x <=sc_keys.size()-1;x++){
					ArrayList<Integer> cluster_sqids = SC.get(sc_keys.get(x));
					if (cluster_sqids.contains(id.get(j))){
						if (map){
							incath++;
						}
						map = false;
						//System.out.print("True - CATH\n");
						break;
					}
					map = true;
				}
			}
		}
		for (int i =0;i<=scop_keys.size()-1;i++){
			ArrayList<Integer> id = SCOP.get(scop_keys.get(i));
			for (int j =0;j<=id.size()-1;j++){
				for(int x = 0;x <=sc_keys.size()-1;x++){
					ArrayList<Integer> cluster_sqids = SC.get(sc_keys.get(x));
					if (cluster_sqids.contains(id.get(j))){

						if (map){
							inscop++;
						}
						map = false;
						//System.out.print("True - SCOP\n");
						break;
					}
					map = true;

				}

			}
		}
		System.out.print("Number of CATH folds representing 1524 CAMPS clusters is  " + incath +"\n"); // the number of sequences which have a CATH classification
		// and are also present in any SC cluster
		System.out.print("Number of SCOP folds representing 1524 CAMPS clusters is  " + inscop +"\n");

		System.out.print("Number of CATH folds are  " + CATH.size() +"\n");
		System.out.print("Number of SCOP folds are  " + SCOP.size() +"\n");

	}

	private static void Compare(int a,String outpath) {
		// TODO Auto-generated method stub
		ArrayList<String> scWithCath = new ArrayList<String>();
		ArrayList<String> scWithCathNegtv = new ArrayList<String>();
		ArrayList<String> positiveList = new ArrayList<String>();

		ArrayList<Boolean> boolList = new ArrayList<Boolean>();
		// a 1 = cath
		// a 2 = scop
		String aa = "x";
		if (a==1){
			aa = "cath";
		}
		else if (a==2){
			aa = "scop";
		}
		try{
			FileWriter f = new FileWriter(outpath+"positiveCase"+"_"+aa+"_"+"30Identity_Camps.txt");
			BufferedWriter bftbl = new BufferedWriter(new FileWriter(outpath+"positiveCaseTable"+"_"+aa+"_"+"30Identity_Camps.txt"));
			// above is the file to write positive cases in table format
			bftbl.write("#clusterid_threshold\tsequenceId\tPDB\tCATHClassification");
			bftbl.newLine();

			//FileWriter f = new FileWriter("/home/users/saeed/positiveCasePfamnCamps.txt");
			//
			BufferedWriter brpositiv = new BufferedWriter(f);

			FileWriter f1 = new FileWriter(outpath+"negativeCase"+"_"+aa+"_"+"30Identity_Camps.txt");
			BufferedWriter bf1tbl = new BufferedWriter(new FileWriter(outpath+"negativeCaseTable"+"_"+aa+"_"+"30Identity_Camps.txt"));
			bf1tbl.write("#clusterid_threshold\tsequenceId\tPDB\tCATHClassification");
			bf1tbl.newLine();
			// above is the file to write positive cases in table format

			//FileWriter f1 = new FileWriter("/home/users/saeed/negativeCasePfamnCamps.txt");
			BufferedWriter brnegtv = new BufferedWriter(f1);

			int numberofProteinsClasssified = 0;
			int diffDomainCounter = 0;
			int greater70Counter = 0;
			int lesser50Counter = 0;
			int greater85Counter = 0;
			int greater95Counter = 0;
			int greater99Counter = 0;
			int numberofClustersWithSingleClassification = 0;
			int numberofClustersWithMultipleClassification = 0;
			int total = 0;
			int singleCathNoComparison = 0;
			int numberofClustersWithSingleClassificationNegtv = 0;
			int numberofClustersWithMultipleClassificationNegtv = 0;

			int numberofClustersWithClassification = 0;

			for (int i=0;i<=sc_keys.size()-1;i++){
				String clus_th = sc_keys.get(i);
				ArrayList<Integer> clus_seqIds = SC.get(clus_th);

				boolean sameDomain = false;
				boolean standardDomainSet = false;
				//int domainPercentCounter = 0;
				ArrayList<String> standardDomains = new ArrayList<String>();
				int NumberOfSeqWithCATHClass = 0;

				ArrayList<String> table = new ArrayList<String>(); // to write in the file
				if(clus_th.equals("100.0_19942")){
					System.out.println("Test Case");
				}
				// 	set standard domain based on min size of cathfolds of a sequence
				ArrayList<String> dom = seStandardDomain(clus_seqIds); // returns the cath /scop classifications for given seqId
				if(dom.size()!=0 && !dom.isEmpty()){
					standardDomains = dom;
					standardDomainSet = true;
				}
				

				for (int j =0; j<= clus_seqIds.size()-1;j++){
					int currentsq = clus_seqIds.get(j);

					ArrayList<String> keys = getKeysByValue(a,currentsq); // returns the cath /scop classifications for given seqId

					if(keys.size()!=0 && !keys.isEmpty()){
						if(keys.size()>1){
							System.out.println("xxxx");
						}
						if(!scWithCath.contains(clus_th)){
							scWithCath.add(clus_th);
						}
						NumberOfSeqWithCATHClass++;

						String splitkeySet = getStringKey(keys);
						if (standardDomainSet){
							// compare
							// the below if is to eliminate cases where standard domain is contains less or more folds as in keys
							// simply comment it out to remove it uncomment sameDomain = pairwisecompare(standardDomains, keys);
							// REMOVING THE IF BELOW AS IT SEEMS TO INCREASE THE FALSE POSITIVES
							/*if(standardDomains.size()==keys.size()){
								sameDomain = pairwisecompare(standardDomains, keys);
							}
							else{
								sameDomain =  false;
							}*/
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
						if(sameDomain){	// to count the percentage same domains
							//domainPercentCounter++;
							System.out.print(keys.get(0)+"\n");
							String s = clus_th+"\t"+currentsq+"\t"+pdb.get(currentsq)+"\t"+splitkeySet;
							table.add(s);
						}
						else{
							String s = clus_th+"\t"+currentsq+"\t"+pdb.get(currentsq)+"\t"+splitkeySet;
							table.add(s);
							//if (a==1)
							//writeSameDomainCAMPSidAndPfam(sc_keys.get(i),keys,a);
						}
					}
				}

				if(standardDomainSet){
					// the cluster had atleast 1 seq with a cath classification
					numberofClustersWithClassification++;
				}
				float perc = 0;

				if(NumberOfSeqWithCATHClass == 1){ // if only one sequence was classified.. consider it positive
					sameDomain = true;
				}
				/*
				if(domainPercentCounter>0){
					//perc = (clus_seqIds.size()/domainPercentCounter)*100;
					//
					int n = NumberOfSeqWithCATHClass - 1;
					perc = (n/domainPercentCounter)*100;
				}*/

				/*
				if(perc <=50){
					lesser50Counter++;
				}
				if(perc >=50){
					greater70Counter++;
				}
				if(perc >=90){
					greater85Counter++;
				}
				 */
				// calculate same as above using boolList
				boolean any = false;
				boolean all = true;
				int comparisons = boolList.size();
				int countPost = 0;
				int countNegv = 0;

				if(boolList.size()>0){
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
				if(NumberOfSeqWithCATHClass == 1){ // if only one sequence was classified.. consider it positive
					any = true;
					// override the booLlist size to 1 because there were no pairwise comparisons
					comparisons = 1;
					countPost = 1;
					singleCathNoComparison ++;
				}

				//if (!sameDomain){
				//if(any){ // use any if want to check even one is positive
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
					//}
					//if(any){
					if(standardDomains.size() == 1){
						numberofClustersWithSingleClassification++;
					}
					else{
						numberofClustersWithMultipleClassification++;
					}
					// stats
					perc = ((float)countPost/(float)comparisons)*100;
					if(perc <50){
						lesser50Counter++;
					}
					if(perc >=50){
						greater70Counter++;
					}
					if(perc >=90){
						greater85Counter++;
					}
					if(perc >=95){
						greater95Counter++;
					}
					if(perc >=99){
						greater99Counter++;
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

			}
			brpositiv.close();
			brnegtv.close();
			f.close();
			f1.close();
			bftbl.close();
			bf1tbl.close();

			if (a == 1){
				System.out.print("\nRESULTS FOR CATH \n");
			}
			if (a == 2){
				System.out.print("\nRESULTS FOR SCOP \n");
			}

			// to make list of clusterIds which had CATH classification but are negative
			for(int i =0;i<=scWithCath.size()-1;i++){
				String temp = scWithCath.get(i);
				if (!positiveList.contains(temp)){
					scWithCathNegtv.add(temp);
				}
			}
			System.out.print("Total Number of SC clusters " + sc_keys.size() + "\n");
			System.out.print("Total Number of Sequences within these SC clusters " + numberofProteinsClasssified + "\n");
			System.out.print("Exactly same folds in "+aa+" and Camps clusters " + (sc_keys.size()-diffDomainCounter) +"\n");
			System.out.print("Different folds in "+aa+" and Camps clusters " + diffDomainCounter +"\n");
			System.out.print("Size of CATH unique folds" + CATH.size() +"\n");
			System.out.print("Size of SCOP unique folds" + SCOP.size() +"\n");

			System.out.print("Camps clusters with lesser than 50 percent members having same fold " + lesser50Counter +"\n");
			System.out.print("Camps clusters with greater than 50 percent members having same fold " + greater70Counter +"\n");
			System.out.print("Camps clusters with greater than 90 percent members having same fold " + greater85Counter +"\n");
			System.out.print("Camps clusters with greater than 95 percent members having same fold " + greater95Counter +"\n");
			System.out.print("Camps clusters with greater than 99 percent members having same fold " + greater99Counter +"\n");
			System.out.println("************************");
			System.out.println("Number of Clusters with cath classification "+ numberofClustersWithClassification);
			System.out.println("Number of Clusters with cath classification - second way "+ scWithCath.size());
			System.out.println("Number of total Clusters in agreement with cath "+ total);
			System.out.println("Number of clusters eaching with only one sequence with a CATH fold, so no pairwise comparison" +
					" but considered positive: "+singleCathNoComparison);
			System.out.println("Number of Clusters with all the members having only one Cath classification "+ numberofClustersWithSingleClassification);
			System.out.println("Number of Clusters with multiple classifications "+ numberofClustersWithMultipleClassification);
			System.out.println("Number of Clusters with all the members having only one Cath classification and Reported Negative "+ numberofClustersWithSingleClassificationNegtv);
			System.out.println("Number of Clusters with multiple classifications and reported Negative"+ numberofClustersWithMultipleClassificationNegtv);
			System.out.println("Number of Clusters with CATH classification and No agreement: "+ scWithCathNegtv.size());			//
			System.out.println("these are: :"+ scWithCathNegtv);			//
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

	private static void Populatepdb() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("" +
					"select sequenceid, pdb_name from camps2pdb");
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

	private static ArrayList<Integer> PopulatepdbFile(String f) {
		// TODO Auto-generated method stub
		ArrayList<Integer> sq = new ArrayList<Integer>();
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(f)));
			String line = "";
			while((line=br.readLine())!=null){
				line.trim();
				String [] parts = line.split("\t");
				String idString = parts[0].trim();
				String struct = parts[1].trim();
				Integer id  = Integer.parseInt(idString);
				Float Identity = Float.parseFloat(parts[2].trim());
				if(Identity > 29){
					if (pdb.containsKey(id)){
						String s = pdb.get(id);
						struct = struct + "#" + s ;
						pdb.put(id, struct);
					}
					else{
						pdb.put(id, struct);
						sq.add(id);
					}
				}
			}
			br.close();
			return sq;
		}
		catch(Exception e ){
			e.printStackTrace();
		}
		return sq;
	}

	private static void writeSameDomainCAMPSidAndPfam(String string, ArrayList<String> keys, int aa) {
		// TODO Auto-generated method stub
		String struct = "";
		int id = 0;
		ArrayList<Integer> seqids_pfm = new ArrayList<Integer>();
		// keys are the pfam domains
		boolean found = false; 

		try{
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select max(tms_id) from tms where sequenceid=?");
			if (aa == 1){
				// cath

				for (int j =0; j <=keys.size()-1;j++){
					seqids_pfm = CATH.get(keys.get(j));
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

							if(max>2){
								System.out.print("PDB Positive Case\n");
								System.out.print("CampsSeqId: " + id + "\n");
								System.out.print("CampsClusId&Theresh: " + string + "\n");
								System.out.print("CathId: " + keys.get(j) + "\n");
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
			else if (aa == 2){

				for (int j =0; j <=keys.size()-1;j++){
					seqids_pfm = SCOP.get(keys.get(j));
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

							if(max>2){
								System.out.print("PDB Positive Case\n");
								System.out.print("CampsSeqId: " + id + "\n");
								System.out.print("CampsClusId&Theresh: " + string + "\n");
								System.out.print("SCOPId: " + keys.get(j) + "\n");
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
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static boolean pairwisecompare(ArrayList<String> standardDomains,
			ArrayList<String> keys) { // all the folds in standard set be present in the current set
		// TODO Auto-generated method stub
		boolean m = false;
		int countTrue = 0;
		for (int i =0;i<=keys.size()-1;i++){
			String fold = keys.get(i);
			m = false;
			for (int j =0;j<=standardDomains.size()-1;j++){
				String folds = standardDomains.get(j);
				if (fold.equals(folds)){
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
	private static ArrayList<String> seStandardDomain(ArrayList<Integer> clus_seqIds) { // returns the standard domain based on the min size of folds	
		ArrayList<String> minDom = new ArrayList<String>();

		for (int j =0; j<= clus_seqIds.size()-1;j++){
			ArrayList<String> keys = new ArrayList<String>();
			int currentseqIdSC = clus_seqIds.get(j);

			for (int i =0; i<=cath_keys.size()-1;i++){
				ArrayList<Integer> sqs = CATH.get(cath_keys.get(i));
				if(sqs.contains(currentseqIdSC)){
					keys.add(cath_keys.get(i));
					//System.out.print("True - CATH\n");
					//	break;
				}
			}
			if(keys.size()!=0 && !keys.isEmpty()){
				if (minDom.size()>keys.size() || minDom.size() == 0){
					minDom = keys;
				}
			}
		}

		return minDom;

	}
	public static ArrayList<String> getKeysByValue(int a, int currentseqIdSC) {
		ArrayList<String> keys = new ArrayList<String>();
		// a 1 -> cath
		// a 2 -> scop
		if (a ==1){
			for (int i =0; i<=cath_keys.size()-1;i++){
				ArrayList<Integer> sqs = CATH.get(cath_keys.get(i));
				if(sqs.contains(currentseqIdSC)){
					keys.add(cath_keys.get(i));
					//System.out.print("True - CATH\n");
					//	break;
				}
			}
			return keys;
		}
		if (a ==2){
			for (int i =0; i<=scop_keys.size()-1;i++){
				ArrayList<Integer> sqs = SCOP.get(scop_keys.get(i));
				if(sqs.contains(currentseqIdSC)){
					keys.add(scop_keys.get(i));
					//System.out.print("True - SCOP\n");
					//	break;
				}
			}
			return keys;
		}
		return keys;

	}

	private static void getCathIdsforSeqs() {
		// TODO Auto-generated method stub
		try{

			ArrayList<Integer> sequencesWithStructs= new ArrayList<Integer>();

			ArrayList<Integer> sqids = new ArrayList<Integer>();
			ArrayList<String> pdb_names = new ArrayList<String>();

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("" +
					"Select sequenceid,pdb_name from camps2pdb");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("" +
					"Select classification,domain,db,pdb_id from pdbtm2scop_cath");
			// to get sequence and check the problem of similarity btw clusters
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("" +
					"Select sequence from sequences2 where sequenceid=?");

			ResultSet rs = pstm1.executeQuery();
			while(rs.next()){
				pdb_names.add(rs.getString("pdb_name"));
				sqids.add(rs.getInt("sequenceid"));
			}
			rs.close();
			pstm1.close();
			//*****
			// since the pdbtm2scopcath has more pdb sequences than in camps2pdb, therefore, first we got all the 
			// proteins which are pdb and exist in camps. The greater number of sequences in camps2pdb makes sense 
			// because the mapping is between pdbtm and cath scop and pdbtm has all sequences i.e. pdbtm has redundant sequences
			// Since we wanted all the sequences and their mapping with CATH and SCOP.. where as CAMPS has non redundant data..
			// Therefore there is a mismatch in these number of sequences btw the two tables
			// Moreover the nr data from pdbtm has only 400 sequences..therefore used NR to get a detail and more number of sequences
			// that could be mapped back to CAMPS


			int nrCountcath = 0; // the count of number of different cs domains in all sc clusters
			int nrCountscop = 0; // the count of number of different cs domains in all sc clusters
			ArrayList<String> countedCs = new ArrayList<String>();

			ResultSet rs2 = pstm2.executeQuery();
			while(rs2.next()){
				String cs = rs2.getString("classification");
				String db = rs2.getString("db");

				// get the domain... 
				int first = cs.indexOf(".");
				int second = cs.indexOf(".", first+1);
				int third = cs.indexOf(".", second+1);
				cs = cs.substring(0,third);
				//String domain = rs2.getString("domain");
				String pdb_id = rs2.getString("pdb_id");

				// this pdb is present in camps and has a mapping
				if (pdb_names.contains(pdb_id)){
					int x =pdb_names.indexOf(pdb_id);
					int sq = sqids.get(x);

					if (db.contains("cath")){
						if (CATH.containsKey(cs)){
							ArrayList<Integer> temp = CATH.get(cs);
							if (!temp.contains(sq)){
								temp.add(sq);
								CATH.put(cs, temp);
							}
						}
						else{
							ArrayList<Integer> temp = new ArrayList<Integer>();
							temp.add(sq);
							sequencesWithStructs.add(sq);
							CATH.put(cs, temp);
							cath_keys.add(cs);
							// to check if sequ is also in sc 
							if (SC_seqIds.containsKey(sq)&&!countedCs.contains(cs)){
								nrCountcath++;
							}
						}
						if(!CATH_rev.containsKey(sq)){
							CATH_rev.put(sq, cs);
						}
					}
					else{
						if (SCOP.containsKey(cs)){
							ArrayList<Integer> temp = SCOP.get(cs);
							if (!temp.contains(sq)){
								temp.add(sq);
								SCOP.put(cs, temp);
							}
						}
						else{
							ArrayList<Integer> temp = new ArrayList<Integer>();
							temp.add(sq);
							if (!sequencesWithStructs.contains(sq)){
								sequencesWithStructs.add(sq);
							}
							SCOP.put(cs, temp);
							scop_keys.add(cs);
							if (SC_seqIds.containsKey(sq)&&!countedCs.contains(cs)){
								nrCountscop++;
							}
						}
						if(!SCOP_rev.containsKey(sq)){
							SCOP_rev.put(sq, cs);
						}
					}

				}
			}
			rs2.close();
			pstm2.close();
			//****
			// calculating number of clusters with structures
			//sqids --> all sequences in camps with structures
			//pdb_names -> CAMPS2PDB and has same index as seqids
			// sequencesWithStructs -- sequences in camps with cath scop refernce
			FileWriter f = new FileWriter("F:/structs_mcl05");
			BufferedWriter brf = new BufferedWriter(f);

			int countStructuralClusters = 0;
			int count_proteinsInclusterswithStructs = 0;

			ArrayList <String> countedCluster = new ArrayList<String>();
			for (int i =0;i<=sqids.size()-1; i++){
				for(int j =0;j<=sc_keys.size()-1;j++){
					ArrayList <Integer> temp = SC.get(sc_keys.get(j));
					if (temp.contains(sqids.get(i))){
						// if the cluster contains the key
						// if the cluster has not been counted before
						if (!countedCluster.contains(sc_keys.get(j))){
							// get the pdbname and sequenceID of camps, get sequence and print to fasta
							pstm3.setInt(1, sqids.get(i));
							ResultSet rs3 = pstm3.executeQuery();
							String seq = "";
							while(rs3.next()){
								seq = rs3.getString("sequence");
							}
							//System.out.print(sqids.get(i)+" - CAMPS SeqId");
							//System.out.print(pdb_names.get(i)+" - PDB SeqId");
							//System.out.print(seq+" - Seq");
							brf.write(">"+sqids.get(i)+"."+pdb_names.get(i));
							brf.newLine();
							brf.write(seq);
							brf.newLine();

							countStructuralClusters++;
							countedCluster.add(sc_keys.get(j));
							seqsToCheck.add(sqids.get(i));
						}
						seqsToCheckRed.add(sqids.get(i));
						count_proteinsInclusterswithStructs ++;

					}
				}

			}
			brf.close();
			f.close();



			System.out.print("File Written"+"\n");
			System.out.print("Number of sequences with structures in SC clusters " +count_proteinsInclusterswithStructs+"\n");
			System.out.print("SC clusters associated with pdb structs " +countStructuralClusters+"\n");

			System.out.print("Number of cath domains in pdbtm and CAMPS " +CATH.size()+"\n");
			System.out.print("Number of scop domains in pdbtm and CAMPS " +SCOP.size()+"\n");

			System.out.print("Number of cath domains in sc clusters " +nrCountcath+"\n");
			System.out.print("Number of scop domains in sc clusters " +nrCountscop+"\n");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void getCathIdsforSeqsFile(ArrayList<Integer> sqids,String campCathMapF,boolean supfam) { // if the bool is true.. instead of using the fold level, superfamily level is used.  
		// TODO Auto-generated method stub
		try{
			ArrayList<Integer> sequencesWithStructs= new ArrayList<Integer>();

			//ArrayList<Integer> sqids = new ArrayList<Integer>();
			ArrayList<String> pdb_names = new ArrayList<String>();

			//PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("" +
			//		"Select sequenceid,pdb_name from camps2pdb");

			//	PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("" +
			//			"Select classification,domain,db,pdb_id from pdbtm2scop_cath");
			// to get sequence and check the problem of similarity btw clusters
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("" +
					"Select sequence from sequences2 where sequenceid=?");

			/*
			ResultSet rs = pstm1.executeQuery();
			while(rs.next()){
				pdb_names.add(rs.getString("pdb_name"));
				sqids.add(rs.getInt("sequenceid"));
			}
			rs.close();
			pstm1.close();
			 */
			for(int i =0;i<=sqids.size()-1;i++){
				Integer id = sqids.get(i);
				pdb_names.add(pdb.get(id));
			}

			//*****
			// since the pdbtm2scopcath has more pdb sequences than in camps2pdb, therefore, first we got all the 
			// proteins which are pdb and exist in camps. The greater number of sequences in camps2pdb makes sense 
			// because the mapping is between pdbtm and cath scop and pdbtm has all sequences i.e. pdbtm has redundant sequences
			// Since we wanted all the sequences and their mapping with CATH and SCOP.. where as CAMPS has non redundant data..
			// Therefore there is a mismatch in these number of sequences btw the two tables
			// Moreover the nr data from pdbtm has only 400 sequences..therefore used NR to get a detail and more number of sequences
			// that could be mapped back to CAMPS


			int nrCountcath = 0; // the count of number of different cs domains in all sc clusters
			int nrCountscop = 0; // the count of number of different cs domains in all sc clusters
			ArrayList<String> countedCs = new ArrayList<String>();

			//classification -- domain -- pdbid -- db
			// read camps2cath to get the pdbs and classification and seq id
			String campCathFile = campCathMapF;
			BufferedReader br = new BufferedReader(new FileReader(new File(campCathFile)));
			String line = "";
			HashMap <Integer,Integer> UniqueMappedSeqCamps = new HashMap<Integer,Integer>();
			while((line=br.readLine())!=null){
				String parts[] = line.split("\t");
				String campsId = parts[0].trim();
				String cs = parts[2].trim();
				Integer sq = Integer.parseInt(campsId);
				if(!UniqueMappedSeqCamps.containsKey(sq)){
					UniqueMappedSeqCamps.put(sq, null);
				}
				if(!supfam){ // if the comparison needs to be at fold level. supfam is false else true
					int first = cs.indexOf(".");
					int second = cs.indexOf(".", first+1);
					int third = cs.indexOf(".", second+1);
					cs = cs.substring(0,third);
					// for architecture level comparison, uncomment below
					//cs = cs.substring(0,second);
				}
				if (CATH.containsKey(cs)){
					ArrayList<Integer> temp = CATH.get(cs);
					if (!temp.contains(sq)){
						temp.add(sq);
						CATH.put(cs, temp);
					}
				}
				else{
					ArrayList<Integer> temp = new ArrayList<Integer>();
					temp.add(sq);
					sequencesWithStructs.add(sq);
					CATH.put(cs, temp);
					cath_keys.add(cs);
					// to check if sequ is also in sc 
					if (SC_seqIds.containsKey(sq)&&!countedCs.contains(cs)){
						nrCountcath++;
					}
				}
				if(!CATH_rev.containsKey(sq)){
					CATH_rev.put(sq, cs);
				}

				//}
			}
			br.close();
			//****
			// calculating number of clusters with structures
			//sqids --> all sequences in camps with structures
			//pdb_names -> CAMPS2PDB and has same index as seqids
			// sequencesWithStructs -- sequences in camps with cath scop refernce
			FileWriter f = new FileWriter("F:/reRUNstructs_mcl05");
			BufferedWriter brf = new BufferedWriter(f);

			int countStructuralClusters = 0;
			int count_proteinsInclusterswithStructs = 0;

			ArrayList <String> countedCluster = new ArrayList<String>();
			for (int i =0;i<=sqids.size()-1; i++){
				for(int j =0;j<=sc_keys.size()-1;j++){
					ArrayList <Integer> temp = SC.get(sc_keys.get(j));
					if (temp.contains(sqids.get(i))){
						// if the cluster contains the key
						// if the cluster has not been counted before
						if (!countedCluster.contains(sc_keys.get(j))){
							// get the pdbname and sequenceID of camps, get sequence and print to fasta
							pstm3.setInt(1, sqids.get(i));
							ResultSet rs3 = pstm3.executeQuery();
							String seq = "";
							while(rs3.next()){
								seq = rs3.getString("sequence");
							}
							//System.out.print(sqids.get(i)+" - CAMPS SeqId");
							//System.out.print(pdb_names.get(i)+" - PDB SeqId");
							//System.out.print(seq+" - Seq");
							brf.write(">"+sqids.get(i)+"."+pdb_names.get(i));
							brf.newLine();
							brf.write(seq);
							brf.newLine();

							countStructuralClusters++;
							countedCluster.add(sc_keys.get(j));
							seqsToCheck.add(sqids.get(i));
						}
						seqsToCheckRed.add(sqids.get(i));
						count_proteinsInclusterswithStructs ++;

					}
				}

			}
			brf.close();
			f.close();



			System.out.print("File Written"+"\n");
			System.out.print("Number of sequences with structures in SC clusters " +count_proteinsInclusterswithStructs+"\n");
			System.out.print("SC clusters associated with pdb structs " +countStructuralClusters+"\n");

			System.out.print("Number of cath domains in pdbtm and CAMPS " +CATH.size()+"\n");
			System.out.print("Number of scop domains in pdbtm and CAMPS " +SCOP.size()+"\n");

			System.out.print("Number of cath domains in sc clusters " +nrCountcath+"\n");
			System.out.print("Number of scop domains in sc clusters " +nrCountscop+"\n");
			System.out.println("***\n Number of Camps sequences Mapped to CATH "+UniqueMappedSeqCamps.size()+"****\n");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void PopulategetSC() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_threshold = ? and cluster_id = ?");
			for (int i =0; i <=files.size()-1;i++){
				System.out.println("In Process: "+i+"/"+files.size());
				String f = files.get(i);
				//cluster_5.0_0.hmm
				//cluster_5.0_46.hmm
				int idx = f.indexOf(".hmm");
				f = f.substring(0,idx);
				//System.out.print(f+"\n");
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
					int id = rs.getInt("sequenceid");
					sqids.add(id);
					SC_seqIds.put(id, 0);
				}
				rs.close();
				//SC.put(clus, sqids);
				//sc_keys.add(clus);
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
