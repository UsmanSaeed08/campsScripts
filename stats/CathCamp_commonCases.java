package stats;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

import utils.DBAdaptor;

public class CathCamp_commonCases {

	/**
	 * @param args
	 */

	private static ArrayList<String> files = new ArrayList<String>();
	//key - clusterId ..... value - cluster_members = sequenceID
	private static HashMap <String, ArrayList<Integer>> SC = new HashMap<String,ArrayList<Integer>>();
	private static HashMap <Integer, ArrayList<Integer>> SC_MCL = new HashMap<Integer,ArrayList<Integer>>();
	// key classification and value is seqId
	private static HashMap <String, ArrayList<Integer>> SCOP = new HashMap<String, ArrayList<Integer>>();
	private static HashMap <String, ArrayList<Integer>> CATH = new HashMap<String, ArrayList<Integer>>();
	private static HashMap <Integer, String> pdb = new HashMap<Integer, String>(); // seqid - pdbid
	// for mcl cluster ids... key of hashmap sc_mcl
	private static ArrayList<Integer> clusids_mcl = new ArrayList<Integer>();
	private static ArrayList<String> cath_keys = new ArrayList<String>();
	private static ArrayList<String> scop_keys = new ArrayList<String>();

	// sequences with structures and different clusters - to be checked in cathscop
	private static ArrayList <Integer> seqsToCheck = new ArrayList<Integer>(); // sequences


	private static ArrayList<String> sc_keys = new ArrayList<String>();

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		//System.out.print("ok");
		run("F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/HMMs/CAMPS4_1/");
		//run("/home/proj/check/RunMetaModel_gef/HMMs/CAMPS4_1/");


	}

	private static void run(String string){
		// get sc cluster sequenceids
		// use these seqids to get pdbids from camps2pdb
		// use these pdbids to get cath and scop domains separately
		System.out.print("Getting files" + "\n");
		getfiles(string);
		System.out.print("Populating SC" + "\n");
		PopulategetSC();
		PopulategetSC_MCL();
		Populatepdb();
		System.out.print("Getting CathScopIds" + "\n");
		getCathIdsforSeqs();
		//

		System.out.print("Compairing" + "\n");
		Compare(1); // cath
		Compare(2); // scop
		test();
		uniquefoldsforSeqsToCheck();


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

	private static void Compare(int a) {
		// TODO Auto-generated method stub
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
			FileWriter f = new FileWriter("F:/positiveCase"+"_"+aa+"_"+"Camps.txt");
			//FileWriter f = new FileWriter("/home/users/saeed/positiveCasePfamnCamps.txt");
			//
			BufferedWriter brpositiv = new BufferedWriter(f);

			FileWriter f1 = new FileWriter("F:/negativeCase"+"_"+aa+"_"+"Camps.txt");
			//FileWriter f1 = new FileWriter("/home/users/saeed/negativeCasePfamnCamps.txt");
			BufferedWriter brnegtv = new BufferedWriter(f1);

			int numberofProteinsClasssified = 0;
			int diffDomainCounter = 0;
			int greater70Counter = 0;

			for (int i=0;i<=sc_keys.size()-1;i++){
				ArrayList<Integer> clus_seqIds = SC.get(sc_keys.get(i));

				boolean sameDomain = false;
				boolean standardDomainSet = false;
				int domainPercentCounter = 0;
				ArrayList<String> standardDomains = new ArrayList<String>();


				for (int j =0; j<= clus_seqIds.size()-1;j++){
					int currentsq = clus_seqIds.get(j);

					ArrayList<String> keys = getKeysByValue(a,currentsq);
					if(keys.size()!=0 && !keys.isEmpty()){
						if (standardDomainSet){
							// compare
							sameDomain = pairwisecompare(standardDomains, keys);
						}
						else{
							// set standard
							standardDomains = keys;
							standardDomainSet = true;
						}
						if(sameDomain){	// to count the percentage same domains
							domainPercentCounter++;
							System.out.print(keys.get(0)+"\n");
						}
						else{
							if (a==2)
								writeSameDomainCAMPSidAndPfam(sc_keys.get(i),keys,a);
						}
					}
				}
				float perc = 0;
				if(domainPercentCounter>0){
					perc = (clus_seqIds.size()/domainPercentCounter)*100;
				}

				if(perc >=40){
					greater70Counter++;
				}
				if (!sameDomain){
					diffDomainCounter++;
					brnegtv.write(sc_keys.get(i));
					brnegtv.newLine();
				}
				else{
					brpositiv.write(sc_keys.get(i));
					brpositiv.newLine();
				}

			}
			brpositiv.close();
			brnegtv.close();
			f.close();
			f1.close();

			if (a == 1){
				System.out.print("\nRESULTS FOR CATH \n");
			}
			if (a == 2){
				System.out.print("\nRESULTS FOR SCOP \n");
			}

			System.out.print("Total Number of SC clusters " + sc_keys.size() + "\n");
			System.out.print("Total Number of Sequences within these SC clusters " + numberofProteinsClasssified + "\n");
			System.out.print("Exactly same folds in "+aa+" and Camps clusters " + (sc_keys.size()-diffDomainCounter) +"\n");
			System.out.print("Different folds in "+aa+" and Camps clusters " + diffDomainCounter +"\n");
			System.out.print("Size of CATH unique folds" + CATH.size() +"\n");
			System.out.print("Size of SCOP unique folds" + SCOP.size() +"\n");
			System.out.print("Camps clusters with greater than 40 percent members having same fold " + greater70Counter +"\n");
		}
		catch(Exception e){
			e.printStackTrace();
		}
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
			ArrayList<String> keys) {
		// TODO Auto-generated method stub
		boolean m = false;/*
		for (int i =0; i<=keys.size()-1;i++){
			m = false;
			for (int j=0;j<=standardDomains.size()-1;j++){
				if (keys.get(i).contains(standardDomains.get(j))){
					m = true;
					break;
				}
			}
		}*/
		for (int i =0;i<=keys.size()-1;i++){
			String fold = keys.get(i);
			for (int j =0;j<=standardDomains.size()-1;j++){
				String folds = standardDomains.get(j);
				if (fold.equals(folds)){
					m = true;
					break;
				}
			}
		}
		return m;
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
			
			
			
			ResultSet rs2 = pstm2.executeQuery();
			while(rs2.next()){
				String cs = rs2.getString("classification");
				String db = rs2.getString("db");
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
						count_proteinsInclusterswithStructs ++;

					}
				}

			}
			/*
			for (int i =0;i<=sqids.size()-1; i++){
				for(int j =0;j<=clusids_mcl.size()-1;j++){
					ArrayList <Integer> temp = SC_MCL.get(clusids_mcl.get(j));
					if (temp.contains(sqids.get(i))){
						// if the cluster contains the key
						// if the cluster has not been counted before
						if (!countedCluster.contains(clusids_mcl.get(j))){
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
							countedCluster.add(clusids_mcl.get(j));
							seqsToCheck.add(sqids.get(i));
						}
						count_proteinsInclusterswithStructs ++;

					}
				}

			}

			/*
			 * ArrayList <String> countedCluster = new ArrayList<String>();
			 * for (int i =0;i<=sqids.size()-1; i++){
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
						count_proteinsInclusterswithStructs ++;

					}
				}

			}*/
			brf.close();
			f.close();
			System.out.print("File Written"+"\n");
			System.out.print("Number of sequences with structures in SC clusters " +count_proteinsInclusterswithStructs+"\n");
			System.out.print("SC clusters associated with pdb structs " +countStructuralClusters+"\n");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private static void PopulategetSC_MCL() {
		// TODO Auto-generated method stub
		try{
			

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select distinct cluster_id from clusters_mcl where cluster_threshold = 5");
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl where cluster_id = ? and cluster_threshold = 5");
			ResultSet r = pstm.executeQuery();
			// got the clusterids
			while(r.next()){
				clusids_mcl.add(r.getInt("cluster_id"));
			}
			r.close();

			for (int i =0 ; i<=clusids_mcl.size()-1;i++){
				pstm1.setInt(1, clusids_mcl.get(i));
				// get the sequences in clusters
				ArrayList<Integer> sq = new ArrayList<Integer>();
				ResultSet r1 = pstm1.executeQuery();
				while(r1.next()){
					sq.add(r1.getInt("sequenceid"));
				}
				SC_MCL.put(clusids_mcl.get(i), sq);
			}
			r.close();


			pstm.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void PopulategetSC() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl where cluster_threshold = ? and cluster_id = ?");
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
