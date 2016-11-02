package stats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import org.biojava.bio.symbol.Alphabet;
import org.biojava.bio.symbol.AlphabetManager;
import org.biojavax.SimpleNamespace;
import org.biojavax.bio.seq.RichSequence;
import org.biojavax.bio.seq.RichSequenceIterator;

import computeResults.CalcStats;

import utils.DBAdaptor;

public class MclAndSCcluster {

	/**
	 * @param args
	 * the class calculates the stats regarding mcl clusters - initial clusters
	 * also the same for SC clusters and 
	 * and the sequence coverage at each threshold
	 */

	/*
	 * my detailed survey reveals that the condition to put in alignments is not very right
	 * but it dint affect much because the constructor wasnt called at every new line
	 * therefore almost ended up entering everything in table, because
	 * seqid_hit + seqid_query both distinct > 1013102... 
	 * the main problem occured when I further used this table to make mcl clusters
	 * i only picked up sequences from seqid_hit column and not query
	 * so missed out alot of sequences
	 * lets re run all and fix
	 * first fix the conditions and remember to reset values in alignments table by calling
	 * the constructor
	 * 
	 * 2878194297 - relationships in alignment file
	 * processed 131580287
	 * remaining lines 2746614010
	 * 44300250 * 62
	 * 

	 * 
	 * 131500000
	 * 56700000
	 * 
	 * id 29411417
	 * at line 131580287
	 * 
	 * 
	 *  
	 * 
	 * 			total sequences 1013102
	 * 
	 * 			select count(distinct seqid_query) from alignments_initial;
	 *			375934
	 *			select count(distinct seqid_hit) from alignments_initial;		*loss due to tm coverage condition*
	 *			652559 
	 *			this means that 15391 protein with TM 1 are still included
	 *number of unique sequences in alignments table
irrespective of the query or hit... so this is the
number of combinations
652562
	 * 
	 *			select count(distinct sequenceid) from clusters_mcl;			*almost the same*
	 *			623819
	 *
	 *			HMM Step
	 *			26614 clusters ignored due to cluster size <15
	 *			~300,000 sequences directly in the clusters	
	 *													*loss due to min cluster size and *
	 *
	 *			single TM protein
	 *			359944
	 *
	 *			Understand the concept, all 1013102 proteins were compared to one another and have a similarity matrix
	 *			So now in alignments table Only those relationships are considered in which the conditions are: 
	 *
	 *			covered_tms_query > 2 || covered_tms_perc_query > 40 || covered_tms_hit > 2 || covered_tms_perc_hit > 40
	 *			alignment_coverage_query > 0.5 && alignment_coverage_hit > 0.5  
	 *			bitscore > 50
	 *
	 *			this is the first major drop in sequence.... because the protein does not have a relationship with any other under 
	 *			the given criteria 
	 *
	 *			select count(sequenceid) from clusters_mcl;
	 *			16823971 
	 *
	 * find the select distinct covered_tms_query from alignments_initial;
	 * and also 
	 * just found out that there can no single tm protien go through this condition... but there are some that did pass through.. how? i checked
	 * there covered tms and it is clearly a junk value, dont know where did this junk come in from.
	 * how ever, i would now have to see how many of such single membrane passed through
	 * Recalculate all camps?
	 * 
	 */

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("Hey\n");
		// sc clusters can be found on hard drive
		// mcl clusters are in mysql
		//count_mcl();
		// find all the proteins with 1 tm in alignments
		//int TmNoToChose = 1;
		//ArrayList<Integer> a = algnCheck2(TmNoToChose);
		//algnCheck(a);
		//test();
		//test2();
		//test3();
		//test4();
		//test5(args);
		//test6();
		//test7();
		//test8(args);
		//test9();
		test10();	// finds the number of sequences which have if(evalue < 1.0E-5 && bitscore > 50)
		//fileCutter();
		//getprots();
		//statLostProts();
	}
	private static void test10() {
		// TODO Auto-generated method stub
		try{
			// first get the lost sequences
			int idx =1;
			HashMap <Integer, Integer> seqs = new HashMap<Integer, Integer>(); // key - seqid and val is statement
			for (int i=1; i<=17;i++){
				PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("select " +
						"seqid_query,seqid_hit from alignments_initial limit "+idx+","+10000000);
				idx = i*10000000;
				//System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
				int setsize = 0;
				ResultSet rs3 = pstm3.executeQuery();
				while (rs3.next()){
					setsize++;
					int q = rs3.getInt(1);
					int h = rs3.getInt(2);
					if (!seqs.containsKey(q)){
						seqs.put(q, null);
					}
					if (!seqs.containsKey(h)){
						seqs.put(h, null);
					}
				}
				System.out.print("\n Hashtable of Alignments "+idx+", the set size was:"+setsize+"\n");
			}
			HashMap <Integer, Integer> seqsAll = new HashMap<Integer, Integer>(); // sequenceids in sequences2
			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("select distinct sequenceid from sequences2");
			ResultSet rs3 = pstm3.executeQuery();
			while (rs3.next()){
				seqsAll.put(rs3.getInt(1), null);
			}

			// now remove sequenceIds which are in alignments to get all the sequenceIds not in alignments

			for (Integer key : seqs.keySet()) {
				seqsAll.remove(key);
			}
			// NOW seqsAll has all the lost sequences;
			seqs.clear();

			HashMap <Integer, Integer> ProtsExcluded = new HashMap<Integer, Integer>();// HashTable of all sequences, excluded only due to bitscore and not Evalue


			FileReader fr = new FileReader(new File("/scratch/usman/download/camps_seq_file.matrix"));
			BufferedReader br = new BufferedReader(fr);
			String s = "";
			//int i =0;
			while ((s = br.readLine())!=null){
				//i++;
				String p[] = s.split("\t");

				int seqidquery = Integer.parseInt(p[0]);
				int seqidhit = Integer.parseInt(p[1]);
				double bitscore = Double.parseDouble(p[3]);
				double evalue= Double.parseDouble(p[4]);
				if (bitscore<50 && evalue < 1.0E-5){		// adds the relationship in hashtable
					if(seqsAll.containsKey(seqidquery)||seqsAll.containsKey(seqidhit)){
						ProtsExcluded.put(seqidquery, null);
						ProtsExcluded.put(seqidhit, null);
					}
				}
				/*
				if(evalue < 1.0E-5 && bitscore > 50){
					if (ProtsExcluded.containsKey(seqidquery)){
						ProtsExcluded.remove(seqidquery);
					}
					if(ProtsExcluded.containsKey(seqidhit)){
						ProtsExcluded.remove(seqidhit);
					}
				}*/
			}
			System.out.print("\n The number of sequences removed only because of bitScore is: "+ProtsExcluded.size()+"\n");
			System.out.print("\n Therefore, this would be the number increased if bitScore Abolished\n");
			br.close();
			fr.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}
	/**
	 * Divides the mentioned file into x parts
	 * 
	 */
	private static void test9() {
		// TODO Auto-generated method stub
		try{
			FileReader fr = new FileReader(new File("/scratch/usman/Camps4/blastOutput/drugBank/temp.blast"));
			BufferedReader br = new BufferedReader(fr);
			int x = 100000;
			int fileNumber = 1;
			FileWriter fw = new FileWriter(new File("/scratch/usman/Camps4/blastOutput/drugBank/parts/camps4_part"+fileNumber+"_drugbank.blast"));
			BufferedWriter bw = new BufferedWriter(fw);
			String line = "";
			int i =1; // line number
			while((line = br.readLine())!= null){
				if (i%x == 0){
					bw.close();
					fw.close();
					fileNumber++;

					fw = new FileWriter(new File("/scratch/usman/Camps4/blastOutput/drugBank/parts/camps4_part"+fileNumber+"_drugbank.blast"));
					bw = new BufferedWriter(fw);
				}
				bw.write(line);
				bw.newLine();
				i++;
			}
			bw.close();
			fw.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}


	}

	// reads the clusters That were to be split in thresh 101 but stopped at 100
	// therefore, trying to see the lost proteins in these clusters
	private static void test8(String[] args) {
		// TODO Auto-generated method stub
		String path = "F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/tmp/CAMPS4_1/";
		final File folder = new File(path);
		int count = 0;
		HashMap <Integer, Integer> Prots = new HashMap<Integer, Integer>();// list of proteins lost as no cluster made in last split round 100 thresh
		ArrayList<String> files_results = new ArrayList<String>();

		for (final File fileEntry : folder.listFiles()) {
			String fileName = fileEntry.getName();
			if (fileEntry.isDirectory()) {

			} else if (fileName.endsWith(".txt") && fileName.contains("CAMPS_cluster100.0_")) {
				count++;
				files_results.add(fileEntry.getName().toString());
			}
		}

		System.out.println("\n total file " + count + " \n");
		for(int i =0; i<=files_results.size()-1;i++){
			String ff = files_results.get(i);
			///System.out.print(f+"\n");
			try{
				//FileReader f = new FileReader(new File("F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/ClusterstoSplit101.txt"));
				FileReader f = new FileReader(new File("F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/tmp/CAMPS4_1/"+ff));
				BufferedReader br = new BufferedReader(f);
				String s = "";
				count =0;
				while((s = br.readLine())!= null){
					s = s.trim();
					Integer cluster_id = Integer.valueOf(s);
					//double threshold = 100.0;
					count++;
					System.out.print("Processing Line: " + count +"\n");
					PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("" +
							"select sequenceid from clusters_mcl where cluster_threshold=100 and cluster_id=?");
					pstm1.setInt(1, cluster_id);
					ResultSet rs1 = pstm1.executeQuery();

					while(rs1.next()){
						int p = rs1.getInt(1);
						if(!Prots.containsKey(p)){
							Prots.put(p,null);
						}
					}



					/*
				count ++;
				System.out.print("Processing Line: " + count +"\n");
				String query = "SELECT DISTINCT " + "clusters_mcl" + "." + "cluster_id" + ", " + 
						"sequences2" + "." + "sequence" +
						" FROM "  + "clusters_mcl" + ", "+ "sequences2" + ", " + "tms_cores" + 
						" WHERE " + "clusters_mcl" + "." + "sequenceid" + "=" + "sequences2" + "." + "sequenceid" + 
						" AND "   + "clusters_mcl" + "." + "sequenceid" +	"=" + "tms_cores" + "." + "sequenceid" + 
						" AND "   + "clusters_mcl" + "." + "cluster_id" + " = " + cluster_id + 
						" AND "   + "clusters_mcl" + "." + "cluster_threshold" + " = " + threshold + 
						" AND "   + "tms_cores" + "."+ "cluster_id" + "=" + cluster_id + 
						" AND "   + "tms_cores" + "."+ "cluster_threshold" + "=" + threshold;
				Statement statement = CAMPS_CONNECTION.createStatement();
				ResultSet result = statement.executeQuery(query);
				while (result.next()) 
				{
					String camperCode = result.getString("sequenceid");
					//String sequence = result.getString(Global.proteins_sequence);	
					//String description = "";
					//				System.out.println(camperCode);
					if(!Prots.containsKey(camperCode)){
						Prots.put(camperCode, null);
					}
				}

				result.close();
				statement.close();
					 ************/
				}
			}
			catch(Exception e){
				e.printStackTrace();

			}
			System.out.print("\nNumber of proteins Lost in HMMs at 100 threshold: " + Prots.size()+"\n");
		}
		try{


			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("" +
					"select distinct sequenceid from tms_cores");
			HashMap <Integer, Integer> ProtsTMS_Core = new HashMap<Integer, Integer>();// list of proteins lost as no cluster made in last split round 100 thresh
			ResultSet rs = pstm1.executeQuery();
			while(rs.next()){
				int x  = rs.getInt(1);
				if(!ProtsTMS_Core.containsKey(x)){
					ProtsTMS_Core.put(x, null);
				}
			}
			rs.close();
			pstm1.close();

			int protsIntms = 0;
			int protsNotInHMM = 0;
			int j =0;
			CalcStats.main(args);
			for (Integer key : Prots.keySet()) {
				j ++;
				if(ProtsTMS_Core.containsKey(key)){
					protsIntms++;
				}
				if(!CalcStats.ProteinsInHmm.containsKey(key)){
					protsNotInHMM++;
				}

				if(j%100 == 0)
					System.out.print(j+"\n");
			}
			System.out.print("\nNumber of proteins Lost in HMMs and present in tms_cores: " + protsIntms+"\n");
			System.out.print("\nNumber of proteins Lost in HMMs and Not present in HMM models at any threshold: " + protsNotInHMM+"\n");





			CAMPS_CONNECTION.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	// gets the unique number of sequences from clusters_mcl;
	private static void test7() {
		// TODO Auto-generated method stub
		try{
			HashMap <Integer, Integer> Prots = new HashMap<Integer, Integer>();// list of proteins in clusters_mcl 
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("" +
					"SELECT sequenceid from clusters_mcl");
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				int s = rs1.getInt(1);
				if(!Prots.containsKey(s)){
					Prots.put(s,null);
				}
			}
			System.out.print("\n"+Prots.size()+"\n");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	// So this function would be used to test the loss of proteins from 
	// MCL clusters to TMS cores and HMMs
	// The count at MCL is 623819
	// The count at HMM is 275189
	// However, in the workflow from MCL to HMM an intermediate step is core extraction
	// The count at Cores is 311517
	// Therefore loss from MCL to Core is 623819 - 311517 = 312302
	// Of this 312302, <15 size cluster size member proteins is 250936
	// How many are coverage losses? Answer is the proteins which belong to >15 size clusters
	// and also are not included in tms_cores 
	private static void test6() {
		// TODO Auto-generated method stub
		// SELECT clusters_mcl.cluster_id from clusters_mcl_nr_info,clusters_mcl WHERE NOT clusters_mcl.cluster_id = clusters_mcl_nr_info.cluster_id AND clusters_mcl.cluster_threshold =5 AND clusters_mcl_nr_info.cluster_threshold=5 limit 10;
		try{
			int totalLoss = 0;
			HashMap <Integer, Integer> ProtsGreater15 = new HashMap<Integer, Integer>();// list of proteins lost mcl to tmsCores key is seqId and value is clusterId
			HashMap <Integer, Integer> ProtsLesser15 = new HashMap<Integer, Integer>();// list of proteins lost mcl to tmsCores key is seqId and value is clusterIdProtsLesser15
			boolean check = false;
			for(int j =0; j<= 100; j++){
				PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("" +
						"select cluster_id,sequenceid from clusters_mcl where cluster_threshold=?");
				pstm1.setInt(1, j);
				ResultSet rs1 = pstm1.executeQuery();
				ArrayList<Integer> cluses = new ArrayList<Integer>();
				HashMap <Integer, ArrayList<Integer>> list = new HashMap<Integer, ArrayList<Integer>>();// list of proteins with same query=hit

				while(rs1.next()){
					check = true;

					int clus = rs1.getInt(1);
					int sq = rs1.getInt(2);

					if(list.containsKey(clus)){
						ArrayList<Integer> x = list.get(clus);
						x.add(sq);
						list.put(clus, x);
					}
					else{
						ArrayList<Integer> x = new ArrayList<Integer>();
						x.add(sq);
						list.put(clus, x);
						cluses.add(clus);
					}

				}
				int count =0;
				int lostProtsUnderFifteen = 0;
				int lostProtsOverFifteen = 0;
				ArrayList<Integer> prots_lostBtwMCL_tms = new ArrayList<Integer>(); 

				for(int i =0;i<=cluses.size()-1;i++){
					int t = cluses.get(i);
					ArrayList<Integer> prots = list.get(t);
					if (prots.size()>=15){
						for(int k = 0;k<=prots.size()-1;k++){
							int temp = prots.get(k);
							if (!ProtsGreater15.containsKey(temp)){
								ProtsGreater15.put(temp, t);
							}
						}
						lostProtsOverFifteen = lostProtsOverFifteen + prots.size();

					}
					else if (prots.size()<15){
						for(int k = 0;k<=prots.size()-1;k++){
							int temp = prots.get(k);
							if (!ProtsLesser15.containsKey(temp)){
								ProtsLesser15.put(temp, t);
							}
						}
						lostProtsUnderFifteen = lostProtsUnderFifteen + prots.size();
						count++;
					}

				}
				if (check){
					check = false;

					System.out.print("\nNumber of clusters with proteins > 15 in clusterThresh: "+j+" number "+count+"\n");
					System.out.print("\nProteins lost within these > 15 in ClusterThresh size clusters: "+j+" number "+lostProtsUnderFifteen+"\n");
					totalLoss = totalLoss + lostProtsUnderFifteen;
				}
			}
			System.out.print("Total Prots in clusters_mcl in clusters > 15: "+ ProtsGreater15.size() +"\n");
			System.out.print("Total Prots in clusters_mcl in clusters < 15: "+ ProtsLesser15.size() +"\n");
			// here I have hash of proteins key and clusterIds as value of the clusters_mcl > 15

			// have to find proteins > 15 but not in tms cores

			HashMap <Integer, Integer> Prots_tmsCores = new HashMap<Integer, Integer>();// list of proteins in tmsCores key is seqId and value is null
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("" +
					"select distinct sequenceid from tms_cores");
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				int sq = rs1.getInt(1);
				Prots_tmsCores.put(sq, null);
			}
			// now have > 15 prots.. and have prots in tmsCores, So now can compare
			// and find prots > 15 clusters but not in tms cores

			// There can be same proteins in the greater15 and lesser15 sets, because
			// for example if one cluster at thresh 5 was greater15 it would have been inclueded in
			// greater15 set, however, increase in threshold caused splitting of cluster and at e.g. threshold 45
			// the cluster size becomes <15, then it would be included in Lesser15 set.
			// Therefore remove proteins from greater15 that are also in Lesser15
			for (Integer key : ProtsLesser15.keySet()) {
				if(ProtsGreater15.containsKey(key)){
					ProtsGreater15.remove(key);
				}
			}
			System.out.print("\nNewSize Total Prots in clusters_mcl in clusters > 15: "+ ProtsGreater15.size() +"\n");
			System.out.print("Total Prots in clusters_mcl in clusters < 15: "+ ProtsLesser15.size() +"\n");

			// iterating through the map key set
			int count_prots_notIntmsCoresGreater15 = 0;
			for (Integer key : ProtsGreater15.keySet()) {
				if(!Prots_tmsCores.containsKey(key)){
					count_prots_notIntmsCoresGreater15++;
				}
			}
			int count_prots_notIntmsCoresLesser15 = 0;
			for (Integer key : ProtsLesser15.keySet()) {
				if(!Prots_tmsCores.containsKey(key)){
					count_prots_notIntmsCoresLesser15++;
				}
			}
			System.out.print("\n\n The Number of Proteins which belong to >15 sized clusters \n");
			System.out.print(" but are not present in tmsCores are: "+count_prots_notIntmsCoresGreater15+"\n\n");

			System.out.print("\n\n The Number of Proteins which belong to <15 sized clusters \n");
			System.out.print(" and are not present in tmsCores are: "+count_prots_notIntmsCoresLesser15+"\n");
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

	// the function investigates the loss of protein 6,30 to 2,80 
	// from clusters_mcl to scClusters
	private static void test5(String[] args) {
		// TODO Auto-generated method stub
		// 1. get list of lost proteins
		// 2. no of proteins lost due to small cluster size
		// 3. what are the other factors?
		//F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/HMMs/CAMPS4_1
		/*
		 * Loss of proteins from clusters_mcl to HMMs
			over 600k to over 200k in HMMs
			Following are the reasons of lost proteins
			//minimal number of cluster members for core extraction
			private static final int MINIMUM_CLUSTER_SIZE = 15;

			//minimal percentage coverage of aligned sequences to form TMS core
			private static final double MIN_COVERAGE_ALIGNMENT = 35;

			//number of residues the core is extended at both sides to form TMS block
			private static final int CORE_EXTENSION = 7;

			private static final int MINIMUM_NUMBER_CORES = 1;

			//sequences longer than 2000 are ignored
			private static final int MAX_SEQUENCE_LENGTH = 2000;

			In CreatsCamps clusters for HMM, the information comes from the clusters_mcl_nr_info,
			which gets the information from tms_cores.... The tms_cores gets core information using the tms table
			TMS cores doest not get information from the alignments table. Only E-Value and bit score from alignments,
			the rest from TMS.


			The homogenity issue is dealt using MIN_COVERAGE_ALIGNMENT. This is calculated using:
			(count * 100) / ((double) candidates.size());
			where count is the number of aligned amino acids in the TM core region.
			This should be divided by number of proteins in this cluster (candidates.size).

			Now find, how many proteins are lost with 1. min_cluster_size and 2. Min_coverage_alignment
			So, from alignments 652562 .... now we have  543886 in tms_cores_info... as acc to restrictions
			example tms_range for each cluster - this is also a part of homogenity
			So, 452160 clusters - of which alot of overlaps are <15 size clusters.
			HMM - 280K sequences
			Total Lost Prots not repeating count: 250936 - this is the count of lost prots 
			from clusters_mcl to hmm only due to the <15 cluster size

		 */

		//CalcStats.main(args);
		//long ProtsSC = 0;
		//for (int i = 0; i<= CalcStats.Model_list.size()-1; i++){
		//	ProtsSC = ProtsSC + CalcStats.Model_list.get(i).getNumber_of_proteins();
		//}
		try{
			int totalLoss = 0;
			HashMap <Integer, Integer> lostProts = new HashMap<Integer, Integer>();// list of proteins with same query=hit
			boolean check = false;
			for(int j =0; j<= 100; j++){

				PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("" +
						"select cluster_id,sequenceid from clusters_mcl where cluster_threshold=?");
				pstm1.setInt(1, j);
				ResultSet rs1 = pstm1.executeQuery();
				ArrayList<Integer> cluses = new ArrayList<Integer>();
				HashMap <Integer, ArrayList<Integer>> list = new HashMap<Integer, ArrayList<Integer>>();// list of proteins with same query=hit

				while(rs1.next()){
					check = true;

					int clus = rs1.getInt(1);
					int sq = rs1.getInt(2);

					if(list.containsKey(clus)){
						ArrayList<Integer> x = list.get(clus);
						x.add(sq);
						list.put(clus, x);
					}
					else{
						ArrayList<Integer> x = new ArrayList<Integer>();
						list.put(clus, x);
						cluses.add(clus);
					}

				}
				int count =0;
				int lostProtsUnderFifteen = 0;
				ArrayList<Integer> prots_lostBtwMCL_tms = new ArrayList<Integer>(); 

				for(int i =0;i<=cluses.size()-1;i++){
					int t = cluses.get(i);
					ArrayList<Integer> prots = list.get(t);
					if (prots.size()<15){
						for(int k = 0;k<=prots.size()-1;k++){
							int temp = prots.get(k);
							if (!lostProts.containsKey(temp)){
								lostProts.put(temp, null);
							}
						}
						lostProtsUnderFifteen = lostProtsUnderFifteen + prots.size();
						count++;
					}
				}
				if (check){
					check = false;

					System.out.print("\nNumber of clusters with proteins < 15 in clusterThresh: "+j+" number "+count+"\n");
					System.out.print("\nProteins lost within these <15 in ClusterThresh size clusters: "+j+" number "+lostProtsUnderFifteen+"\n");
					totalLoss = totalLoss + lostProtsUnderFifteen;
				}
			}
			System.out.print("Total Lost Prots not repeating count: "+ lostProts.size() +"\n");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	// checks the hypothesis that about 30k proteins lost from alignments run to mcl clusters 
	// is because of same seqid query and seqid hit... some proteins dont have any other 
	// relatioship apart from themseleves
	private static void test4() {
		// TODO Auto-generated method stub
		try{
			HashMap <Integer, Integer> list = new HashMap<Integer, Integer>();// list of proteins with same query=hit
			int idx =1;
			int x = 0;
			for (int i=1; i<=17;i++){
				PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("select " +
						"seqid_query,seqid_hit from alignments_initial limit "+idx+","+10000000);
				idx = i*10000000;
				System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");

				ResultSet rs3 = pstm3.executeQuery();
				while (rs3.next()){
					int sqQ = rs3.getInt(1);
					int sqH = rs3.getInt(2);
					if (x %100000 == 0){
						System.out.print("Processed " + x +"\n");
					}
					if(sqQ == sqH){
						if (!list.containsKey(sqQ)){
							list.put(sqQ,null);
						}
					}
					x++;
				}
				rs3.close();
				pstm3.close();
			}

			// till now have retrieved  the same query and hit 
			// now find if they have any other relation
			// but for that
			System.out.print("\n\n List Size Before population: "+list.size()+"\n");
			idx =1;
			x = 0;
			for (int i=1; i<=17;i++){
				PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("select " +
						"seqid_query,seqid_hit from alignments_initial limit "+idx+","+10000000);
				idx = i*10000000;
				System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");

				ResultSet rs3 = pstm3.executeQuery();
				while (rs3.next()){
					int sqQ = rs3.getInt(1);
					int sqH = rs3.getInt(2);
					if (x %100000 == 0){
						System.out.print("Processed " + x +"\n");
					}
					if(sqQ != sqH){
						if (list.containsKey(sqQ)){
							list.remove(sqQ);
						}
						if (list.containsKey(sqH)){
							list.remove(sqH);
						}
					}
					x++;
				}
				rs3.close();
				pstm3.close();
			}
			System.out.print("\n\nNumber of proteins with only self scores in Alignment_initial: " + list.size() +"\n");


		}
		catch(Exception e){
			e.printStackTrace();
		}

	}


	//Reads the file of lost proteins between sequences2 and alignments_initial
	// the file was made by getprots
	// This function would 
	/*
	 * 	Step 2: how many of these are hypothetical proteins?
		Step 3: what is the average length of these sequences?
		Step 4: What is the average number of TM segments in these proteins?
	 */
	private static void statLostProts() {
		// TODO Auto-generated method stub
		BufferedReader br = null;
		try{
			//read file
			HashMap <Integer, String> lostProts = new HashMap<Integer, String>(); // key - seqid and val is statement
			ArrayList<Integer> ids = new ArrayList<Integer>();

			System.out.print("Reading File \n");
			br = new BufferedReader(new FileReader("F:/proteinsNotInAlignments.txt"));

			long totalLen = 0;
			int counthypo = 0;
			int les100 =0;

			// read file of lost proteins
			Alphabet alpha = AlphabetManager.alphabetForName("PROTEIN");
			SimpleNamespace ns = new SimpleNamespace("biojava");
			RichSequenceIterator iterator = RichSequence.IOTools.readFasta(br,
					alpha.getTokenization("token"), ns);

			while (iterator.hasNext()) {	//populate temp_file
				RichSequence rec = iterator.nextRichSequence();
				String acc = rec.getAccession();
				String seq = rec.seqString();
				ids.add(Integer.parseInt(acc));
				lostProts.put(Integer.parseInt(acc), seq);
				if(seq.length()<100){
					les100++;
				}
			}
			br.close();

			System.out.print("Protein less than 100: "+les100+" \n");

			// get description and hypthetical
			int total = 0;
			long tm1 = 0;
			long tm2 = 0;
			long tm3 = 0;

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid, description from proteins2 group by sequenceid");
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				int id = rs.getInt(1);
				String desc = rs.getString(2);
				if(lostProts.containsKey(id)){
					total++;
					if (desc.contains("hypothetical protein")){
						counthypo ++;
					}
					totalLen = totalLen + lostProts.get(id).length(); 
				}
			}
			pstm.close();
			rs.close();

			System.out.print("Getting avg TMS size \n");
			// get tms number average
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("" +
					"select sequenceid,tms_id from tms group by sequenceid,tms_id");
			ResultSet rs1 = pstm1.executeQuery();
			int sqLast = 0;
			int tNow = 0;
			int tLast = 0;

			int tms = 0;
			int count =0;

			boolean first = true;
			int x =0;
			while(rs1.next()){
				x++;
				if (x%1000 == 0){
					System.out.print("Processed " + x +"\n");
				}
				int sqNow = rs1.getInt(1);
				tNow = rs1.getInt(2);

				if(sqNow != sqLast && !first){
					if(lostProts.containsKey(sqLast)){
						count++;
						tms = tms + tLast;
						if(tLast==1){
							tm1 = tm1 + 1;
						}
						else if(tLast==2){
							tm2 = tm2 + 1;
						}
						else if(tLast==3){
							tm3 = tm3 + 1;
						}

					}
					sqLast = sqNow;

				}
				tLast = tNow;
				// the set of tms for this protein is complete
				// add sqlast and the refresh all
				if (first){
					sqLast = sqNow;
					tLast = tNow;
					first = false;
				}
			}
			pstm1.close();
			rs1.close();

			float avglen = totalLen/ids.size();
			System.out.print("Distinct TMS Sequences Count "+count+"\n");
			System.out.print("Total Number of Lost prots "+total+" and by id "+ids.size()+"\n");

			System.out.print("Number of Hypothetical Proteins or without any data "+counthypo+"\n");
			System.out.print("Average Length of Lost Prots "+avglen+"\n");
			System.out.print("Average Number of TM in Lost prots "+tms/ids.size()+"\n");

			System.out.print("Lost prots of TM 1: "+tm1+"\n");
			System.out.print("Lost prots of TM 2: "+tm2+"\n");
			System.out.print("Lost prots of TM 3: "+tm3+"\n");

			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}


	// function to get proteins which are in sequences but not in alignments
	private static void getprots() {
		// TODO Auto-generated method stub
		try{
			BufferedWriter wr = null;
			wr = new BufferedWriter(new FileWriter("F:/proteinsNotInAlignments.txt"));

			// get from sequences 2
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid from sequences2");
			ResultSet rs3 = pstm.executeQuery();

			ArrayList<Integer> ids = new ArrayList<Integer>();
			while(rs3.next()){
				int id = rs3.getInt(1);
				ids.add(id);
			}
			rs3.close();
			pstm.close();

			// get from alignments 2
			HashMap <Integer, Integer> seqsAlign = new HashMap<Integer, Integer>(); // key - seqid and val is statement
			int idx =1;

			for (int i=1; i<=17;i++){
				PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("select " +
						"seqid_query,seqid_hit from alignments_initial limit "+idx+","+10000000);
				idx = i*10000000;
				//System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
				int setsize = 0;
				ResultSet rs = pstm4.executeQuery();
				while (rs.next()){
					setsize++;
					int q = rs.getInt(1);
					int h = rs.getInt(2);
					if (!seqsAlign.containsKey(q)){
						seqsAlign.put(q, null);
					}
					if (!seqsAlign.containsKey(h)){
						seqsAlign.put(h, null);
					}
				}
				pstm4.close();
				rs.close();
				System.out.print("\n Hashtable of Alignments "+idx+", the set size was:"+setsize+"\n");
			}

			// compare to get non present sequences
			ArrayList<Integer> idsToDeal = new ArrayList<Integer>();
			for (int i =0; i<=ids.size()-1;i++){
				if (!seqsAlign.containsKey(ids.get(i))){
					idsToDeal.add(ids.get(i));
				}
			}
			ids = null;
			seqsAlign = null;

			System.out.print("\nComplete\n");
			PreparedStatement pstm1 = null;
			ResultSet rs1 = null;
			for(int i =0;i<=idsToDeal.size()-1;i++){
				Integer id = idsToDeal.get(i);
				pstm1 = CAMPS_CONNECTION.prepareStatement("select sequence from sequences2 where sequenceid=?");
				pstm1.setInt(1, id);
				rs1 = pstm1.executeQuery();
				String s = "";
				while(rs1.next()){
					s = rs1.getString(1);
				}
				wr.write(">"+id.toString());
				wr.newLine();
				wr.write(s);
				wr.newLine();
			}
			rs1.close();
			pstm1.close();

			wr.close();
			CAMPS_CONNECTION.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void fileCutter() {
		// TODO Auto-generated method stub
		/*
		 * 2878194297 - relationships in alignment file
		 * 
		 * processed 131580287
		 * 131580287
		 * remaining lines 2746614010
		 * 44300250 * 62
		 * 44300225
		 * 
		 *  * to find new split
		 * 30,00,000 * x = 2746614010
		 * x = 2746614010/30,00,000 = 916 files
		 * 
		 * 
		 * 
		 */
		BufferedReader br = null;
		BufferedWriter wr = null;
		int fileNUmebr = 1;
		try{
			br = new BufferedReader(new FileReader("/scratch/usman/download/camps_seq_file.matrix"));
			//br = new BufferedReader(new FileReader("I:/CAMPS_Similarity_Scores/camps_seq_file.matrix"));
			//I:\CAMPS_Similarity_Scores
			wr = new BufferedWriter(new FileWriter("/scratch/usman/download/parts/camps_seq_file"+fileNUmebr+".matrix"));

			String sCurrentLine = "";
			long i =0;
			int fcounter = 0;
			while ((sCurrentLine = br.readLine()) != null ) {
				i++;

				if(i > 131580287){
					if (fcounter < 3000000){
						wr.write(sCurrentLine);
						wr.newLine();
						fcounter++;

					}
					else{
						fcounter = 0;
						wr.close();

						fileNUmebr++;
						wr = new BufferedWriter(new FileWriter("/scratch/usman/download/parts/camps_seq_file"+fileNUmebr+".matrix"));
						wr.write(sCurrentLine);
						wr.newLine();
						fcounter++;
					}
				}
			}
			System.out.print("\n"+i+" and files "+fileNUmebr+"\n");
			wr.close();
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	//checks the distinct number of sequences from the similarity matrix file
	private static void test2() {
		// TODO Auto-generated method stub
		BufferedReader br = null;
		try{
			HashMap <Integer, Integer> seqs = new HashMap<Integer, Integer>(); // key - seqid and val is statement

			br = new BufferedReader(new FileReader("/scratch/usman/download/camps_seq_file.matrix"));
			String sCurrentLine = "";
			int i =0;
			while ((sCurrentLine = br.readLine()) != null ) {
				String q2[] = sCurrentLine.split("\t");

				int q = Integer.parseInt(q2[0]);
				int h = Integer.parseInt(q2[1]);
				if (!seqs.containsKey(q)){
					seqs.put(q, null);
				}
				if (!seqs.containsKey(h)){
					seqs.put(h, null);
				}
				if (i%100000 == 0){
					System.out.print("\n Hashtable of File In Process "+i+"\n");
				}
				i++;
			}
			br.close();
			System.out.print("number of unique sequences in SimilarityMatrix\n" +
					"irrespective of the query or hit... so this is the\n" +
					"number of combinations\n"+ seqs.size()+"\n");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	// checks the distinct number of sequences in alignments_initial
	private static void test() {
		// TODO Auto-generated method stub
		try{
			int idx =1;
			HashMap <Integer, Integer> seqs = new HashMap<Integer, Integer>(); // key - seqid and val is statement
			for (int i=1; i<=17;i++){

				PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("select " +
						"seqid_query,seqid_hit from alignments_initial limit "+idx+","+10000000);
				idx = i*10000000;
				//System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
				int setsize = 0;
				ResultSet rs3 = pstm3.executeQuery();
				while (rs3.next()){
					setsize++;
					int q = rs3.getInt(1);
					int h = rs3.getInt(2);
					if (!seqs.containsKey(q)){
						seqs.put(q, null);
					}
					if (!seqs.containsKey(h)){
						seqs.put(h, null);
					}
				}
				System.out.print("\n Hashtable of Alignments "+idx+", the set size was:"+setsize+"\n");
			}

			//*******
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("" +
					"select sequenceid,tms_id from tms group by sequenceid,tms_id");
			ResultSet rs1 = pstm1.executeQuery();
			int sqLast = 0;
			int tNow = 0;
			int tLast = 0;

			int tms = 0;
			int count =0;
			int tm1 = 0;
			int tm2 = 0;
			int tm3 = 0;

			boolean first = true;
			int x =0;
			while(rs1.next()){
				x++;
				if (x%1000 == 0){
					System.out.print("Processed " + x +"\n");
				}
				int sqNow = rs1.getInt(1);
				tNow = rs1.getInt(2);

				if(sqNow != sqLast && !first){
					if(seqs.containsKey(sqLast)){
						count++;
						tms = tms + tLast;
						if(tLast==1){
							tm1 = tm1 + 1;
						}
						else if(tLast==2){
							tm2 = tm2 + 1;
						}
						else if(tLast==3){
							tm3 = tm3 + 1;
						}

					}
					sqLast = sqNow;

				}
				tLast = tNow;
				// the set of tms for this protein is complete
				// add sqlast and the refresh all
				if (first){
					sqLast = sqNow;
					tLast = tNow;
					first = false;
				}
			}
			pstm1.close();
			rs1.close();
			// ****************
			System.out.print("number of unique sequences in alignments2 table\n" +
					"irrespective of the query or hit... so this is the\n" +
					"number of combinations\n"+ seqs.size());
			System.out.print("Proteins with TM1: "+ tm1 + "\n");
			System.out.print("Proteins with TM2: "+ tm2 + "\n");
			System.out.print("Proteins with TM3: "+ tm3 + "\n");

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void test3() {
		// TODO Auto-generated method stub
		try{

			for (int i=1; i<=5;i++){

				PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
						"INSERT INTO proteins " +
								"(sequenceid,name,description,databaseid,taxonomyid) VALUES " +
						"(?,?,?,?,?)");

				pstm1.setInt(1, i);
				pstm1.setString(2, "test");
				pstm1.setString(3, "test");
				pstm1.setInt(4, 0);
				pstm1.setInt(5, 0);

				int p = pstm1.executeUpdate();
				if(i>0){
					System.out.print("\nSuccess "+i +" "+p+"\n");
				}
				else {
					System.out.print("\nFailed "+i +" "+p+"\n");
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static ArrayList<Integer> algnCheck2(int TmNoToChose){
		ArrayList<Integer> sequenceIds = new ArrayList<Integer>(); // 1tm seqids
		try{
			int x = 0;
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("" +
					"select sequenceid,tms_id from tms group by sequenceid,tms_id");
			ResultSet rs1 = pstm1.executeQuery();
			int sqLast = 0;
			ArrayList<Integer> tids = new ArrayList<Integer>();
			boolean first = true;
			while(rs1.next()){
				x++;
				if (x%1000 == 0){
					System.out.print("Processed " + x +"\n");
				}
				int sqNow = rs1.getInt(1);

				if (first){
					sqLast = sqNow;
					first = false;
				}

				if (sqNow != sqLast){
					// the set of tms for this protein is complete
					// add sqlast and the refresh all
					if (tids.size() == TmNoToChose){
						sequenceIds.add(sqLast);
					}
					sqLast = sqNow;
					tids = new ArrayList<Integer>();
				}
				int tid = rs1.getInt(2);
				tids.add(tid);
			}
			pstm1.close();
			rs1.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return sequenceIds;
	}

	private static void algnCheck(ArrayList<Integer> sequenceIds) {
		// TODO Auto-generated method stub
		try {
			System.out.print("One tm sequences "+sequenceIds.size() + "\n");

			// pstm3 to see if the sequence is in alignments or not... and if present check its core count
			HashMap <Integer, Integer> alignmentTable = new HashMap<Integer, Integer>(); // key - seqid and val is statement
			// ^^^has the 1 tm sequences with 
			int idx =1;
			int x = 0;
			for (int i=1; i<=17;i++){

				PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("select " +
						"seqid_hit,covered_tms_query from alignments_initial limit "+idx+","+10000000);
				idx = i*10000000;
				System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");

				ResultSet rs3 = pstm3.executeQuery();

				while (rs3.next()){
					int sq = rs3.getInt(1);
					if (x %100000 == 0){
						System.out.print("Processed " + x +"\n");
					}
					if (sequenceIds.contains(sq)){
						int t = rs3.getInt(2);
						//alignmentTable.put(sq, t);
						if (!alignmentTable.containsKey(sq)){
							alignmentTable.put(sq, t);
						}
					}
					x++;
				}
				rs3.close();
				pstm3.close();
			}
			System.out.print("Results: 1TM proteins in alignment table with no of core\n\n\n");
			for (int i =0; i<=sequenceIds.size()-1;i++){
				if (alignmentTable.containsKey(sequenceIds.get(i))){
					int sq = sequenceIds.get(i);
					int t = alignmentTable.get(sequenceIds.get(i));

					System.out.print("SequenceId "+sq + "\t"+t+"\n");
				}
			}

		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

}
