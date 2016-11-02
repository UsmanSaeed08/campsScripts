package stats;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

public class NegativCasesInCathCamp_commonCases2 {

	/**
	 * @param args
	 * 
	 * The class is used to investigate the negative cases in the CATH CAMP comparison
	 * The definition of agreement used is.. If the cluster has more than one sequences, each 
	 * with a cath fold assignment. Then this cath fold should be represented in all the sequences
	 * with a cath fold assignment. 
	 * This definition shows 35 clusters with disagreements of the total 251
	 * In order to explain these disagreement this class is being made. 
	 * 
	 * Possibiities:
	 * 
	 * 1. Either the fold classification is just wrong - we can only say that if there are atleast
	 * 50% sequences with each with different classification while the identity threshold of their
	 * allignment is over atleast 60%
	 * 
	 * 2. False positives
	 * false positives may be designated as false positives if the given cath fold has respective 
	 * pdb identity of <60%. The idea is, lower the identity of pdb mapping to camps sequence, the 
	 * more chances are that assigned fold is dubious. This is because CATH takes into account complete
	 * structure while assigning them to particular folds
	 * 
	 * To Do:
	 * 1. Have to read the negative cases file.. 
	 * For each cluster
	 * - get the cath fold in maximum number of cases (can be single fold or two folds)
	 * Now: for the sequences which do not have this fold present in max sequences (responsible for rejections sequences)
	 * get the the identity of pdb sequences - and count identity below 60 70 or 90
	 * finally we can say at what identity level, these pdbs are discarded.    
	 *  
	 */
	private static HashMap<String,String> seqid2pdbs = new HashMap<String,String>(); // is populated for each cluster in getmaxFold
	private static ArrayList<String> seqidsKeys = new ArrayList<String>();
	private static HashMap<String,String> seqid2fold = new HashMap<String,String>(); // is populated for each cluster in getmaxFold
	
	private static HashMap<String,Integer> Folds2Seqcount = new HashMap<String,Integer>(); // foldkey and value is number of sequences its present in
	private static ArrayList<String> keyFolds = new ArrayList<String>(); // keys for above hashmap
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// for each cluster in disagreement.. get the percent identities of the folds present
		System.out.println("Running...");
		String negtvTblFile = "F:/Scratch/reRunCAMPS_CATH_PDB/foldLevel/negativeCaseTable_cath_30Identity_Camps.txt";
		String mapF = "F:/Scratch/reRunCAMPS_CATH_PDB/reRUNCAMPSSeqToPDBTMOnly.txt";
		ArrayList<String> clusIdsNegativ = getNegativeClusIds(negtvTblFile);
		Process(clusIdsNegativ,negtvTblFile,mapF);
		
	}

	private static void Process(ArrayList<String> clusIdsNegativ,
			String negtvTblFile,String mapFile) {
		// TODO Auto-generated method stub
		try{
			for(int i =0;i<=clusIdsNegativ.size()-1;i++){
				String id = clusIdsNegativ.get(i);
				String maxFold = getmaxFold(id,negtvTblFile); // gives the fold present in max cases, but also populates the hashmaps
				getIdentities(id,mapFile,maxFold);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void getIdentities(String id, String mapFile,String maxFold) {
		// TODO Auto-generated method stub
		try{
			// for this cluster id - and the given sequences, their respective pdb and fold - get identitites
			BufferedReader br = new BufferedReader(new FileReader(new File(mapFile)));
			String l = "";
			int count = 0;
			while((l=br.readLine())!=null){
				if (!l.startsWith("#")){
					String p[] =l.split("\t");
					if(seqid2pdbs.containsKey(p[0])){
						String pdb = seqid2pdbs.get(p[0]);
						if(pdb.contains(p[1])){
							// respective seqid and its pdb hit found
							float identity =  Float.parseFloat(p[2]);
							if(identity>90){
								count++;
								// check if it is MaxFold or not
								String fo = seqid2fold.get(p[0]);
								if(!fo.equals(maxFold)){ // if this sequence id had a max fold or not - report
									System.err.print("Fold diagreement at at identity 90 Report\n"+p[0]+"\n"+fo+"\n");
								}
							}
						}
					}
				}
			}
			br.close();
			if(count>0){
				System.out.println();
				System.out.println(id+" -- Negative Cases with over 90 identity = "+count);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static String getmaxFold(String id, String negtvTblFile) { // returns the fold present in max of the sequences in this cluster
		// TODO Auto-generated method stub
		try{
			String fold = "xxx";
			HashMap<String,Integer> Folds2count = new HashMap<String,Integer>();
			ArrayList<String> keys = new ArrayList<String>();
			
			seqid2pdbs = new HashMap<String,String>();
			seqidsKeys = new ArrayList<String>();
			seqid2fold = new HashMap<String,String>();
			
			BufferedReader br = new BufferedReader(new FileReader(new File(negtvTblFile)));
			//100.0_19942	4262331	3tuj_A#3tuz_A#3dhw_A#3tui_B#3tuj_B#3tuz_B#3dhw_B#3tui_A	1.10.3720
			String l = "";
			while((l =br.readLine())!=null){
				if(!l.startsWith("#")){
					String parts[] = l.split("\t");
					if(parts[0].equals(id)){ // this clus id
						seqid2pdbs.put(parts[1].trim(), parts[2].trim());
						seqidsKeys.add(parts[1].trim());
						String fo = parts[3].trim();
						if(Folds2count.containsKey(fo)){
							int x = Folds2count.get(fo);
							x = x+1;
							Folds2count.put(fo, x);
						}
						else{
							Folds2count.put(fo, 1);
							keys.add(fo);
						}
						//populate seqid2fold
						if(seqid2fold.containsKey(parts[1].trim())){
							System.err.println("Unexpected Case..Exit");
							System.exit(0);
						}
						else{
							seqid2fold.put(parts[1].trim(),fo);
						}
					}
				}
			}
			br.close();
			// get max
			int max = 0;
			int total = 0;
			for(int i =0;i<=keys.size()-1;i++){
				String k = keys.get(i);
				int c = Folds2count.get(k); 
				if(c>max){
					fold = k;
					max = c;
				}
				total = total +c;
				//System.out.println(k+" -- "+c);
			}
			Folds2Seqcount = Folds2count;
			keyFolds = keys;
			
			System.out.println("For ClusIds "+id+" ,Max number of sequences: "+ max + " of the total "+total+ " have exactly"+fold+" fold");
			return fold;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	private static ArrayList<String> getNegativeClusIds(String negtvTblFile) {
		// TODO Auto-generated method stub
		try{
			ArrayList<String> ids = new ArrayList<String>();
			BufferedReader br = new BufferedReader(new FileReader(new File(negtvTblFile)));
			String l = "";
			while((l =br.readLine())!=null){
				if(!l.startsWith("#")){
					l = l.split("\t")[0];
					if(!ids.contains(l)){
						ids.add(l);
					}
				}
			}
			br.close();
			System.out.println("Retreived number of clusters with no agreement: "+ ids.size());
			return ids;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

}
