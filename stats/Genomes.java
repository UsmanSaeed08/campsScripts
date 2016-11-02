package stats;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

import utils.DBAdaptor;

public class Genomes {

	/**
	 * @param args
	 */
	// for clusters superkingdom
		private static int A = 0;
		private static int B = 0;
		private static int V = 0;
		private static int E = 0;
		
		private static int AB = 0;
		private static int AV = 0;
		private static int AE = 0;
		
		private static int BV = 0;
		private static int BE = 0;
		private static int VE = 0;
		
		private static int ABV = 0;
		private static int AEB = 0;
		private static int AVE = 0;
		private static int BVE = 0;
		
		private static int ABVE = 0;
		

	// finds the genome distribution --- how many clusters are required to get x number of genomes

	//key - thresh_clusterId ..... value - cluster_members = sequenceID
	// e.g. 5.0_30
	private static HashMap <String, ArrayList<Integer>> SC = new HashMap<String,ArrayList<Integer>>();
	// key sequence id and value is taxonomy
	private static HashMap <Integer, Integer> taxids = new HashMap<Integer,Integer>();
	// key - taxid ...... val - superKingdom of this taxId
	private static HashMap <Integer, String> taxidKingdom = new HashMap<Integer,String>();

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");

	private static ArrayList<String> sc_keys = new ArrayList<String>();
	private static ArrayList<Integer> sc_keys_sizes = new ArrayList<Integer>();

	private static ArrayList<String> files = new ArrayList<String>();

	private static ArrayList<Integer> totalTaxas = new ArrayList<Integer>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//System.out.print("H");
		System.out.print("Getting files" + "\n");
		//getfiles("F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/HMMs/CAMPS4_1/"); // old
		getfiles("F:/SC_Clust_postHmm/MetaModelsJune2016/HMMs/CAMPS4_1/"); // new 
		System.out.print("Populating SC" + "\n");
		PopulategetSC();
		System.out.print("getting taxas" + "\n");
		getTaxas();
		//System.out.print("Getting and arranging to size" + "\n");
		//getSizes();
		//arrangeAccToSizes();
		//System.out.print("Compute" + "\n");
		//compute();
		System.out.print("getting Kingdoms" + "\n");
		getkingdoms();
		System.out.print("Calculating Stats" + "\n");
		getGenomes();
		forWenDiag();
		
		printSuperkingdoms();
	}
	private static void getGenomes() {
		// TODO Auto-generated method stub
		try{
			ArrayList<Integer> archGenom = new ArrayList<Integer>();
			ArrayList<Integer> bacGenom = new ArrayList<Integer>();
			ArrayList<Integer> virGenom = new ArrayList<Integer>();
			ArrayList<Integer> euGenom = new ArrayList<Integer>();
			
			PreparedStatement pstmx = CAMPS_CONNECTION.prepareStatement("" +
					"select taxonomyid from proteins2");
			ResultSet rsx = pstmx.executeQuery();
			while(rsx.next()){
				int txid = rsx.getInt("taxonomyid");
				String kingdom = taxidKingdom.get(txid);
				if (kingdom!=null){
				if (kingdom.contains("Archaea")){
					if(!archGenom.contains(txid))
					archGenom.add(txid);
				}
				else if (kingdom.contains("Bacteria")){					
					if(!bacGenom.contains(txid))
						bacGenom.add(txid);
				}
				else if (kingdom.contains("Viruses")){
					if(!virGenom.contains(txid))
						virGenom.add(txid);
				}
				else if (kingdom.contains("Eukaryota")){
					if(!euGenom.contains(txid))
						euGenom.add(txid);
				}
				else 
				{
					//System.err.print("\n"+txid + " NOT Super kingdom");
				//	System.exit(0);
				}
			}
			}
			rsx.close();
			pstmx.close();
			System.out.print("Genomes Arch: " + archGenom.size()+"\n");
			System.out.print("Genomes Bacteria: " + bacGenom.size()+"\n");
			System.out.print("Genomes Virus: " + virGenom.size()+"\n");
			System.out.print("Genomes Eukar: " + euGenom.size()+"\n\n");
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		
	}
	private static void printSuperkingdoms(){
		System.out.print("Only Archea "+A+"\n");
		System.out.print("Only Bacteria "+B+"\n");
		System.out.print("Only Viral "+V+"\n");
		System.out.print("Only Eukaryotes "+E+"\n\n");
		
		System.out.print("Archea & Bacteria "+AB+"\n");
		System.out.print("Archea & Virus "+AV+"\n");
		System.out.print("Archea & Eukaryote "+AE+"\n");
		System.out.print("Bacteria & Virus "+BV+"\n");
		System.out.print("Bacteria & Eukaryote"+BE+"\n");
		System.out.print("Virus & Eukaryote "+VE+"\n\n");
			
		
		System.out.print("Archea Bacteria & Virus "+ABV+"\n");
		System.out.print("Archea Eukaryote & Bacteria "+AEB+"\n");
		System.out.print("Archea Virus & Eukaryote "+AVE+"\n");
		System.out.print("Bacteria Virus & Eukaryote "+BVE+"\n\n");
		
		System.out.print("Archea Bacteria Eukaryote & Virus "+ABVE+"\n");
	}
	
	private static void forWenDiag() {
		// TODO Auto-generated method stub
		// for total number of proteins
		int a = 0;	// archae 
		int b = 0;	// bacteria
		int v = 0;	// virus
		int e = 0;	// eukaryotes
		
	
		
		// for all the clusters get number of superkingdoms separately
		for (int i =0; i<=sc_keys.size()-1;i++){
			String k = sc_keys.get(i);
			ArrayList<Integer> ids = SC.get(k);

			boolean abool =  false;
			boolean bbool =  false;
			boolean vbool =  false;
			boolean ebool =  false;

			for(int j =0; j<=ids.size()-1;j++){
				int id = ids.get(j);
				// have the seqid now get tax id
				int txid = taxids.get(id);
				// have taxid no get kingdom
				String kingdom = taxidKingdom.get(txid);
				if (kingdom.contains("Archaea")){
					a++;
					abool = true;
					
				}
				else if (kingdom.contains("Bacteria")){
					b++;
					bbool = true;
					
				}
				else if (kingdom.contains("Viruses")){
					v++;
					vbool = true;
					
				}
				else if (kingdom.contains("Eukaryota")){
					e++;
					ebool = true;
					
				}
				else 
				{
					
					System.err.print("\n"+txid + " NOT Super kingdom");
					System.exit(0);
				}

			}
			// now that we know which sequences are there in cluster
			// so get the stats of sequences
			if (abool == true && bbool == false && vbool == false && ebool == false){
				A++;
			}
			else if (abool == false && bbool == true && vbool == false && ebool == false){
				B++;
			}
			else if (abool == false && bbool == false && vbool == true && ebool == false){
				V++;
			}
			else if (abool == false && bbool == false && vbool == false && ebool == true){
				E++;
			}
			
			// ab av ae bv be ve
			if (abool == true && bbool == true && vbool == false && ebool == false)
				AB++;
			if (abool == true && bbool == false && vbool == true && ebool == false)
				AV++;
			if (abool == true && bbool == false && vbool == false && ebool == true)
				AE++;
			if (abool == false && bbool == true && vbool == true && ebool == false)
				BV++;
			if (abool == false && bbool == true && vbool == false && ebool == true)
				BE++;
			if (abool == false && bbool == false && vbool == true && ebool == true)
				VE++;
			
			
			// ABV AEB AVE BVE
			if (abool == true && bbool == true && vbool == true && ebool == false)
				ABV++;
			if (abool == true && ebool == true && bbool == true && vbool == false)
				AEB++;
			if (abool == true && vbool == true && ebool == true && bbool == false)
				AVE++;
			if (bbool == true && vbool == true && ebool == true && abool == false)
				BVE++;
			
			if (abool == true && bbool == true && vbool == true && ebool == true)
				ABVE++;
		}
		
		
		System.out.print("Archea Proteins in total are "+ a +"\n");
		System.out.print("Bacteria Proteins in total are "+ b +"\n");
		System.out.print("Viral Proteins in total are "+ v +"\n");
		System.out.print("Eukaryotes Proteins in total are "+ e +"\n");
		
		
	}
	private static void getkingdoms() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("" +
					"select taxonomyid,superkingdom from taxonomies_merged");
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				int tx = rs.getInt("taxonomyid");
				String kg = rs.getString("superkingdom");
				taxidKingdom.put(tx,kg);
			}
			System.out.print("TaxKingdoms retreived " +"\n");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void arrangeAccToSizes() {
		// TODO Auto-generated method stub
		boolean swapped = true;
		int j = 0;
		int tmp;
		String temp2;
		while (swapped) {
			swapped = false;
			j++;
			for (int i = 0; i < sc_keys_sizes.size() - j; i++) {                                       
				if (sc_keys_sizes.get(i) > sc_keys_sizes.get(i+1)) {                          
					tmp = sc_keys_sizes.get(i);
					temp2 = sc_keys.get(i);

					sc_keys_sizes.set(i, sc_keys_sizes.get(i+1));
					sc_keys.set(i, sc_keys.get(i+1));

					sc_keys_sizes.set(i+1, tmp);
					sc_keys.set(i+1, temp2);

					swapped = true;
				}
			}                
		}

	}
	private static void getSizes() {
		// TODO Auto-generated method stub
		for (int i =0;i<=sc_keys.size()-1;i++){
			sc_keys_sizes.add(SC.get(sc_keys.get(i)).size());
		}

	}
	private static void compute() {
		// TODO Auto-generated method stub
		int genomes = 0;
		ArrayList<Integer> tax_temp = new ArrayList<Integer>();
		ArrayList<Integer> tax_NotinSC = new ArrayList<Integer>();
		for(int i =0; i<=sc_keys.size()-1;i++){
			String key = sc_keys.get(i);
			ArrayList<Integer> members = SC.get(key);

			for (int j =0;j<=members.size()-1;j++){
				int idp = members.get(j);
				int idt = taxids.get(idp);
				if(!tax_temp.contains(idt)){
					tax_temp.add(idt);
					genomes++;
				}
			}
			if (i % 50 ==0){
				System.out.print(i+"\t"+genomes+"\n");
			}

		}
		System.out.print(sc_keys.size()+"\t"+genomes+"\n");
		System.out.print("Number of total taxas is "+totalTaxas.size()+"\n");
		// to check non representing taxas
		for(int i =0 ; i<=tax_temp.size()-1;i++){
			if (!totalTaxas.contains(tax_temp.get(i))){
				tax_NotinSC.add(tax_temp.get(i));
			}
		}
		System.out.print("Non represented taxas "+tax_NotinSC.size()+"\n");

	}
	private static void getTaxas() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select distinct sequenceid,taxonomyid from proteins2");
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				int sq = rs.getInt("sequenceid");
				int tx = rs.getInt("taxonomyid");
				taxids.put(sq, tx);
				if (!totalTaxas.contains(tx)){
					totalTaxas.add(tx);
				}
			}
			System.out.print("Taxids retreived for sequences " + taxids.size()+"\n");
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
	private static void PopulategetSC() {
		// TODO Auto-generated method stub
		try{
			//PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl where cluster_threshold = ? and cluster_id = ?");
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

}
