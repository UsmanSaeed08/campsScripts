package wokflow2;

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
import java.util.Hashtable;

import mips.gsf.de.simapclient.client.SimapAccessWebService;
import mips.gsf.de.simapclient.datatypes.HitProtein;
import mips.gsf.de.simapclient.datatypes.HitSet;
import mips.gsf.de.simapclient.datatypes.ResultParser;

import utils.DBAdaptor;

public class MappingCampsCATH extends Thread{

	/**
	 * @param args
	 */
	MappingCampsCATH(){

	}
	MappingCampsCATH(String q, int i){
		// constructor - initialize
		this.threadStatus = true;
		this.queryToRun = q;
		this.seqIdToRun = i;
	}
	private static HashMap <String, ArrayList<Integer>> SC = new HashMap<String,ArrayList<Integer>>();
	private static HashMap <Integer, String> SC_seqToCode = new HashMap<Integer,String>();

	private static boolean tocheck;

	public static HashMap <Integer, String> SeqIdToSeq = new HashMap<Integer,String>();
	private static HashMap <Integer, Integer> SC_seqIds_hash = new HashMap<Integer,Integer>();
	public static ArrayList<Integer> SC_seqIds = new ArrayList<Integer>();
	private static ArrayList<String> sc_keys = new ArrayList<String>();

	private static HashMap <Integer, Integer> AlreadyDoneSeqIds = new HashMap<Integer,Integer>();

	private static BufferedWriter allseqs ;
	private static BufferedWriter processedseqs ;
	private static BufferedWriter Camps2PDBmap ;


	private static int SeqWithHitsCount;
	static int ActiveThreadCount=0;
	public boolean threadStatus;

	private String queryToRun;
	private int seqIdToRun;


	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	private static ArrayList<String> files = new ArrayList<String>();
/*
	public static void Initialize(String inputFilesPath, String allseq, String processedSeqFile, String mapFile,String backupProcessedSequenceFile,boolean checkAlready) {
		// TODO Auto-generated method stub
		try{
			System.out.print("Here we go!\n");
			SeqWithHitsCount = 0;
			// to map to cath
			//Phase 1
			// 1. get all sc clust seq in CAMPS -- X
			// 2. save them cluster wise - hashtable clusid key and arraylist of protein members -- X
			// 3. for each cluster , and each protein in the cluster - run simap (BLAST) against pdb hits - min identity 30%
			// 4. save the hits in hashmap and write to file for later use
			//getfiles("F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/HMMs/CAMPS4_1/");

			if(checkAlready){
				tocheck = checkAlready;
				//alreadyProcessedSeq = new BufferedReader(new FileReader(new File(backupProcessedSequenceFile)));
				getAlreadyDoneSeq(backupProcessedSequenceFile);
			}
			else{
				tocheck = false;
			}

			System.out.print("Fetching Files!\n");
			getfiles(inputFilesPath);
			System.out.print("Fetched Files "+files.size() +"\n");

			System.out.print("Fetching SC clusters\n");
			getAllSC();

			System.out.print("Populating Sequences \n");
			getAllSeq();
			System.out.print("Writing all sequences\n");
			///home/users/saeed/scratch/mappingCampsToCATHandPDB
			/*
			allseqs = new BufferedWriter(new FileWriter(new File("F:/Scratch/mappingCampsToCATHandPDB/allSequeces.txt")));
			processedseqs = new BufferedWriter(new FileWriter(new File("F:/Scratch/mappingCampsToCATHandPDB/ProcessedSequeces.txt")));
			Camps2PDBmap = new BufferedWriter(new FileWriter(new File("F:/Scratch/mappingCampsToCATHandPDB/Camps2PDBmap.txt")));
			 */
			/*
			allseqs = new BufferedWriter(new FileWriter(new File(allseq)));
			processedseqs = new BufferedWriter(new FileWriter(new File(processedSeqFile)));
			Camps2PDBmap = new BufferedWriter(new FileWriter(new File(mapFile)));

			WriteAllSequences(); // writes allseqs and closes

			System.out.print("Sequences which had atleast 1 hit: "+ SeqWithHitsCount+ "\n");
			// point 3 run simap

			// Phase 2
			// 1. for each identified pdb hit
			// put the identity, and eval pdbId and seqId in table
			// for the pdb id, get cath domain - and score if possible
			// put the mapped score in table
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	*/
	public static void Initialize(String mapFile, String seqtorun) {
		// TODO Auto-generated method stub
		try{
			System.out.print("Here we go!\n");
			SeqWithHitsCount = 0;
			// Mapping reaming enteries to Pdbtm
			System.out.print("Populating Sequences \n");
			getAllSeq(seqtorun);
			System.out.print("Getting SC clusters\n");
			SC_seqToCode = populateSequencetoSC();
			Camps2PDBmap = new BufferedWriter(new FileWriter(new File(mapFile)));
			// point 3 run simap
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}
	private static HashMap<Integer, String> populateSequencetoSC(){
		try{
			HashMap <Integer, String> SeqIdToSCCluster = new HashMap<Integer,String>();

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select cluster_id, cluster_threshold,code from cp_clusters2 where type=\"sc_cluster\"");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_id=? and cluster_threshold=?");
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				Integer clusid = rs.getInt(1);
				float thresh = rs.getFloat(2);
				String code = rs.getString(3);

				pstm2.setInt(1, clusid);
				pstm2.setFloat(2, thresh);
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()){
					int seqid = rs2.getInt(1);
					if (SeqIdToSeq.containsKey(seqid)){ // only if the sequence is in sc clusters
						if(!SeqIdToSCCluster.containsKey(seqid)){
							SeqIdToSCCluster.put(seqid, code);
						}
					}
				}
				rs2.close();
			}
			rs.close();
			return SeqIdToSCCluster; 
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	private static void getAlreadyDoneSeq(String fol) { // receives the folder of files with completed sequence
		// TODO Auto-generated method stub
		try{

			File folder = new File(fol);
			File[] listOfFiles = folder.listFiles();
			ArrayList<File> processedSeq = new ArrayList<File>();

			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					//System.out.println("File " + listOfFiles[i].getName());
					//cluster_5.0_0.hmm.serialized
					if (!listOfFiles[i].getName().endsWith(".serialized")){
						processedSeq.add(listOfFiles[i]);
						//System.out.println("File " + listOfFiles[i].getName());
					}
				} else if (listOfFiles[i].isDirectory()) {
					System.out.println("Directory " + listOfFiles[i].getName());
				}
			}

			for(int i =0;i<=processedSeq.size()-1;i++){
				String line;
				BufferedReader tempFile = new BufferedReader(new FileReader(processedSeq.get(i)));

				while((line = tempFile.readLine())!=null){
					line.trim();
					if(line!=""){
						int seqid = Integer.parseInt(line);
						AlreadyDoneSeqIds.put(seqid, 0);
					}
				}
				tempFile.close();
			}
			System.out.println("Recovered Sequences: "+AlreadyDoneSeqIds.size());

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void getAllSeq() {
		// TODO Auto-generated method stub
		try{
			//SeqIdToSeq
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid, sequence from sequences2");
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				int key = rs.getInt(1);
				String val = rs.getString(2);
				if (SC_seqIds_hash.containsKey(key)){// if it is in SC 
					if (!SeqIdToSeq.containsKey(key)){
						val = val.toUpperCase();
						SeqIdToSeq.put(key, val);
					}
				}
			}
			rs.close();
			pstm.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void getAllSeq(String seqtorun) {
		// TODO Auto-generated method stub
		try{
			// get seqId to run
			BufferedReader br = new BufferedReader(new FileReader(new File(seqtorun)));
			String line = "";
			while((line = br.readLine())!=null){
				line = line.trim();
				line = line.trim().split("\t")[1].trim();
				if(line != ""){
					Integer sqid = Integer.parseInt(line);
					if(!SC_seqIds_hash.containsKey(sqid)){
						SC_seqIds_hash.put(sqid, 0);
					}
				}
			}
			br.close();

			//SeqIdToSeq
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid, sequence from sequences2");
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				int key = rs.getInt(1);
				String val = rs.getString(2);
				if (SC_seqIds_hash.containsKey(key)){// if it is in SC 
					if (!SeqIdToSeq.containsKey(key)){
						val = val.toUpperCase();
						SeqIdToSeq.put(key, val);
						SC_seqIds.add(key);
					}
				}
			}
			rs.close();
			pstm.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void WriteAllSequences() {
		// TODO Auto-generated method stub
		try{
			for(int i =0;i<=SC_seqIds.size()-1;i++){
				String code = SC_seqToCode.get(SC_seqIds.get(i));
				allseqs.write(code + "\t" + SC_seqIds.get(i));
				allseqs.newLine();
			}
			allseqs.flush();
			allseqs.close();
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

	private static void getAllSC() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_threshold = ? and cluster_id = ?");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select code from cp_clusters2 where cluster_id = ? and cluster_threshold = ? and type=\"sc_cluster\"");

			for (int i =0; i <=files.size()-1;i++){
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
				// get the code;
				pstm2.setInt(1, Integer.parseInt(clusid));
				pstm2.setFloat(2, Float.parseFloat(thresh));
				ResultSet rs2 = pstm2.executeQuery();
				String code = "";
				while(rs2.next()){
					code = rs2.getString(1);
				}
				rs2.close();


				pstm.setFloat(1, Float.parseFloat(thresh));
				pstm.setInt(2, Integer.parseInt(clusid));
				ResultSet rs = pstm.executeQuery();
				ArrayList<Integer> sqids= new ArrayList<Integer>();
				while(rs.next()){
					int id = rs.getInt("sequenceid");
					sqids.add(id);
					//SC_seqIds.put(id, 0);
					if(tocheck){
						if(!AlreadyDoneSeqIds.containsKey(id)){ // if the sequence is not already processed
							SC_seqIds.add(id);
							SC_seqToCode.put(id, code);
							SC_seqIds_hash.put(id,0);
						}
					}
					else{
						SC_seqIds.add(id);
						SC_seqToCode.put(id, code);
						SC_seqIds_hash.put(id,0);
					}
				}
				rs.close();
				//SC.put(clus, sqids);
				//sc_keys.add(clus);
				SC.put(code, sqids);
				sc_keys.add(code);
			}
			pstm.close();
			pstm2.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}	
	}
	// run for split files
	public void run(){
		try {

			ArrayList<String> temp = new ArrayList<String>();

			MappingCampsCATH.ActiveThreadCount ++;
			SimapAccessWebService simap=new SimapAccessWebService();
			String md5=simap.computeMD5(this.queryToRun);
			// Setting up the database Id
			int dbid=474; //474 for pdb
			// we get all available databases to look up the dataset-id we want

			simap.addDatabase(dbid);
			simap.setMd5(md5);
			simap.setMax_evalue(1e-5);
			simap.alignments(true);
			simap.sequences(true);

			ArrayList<HitSet> result = new ArrayList<HitSet>();
			try{
				result =ResultParser.parseResult(simap.getHitsXML());
			}
			catch(Exception e){
				System.out.print("No Result Found\n");
				return;
			}
			if(result.size()>=1){
				SeqWithHitsCount ++;
				for(int i =0;i<=result.size()-1;i++){
					HitSet second=result.get(i);

					double identity = second.getHitAlignment().getIdentity();
					double eval = second.getHitAlignment().getEvalue();
					int queryStart = second.getHitAlignment().getQuery_start();
					int queryEnd = second.getHitAlignment().getQuery_stop();
					int hitStart = second.getHitAlignment().getHit_start();
					int hitEnd = second.getHitAlignment().getHit_stop();
					int x =0;
					if(identity > 29){
						for (HitProtein o : second.getHitData().getProteins()) {
							x++;
							// campsid pdbid identity eval qst qend hitst hitend description
							String lineToWrite = this.seqIdToRun+"\t"+o.getTitle()+"\t"+identity+"\t"+eval+"\t"+queryStart+"\t"+queryEnd+"\t"+hitStart+"\t"+hitEnd+"\t"+o.getDescription();
							//System.out.println(lineToWrite);
							temp.add(lineToWrite);
							if(temp.size()>5000){
								write_to_MappingFile(temp);
								temp = new ArrayList<String>();
							}
						}
					}
				}
			}
			write_to_MappingFile(temp);
			temp = new ArrayList<String>();
			this.threadStatus = false;

		} catch (Exception e){
			e.printStackTrace();
			System.exit(0);
		}
	}


	/*// normal run
	public void run(){
		try {

			MappingCampsCATH.ActiveThreadCount ++;
			SimapAccessWebService simap=new SimapAccessWebService();
			//String se = "MTNIRKSHPLMKIVNNAFIDLPAPSNISSWWNFGSLLGICLILQILTGLFLAMHYTSDTTTAFSSVTHICRDVNYGWIIRYMHANGASMFFICLYMHVGRGLYYGSYTFLETWNIGVILLLTVMATAFMGYVLPWGQMSFWGATVITNLLSAIPYIGTNLVEWIWGGFSVDKATLTRFFAFHFILPFIIMAIAMVHLLFLHETGSNNPTGISSDVDKIPFHPYYTIKDILGALLLILALMLLVLFAPDLLGDPDNYTPANPLNTPPHIKPEWYFLFAYAILRSIPNKLGGVLALAFSILILALIPLLHTSKQRSMMFRPLSQCLFWALVADLLTLTWIGGQPVEHPYITIGQLASVLYFLLILVLMPTAGTIENKLLKW";
			//String se = "TATYAQALQSVPETQVSQLDNGLRVASEQSSQPTCTVGVWIDAGSRYESEKNNGAGYFVEHLAFKGTKNRPGNALEKEVESMGAHLNAYSTREHTAYYIKALSKDLPKAVELLADIVQNCSLEDSQIEKERDVILQELQENDTSMRDVVFNYLHATAFQGTPLAQSVEGPSENVRKLSRADLTEYLSRHYKAPRMVLAAAGGLEHRQLLDLAQKHFSGLSGTYDEDAVPTLSPCRFTGSQICHREDGLPLAHVAIAVEGPGWAHPDNVALQVANAIIGHYDCTYGGGAHLSSPLASIAATNKLCQSFQTFNICYADTGLLGAHFVCDHMSIDDMMFVLQGQWMRLCTSATESEVLRGKNLLRNALVSHLDGTTPVCEDIGRSLLTYGRRIPLAEWESRIAEVDARVVREVCSKYFYDQCPAVAGFGPIEQLPDYNRIRSGMFWLRF";

			String md5=simap.computeMD5(this.queryToRun);
			// Setting up the database Id
			int dbid=474; //474 for pdb
			// we get all available databases to look up the dataset-id we want
			/*
			ArrayList<Hashtable> databases=simap.getAllDatabases();
			for (Hashtable mydatab : databases) {
				//System.out.println(mydatab.get("taxon_id")+"\t"+mydatab.get("name")+"\t"+mydatab.get("id")+"\t"+mydatab.get("source"));
				// System.out.println(mydatab.get("name")+"\t"+mydatab.get("id")+"\t"+mydatab.get("source"));
				if (mydatab.get("name").equals("PDB")) {
					//System.out.println(mydatab.get("id"));
					dbid=new Integer((String)mydatab.get("id"));
					break;
				}
			}

			simap.addDatabase(dbid);
			simap.setMd5(md5);
			simap.setMax_evalue(1e-35);
			//simap.setMax_number_hits(10);
			simap.alignments(true);
			simap.sequences(true);

			ArrayList<HitSet> result = new ArrayList<HitSet>();
			try{
				result =ResultParser.parseResult(simap.getHitsXML());
			}
			catch(Exception e){
				System.out.print("No Result Found\n");
			}
			if(result.size()>=1){
				SeqWithHitsCount ++;
				for(int i =0;i<=result.size()-1;i++){
					//System.out.println("Printing Result "+i+" of "+result.size());
					HitSet second=result.get(i);

					double identity = second.getHitAlignment().getIdentity();
					//System.out.println("Identity = "+identity);

					double eval = second.getHitAlignment().getEvalue();
					//System.out.println("Eval = "+eval);

					int queryStart = second.getHitAlignment().getQuery_start();
					int queryEnd = second.getHitAlignment().getQuery_stop();
					int hitStart = second.getHitAlignment().getHit_start();
					int hitEnd = second.getHitAlignment().getHit_stop();
					//System.out.println("from: "+queryStart+" to "+queryEnd+ " in query sequence");
					//System.out.println("from: "+hitStart+" to "+hitEnd+ " in hit sequence");

					int x =0;
					for (HitProtein o : second.getHitData().getProteins()) {
						x++;
						//System.out.println("Hit number "+ x);
						//System.out.println(o.getTitle());
						//System.out.println(o.getSequence_id());
						//System.out.println(o.getDatabase_name()+"-- "+o.getDescription());

						// campsid pdbid identity eval qst qend hitst hitend description
						String lineToWrite = this.seqIdToRun+"\t"+o.getTitle()+"\t"+identity+"\t"+eval+"\t"+queryStart+"\t"+queryEnd+"\t"+hitStart+"\t"+hitEnd+o.getDescription();
						write_to_MappingFile(lineToWrite);

						//Camps2PDBmap.write(this.seqIdToRun+"\t"+o.getTitle()+"\t"+identity+"\t"+eval+"\t"+queryStart+"\t"+queryEnd+"\t"+hitStart+"\t"+hitEnd+o.getDescription());
						//Camps2PDBmap.newLine();
					}
					//System.out.println("----");
				}
			}
			Thread.sleep(60);
			write_to_processedSeqsFile(this.seqIdToRun);
			//processedseqs.write(this.seqIdToRun);
			//processedseqs.newLine();
			//processedseqs.flush();

			//Camps2PDBmap.flush();
			//MappingCampsCATH.ActiveThreadCount --;
			this.threadStatus = false;

		} catch (Exception e){
			e.printStackTrace();
			System.exit(0);
		}
	}
	 *//*	
	public synchronized void write_to_processedSeqsFile(Integer seqIdToRun2) {
		// TODO Auto-generated method stub
		try{

			processedseqs.write(seqIdToRun2.toString());
			processedseqs.newLine();
			processedseqs.flush();

			MappingCampsCATH.ActiveThreadCount --;
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}*/
	public synchronized void write_to_MappingFile(String lineToWrite) {
		// TODO Auto-generated method stub
		try{
			Camps2PDBmap.write(lineToWrite);
			Camps2PDBmap.newLine();
			Camps2PDBmap.flush();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	public synchronized void write_to_MappingFile(ArrayList<String> lines) {
		// TODO Auto-generated method stub
		try{
			for(int i=0; i<=lines.size()-1;i++){
				Camps2PDBmap.write(lines.get(i));
				Camps2PDBmap.newLine();
			}
			Camps2PDBmap.flush();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void closeFiles(){
		try{
			processedseqs.close();
			Camps2PDBmap.close();
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
	public static void closeFiles2(){
		try{
			Camps2PDBmap.close();
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

}


