package workflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;

import datastructures.TMS;

import properties.ConfigFile;
import utils.DBAdaptor;


public class CreateAlignments {


	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");


	private int start;
	private int length;
	private String dir;


	////////////////////////////////////////////////////////////////////////////
	//																		  //
	//				filter parameters										  //
	//      																  //
	////////////////////////////////////////////////////////////////////////////

	private static int BLOB_SIZE = 10000;

	//sequences longer than 2000 are ignored
	private static final int MAX_SEQUENCE_LENGTH = 2000;



	/*
	 * For the specified set of CAMPS clusters (given by a start index and a
	 * number of entries that are used to get a subset of CAMPS clusters from
	 * the table 'cp_clusters') the alignments are generated.
	 * 	
	 */
	public CreateAlignments(int start, int length, String dir) {
		this.start = start;
		this.length = length;
		this.dir = dir;
	}


	/**
	 * Runs the whole program.
	 */
	public void createClustalwAlignments() {		
		try {

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequences FROM clusters_mcl_nr_info WHERE cluster_id=? AND cluster_threshold=?");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT sequences FROM fh_clusters_info WHERE code=?");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl WHERE cluster_id=? and cluster_threshold=? AND redundant=\"No\"");
			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM fh_clusters WHERE code=? AND redundant=\"No\"");

			PreparedStatement pstm5 = CAMPS_CONNECTION.prepareStatement("SELECT tms_range FROM clusters_mcl_nr_info WHERE cluster_id=? AND cluster_threshold=?");
			PreparedStatement pstm6 = CAMPS_CONNECTION.prepareStatement("SELECT tms_range FROM fh_clusters_nr_info WHERE code=?");

			PreparedStatement pstm7 = CAMPS_CONNECTION.prepareStatement("SELECT begin,end FROM tms WHERE sequenceid=? order by begin");		
			PreparedStatement pstm8 = CAMPS_CONNECTION.prepareStatement("SELECT length FROM sequences2 WHERE sequenceid=?");			
			PreparedStatement pstm9 = CAMPS_CONNECTION.prepareStatement("SELECT sequence FROM sequences2 WHERE sequenceid=?");

			PreparedStatement pstm10 = CAMPS_CONNECTION.prepareStatement("SELECT name FROM sequences2names WHERE sequenceid=?");


			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold,code,type,super_cluster_id,super_cluster_threshold FROM cp_clusters limit "+this.start+","+this.length);	
			int counter = 0;
			while(rs.next()) {
				counter++;
				int clusterID = rs.getInt("cluster_id");
				int clusterThreshold = (int) rs.getFloat("cluster_threshold");
				String code = rs.getString("code");
				String type = rs.getString("type");
				int supid = rs.getInt("super_cluster_id");
				float supTh = rs.getFloat("super_cluster_threshold");

				if(supid == 786 && supTh == 105f){ // if non parent md cluster then ignore this cluster... 46 such clusters
					continue;
				}
				int fullSize = 0;

				if(type.equals("sc_cluster") || type.equals("md_cluster")) {

					pstm1.setInt(1, clusterID);
					pstm1.setFloat(2, clusterThreshold);
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {
						fullSize = rs1.getInt("sequences");
					}
					rs1.close();
				}
				else if(type.equals("fh_cluster")) {

					pstm2.setString(1, code);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						fullSize = rs2.getInt("sequences");
					}
					rs2.close();
				}


				//
				//ignore CAMPS clusters that have less than eight members
				//since they are not displayed on the website anyway
				//
				/*
				if(fullSize < 8) {
					continue;
				}
				 */	

				//get distinct sequences for each cluster
				System.out.println("\n\n\t[INFO] Get cluster members for: "+code+"  ("+counter+"/"+this.length+")");
				BitSet sequenceIDs = new BitSet();

				if(type.equals("sc_cluster") || type.equals("md_cluster")) {

					pstm3.setInt(1, clusterID);		
					pstm3.setFloat(2, clusterThreshold);
					ResultSet rs3 = pstm3.executeQuery();
					while(rs3.next()) {
						int sequenceid = rs3.getInt("sequenceid");					
						sequenceIDs.set(sequenceid);					
					}
					rs3.close();
					rs3 = null;


				}
				else if(type.equals("fh_cluster")) {

					pstm4.setString(1, code);
					ResultSet rs4 = pstm4.executeQuery();
					while(rs4.next()) {
						int sequenceid = rs4.getInt("sequenceid");					
						sequenceIDs.set(sequenceid);					
					}
					rs4.close();
					rs4 = null;
				}



				//avoid Blobs
				if(sequenceIDs.cardinality() > BLOB_SIZE) {
					System.out.println("\t[INFO] Cluster size: "+sequenceIDs.cardinality()+" --> Cluster too large --> Ignore!");					
					continue;
				}

				String tmsRange = null;

				if(type.equals("sc_cluster") || type.equals("md_cluster")) {

					pstm5.setInt(1, clusterID);		
					pstm5.setFloat(2, clusterThreshold);
					ResultSet rs5 = pstm5.executeQuery();
					while(rs5.next()) {
						tmsRange = rs5.getString("tms_range");					
					}
					rs5.close();
					rs5 = null;
				}
				else if(type.equals("fh_cluster")) {

					pstm6.setString(1, code);
					ResultSet rs6 = pstm6.executeQuery();
					while(rs6.next()) {
						tmsRange = rs6.getString("tms_range");								
					}
					rs6.close();
					rs6 = null;
				}

				int minNumTms = Integer.parseInt(tmsRange.split("-")[0]);
				int maxNumTms = Integer.parseInt(tmsRange.split("-")[1]);


				System.out.println("\t[INFO] Choose candidates");
				//sequenceid2TmsPredictions: key: sequenceid, value: list of TMS predictions (format: tms_start-tms_end)				
				Hashtable<Integer,ArrayList<String>> sequenceid2TmsPredictions = new Hashtable<Integer,ArrayList<String>>();
				//candidates: subset of sequences with sufficient struct. homog.
				ArrayList<Integer> candidates = new ArrayList<Integer>();		
				for(int sequenceid = sequenceIDs.nextSetBit(0); sequenceid>=0; sequenceid = sequenceIDs.nextSetBit(sequenceid+1)) {

					//get predicted tms
					ArrayList<String> tmsPredictions = new ArrayList<String>();
					int numTms = 0;
					pstm7.setInt(1, sequenceid);
					ResultSet rs7 = pstm7.executeQuery();
					while(rs7.next()) {
						numTms++;						
						int tmsBegin = rs7.getInt("begin");
						int tmsEnd = rs7.getInt("end");						
						tmsPredictions.add(tmsBegin + "-" + tmsEnd);
					}
					rs7.close();
					rs7 = null;

					//get length
					int length = -1;
					pstm8.setInt(1, sequenceid);
					ResultSet rs8 = pstm8.executeQuery();
					while(rs8.next()) {
						length = rs8.getInt("length");
					}
					rs8.close();

					//consider only cluster members with sufficient structural homogeneity
					//and sequence length below threshold
					if(numTms >= minNumTms && numTms <= maxNumTms && length <= MAX_SEQUENCE_LENGTH) {
						sequenceid2TmsPredictions.put(Integer.valueOf(sequenceid), tmsPredictions);
						candidates.add(Integer.valueOf(sequenceid));
					}
				}



				int numCandidates = candidates.size();

				//take most divergent sequences for seed alignment if cluster is too large
				if(numCandidates > 400) {	

					System.out.println("\t[INFO] Get identities");
					float[][] matrix = fillMatrix(candidates);

					System.out.println("\t[INFO] Choose most divergent sequences");
					ArrayList<Integer> newCandidates = getMostDivergent(matrix, candidates, 400);

					Hashtable<Integer,ArrayList<String>> sequenceid2TmsPredictionsCopy = (Hashtable<Integer, ArrayList<String>>) sequenceid2TmsPredictions.clone();
					ArrayList<Integer> candidatesCopy = (ArrayList<Integer>) candidates.clone();
					sequenceid2TmsPredictions = new Hashtable<Integer,ArrayList<String>>();	//reset
					candidates = new ArrayList<Integer>();	//reset

					for(Integer newCandidate: newCandidates) {
						sequenceid2TmsPredictions.put(newCandidate, sequenceid2TmsPredictionsCopy.get(newCandidate));
						candidates.add(newCandidate);
					}				

					sequenceid2TmsPredictionsCopy = null;
					candidatesCopy = null;	
					newCandidates = null;
				}


				System.out.println("\t[INFO] Run alignment program");
				//create FASTA file as input
				File alignmentInfileTMP = File.createTempFile(code+"_", ".fasta");			
				PrintWriter pw = new PrintWriter(new FileWriter(alignmentInfileTMP));
				for(Integer candidate: candidates) {
					String sequence = null;
					pstm9.setInt(1, candidate.intValue());
					ResultSet rs9 = pstm9.executeQuery();
					while(rs9.next()) {
						sequence = rs9.getString("sequence").toUpperCase();
					}
					rs9.close();
					rs9 = null;

					String name = "";
					pstm10.setInt(1, candidate.intValue());
					ResultSet rs10 = pstm10.executeQuery();
					while(rs10.next()) {
						name = rs10.getString("name");
					}
					rs10.close();

					//pw.println(">"+candidate.intValue()+"\n"+sequence);
					pw.println(">"+name+"\n"+sequence);
				}
				pw.close();

				//
				//run alignment program
				//
				File alignmentOutfileTMP = new File(dir+code+".aln");			    
				ConfigFile cf = new ConfigFile("/home/users/saeed/workspace/CAMPS3/config/config.xml");
				//String alignmentDir = cf.getProperty("apps:clustalw:install-dir");clustalOmega
				String alignmentDir = cf.getProperty("apps:clustalOmega:install-dir");
				//String cmd = alignmentDir+" -INFILE=" + alignmentInfileTMP.getAbsolutePath() + " -OUTFILE=" + alignmentOutfileTMP.getAbsolutePath() + " -OUTORDER=INPUT -OUTPUT=FASTA";
				String cmd = alignmentDir+" -i " + alignmentInfileTMP.getAbsolutePath() + " -o " + alignmentOutfileTMP.getAbsolutePath() + " --output-order=input-order" +" --threads=10";

				Process p = Runtime.getRuntime().exec(cmd);
				BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
				String line;
				while((line=br.readLine()) != null) {
					//System.err.println("\t\t[INFO]"+line);
				}
				br.close();

				p.waitFor();
				p.destroy();


				new File(alignmentInfileTMP.getAbsolutePath().replace(".fasta", ".dnd")).delete();
				alignmentInfileTMP.delete();


			}//for each cluster


			pstm1.close();
			pstm1 = null;
			pstm2.close();
			pstm2 = null;
			pstm3.close();
			pstm3 = null;
			pstm4.close();
			pstm4 = null;
			pstm5.close();
			pstm5 = null;
			pstm6.close();
			pstm6 = null;
			pstm7.close();
			pstm7 = null;
			pstm8.close();
			pstm8 = null;
			pstm9.close();
			pstm9 = null;
			pstm10.close();
			pstm10 = null;

		} catch(Exception e) {
			System.err.println("Exception in CreateAlignments.run(): " +e.getMessage());
			e.printStackTrace();

		} finally {			
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}


	/*
	 * Returns a square matrix of the specified size that contains the specified 
	 * float value at each position.
	 * 
	 * size - number of columns/rows 
	 * f    - default value for each matrix cell
	 */
	private static float[][] getDefaultMatrix(int size, float f) {

		float[][] matrix = new float[size][size];
		for(int i=0; i<size; i++) {
			for(int j=0; j<size; j++) {
				matrix[i][j] = f;
			}
		}

		return matrix;
	}


	/*
	 * Returns a square matrix that contains the pairwise identity values for
	 * each pair of the specified set of candidate sequences as extracted from 
	 * table 'alignments'.
	 * 
	 * Self scores are not inserted, as well as redundant entries. At those
	 * positions the matrix contains the value 2 (since the identity values from 
	 * table 'alignments' range from 0 to 1). 
	 * 
	 * candidates - set of sequences (given by sequence id)
	 */

	private static Hashtable<Integer,Integer> SequencesInCp(){
		Hashtable<Integer,Integer> list = new Hashtable<Integer,Integer>();
		try{
			System.out.println("Generating list for alignments table");
			System.out.flush();
			PreparedStatement pr = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters");

			PreparedStatement pr2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl where cluster_id=? and cluster_threshold=?");

			ResultSet rs = pr.executeQuery();
			while(rs.next()){
				int clusid = rs.getInt(1);
				float th = rs.getFloat(2);
				pr2.setInt(1, clusid);
				pr2.setFloat(2, th);
				ResultSet rs2 = pr2.executeQuery();
				while(rs2.next()){
					int sq = rs2.getInt(1);
					if(!list.containsKey(sq)){
						list.put(sq, 0);
					}
				}
				rs2.close();
			}
			rs.close();
			pr.close();
			pr2.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return list;
	}
	static Hashtable<Integer, Dictionary> dict = new Hashtable<Integer, Dictionary>();
	static boolean dictbool = false;
	private static float[][] fillMatrix(ArrayList<Integer> candidates) {

		float[][] matrix = null;
		try {			
			Collections.sort(candidates);
			//
			//note: searching in only one direction here is fine since 
			//table 'alignments' is structured in the way that the 
			//sequenceid_query is always smaller than the  
			//sequenceid_hit
			//and the specified candidates list is sorted in ascending order
			//
			//PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_hit,identity FROM alignments_initial where seqid_query=?");
			// The seq id hit and query have not been changed as after being checked by distinct 
			// statement on seqid_query .... > 300k seqid_query

			// make a hashmap with key as seqid_query and value is seqid_hit and identity value.
			// Can use the Dictionary class, as it is exactly what is required.
			matrix = getDefaultMatrix(candidates.size(), 2);
			PreparedStatement pstm = null;
			if(dictbool == false){
				Hashtable<Integer,Integer> list = SequencesInCp();
				int idx =1;
				for (int i=1; i<=17;i++){				
					pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial limit "+idx+","+10000000);
					//pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial");
					idx = i*10000000;
					System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
					ResultSet rs = pstm.executeQuery();
					while(rs.next()) {
						int idhit = rs.getInt("seqid_hit");
						int key = rs.getInt ("seqid_query");
						float identity = rs.getFloat("identity");
						if(list.containsKey(idhit) || list.containsKey(key)){
							if (dict.containsKey(key)){ // the key already doesnt exist...so that you dont delete the previous info of the key
								Dictionary temp = (Dictionary)dict.get(key);
								temp.set(idhit, identity);
								dict.put(key,temp);
							}
							else{
								Dictionary temp = new Dictionary(idhit, identity);
								dict.put(key,temp);
							}
						}
					}

					// dict population complete
					rs.close();
				}
				dictbool = true;
				pstm.close();
			}
			System.out.print("\n Hashtable of Alignments Complete\n");
			System.out.print(" Making Matrix Now\n");
			System.out.print(" Size of Candidates "+candidates.size()+"\n");
			System.out.print(" Size of hashtable "+dict.size()+"\n");
			System.out.flush();

			for(int i=0; i<candidates.size(); i++) {

				int id1 = candidates.get(i).intValue();	// query

				// search the dict for this query and get all its hits and populate as it is
				if (dict.containsKey(id1)){	// contains the query as key
					Dictionary temp = (Dictionary)dict.get(id1);
					// find the hits for presence of candidates and put in matrix
					// for all the hits... are they present in cadidates
					for (int j =0; j<=temp.protein.size()-1;j++){
						Integer id2I = temp.protein.get(j);	// hit
						if(candidates.contains(id2I)) {			// if the hit is a part of selected candidates 
							int index = candidates.indexOf(id2I);	// index of hit
							float identity = temp.score.get(j);
							matrix[i][index] = identity;
							//System.out.print(".");
							//System.out.flush();
						}
					}
				}
			}
			System.out.print(" End of Matrix formulation for number of candidates "+candidates.size()+"\n");
			System.out.flush();

		}catch(Exception e) {
			e.printStackTrace();
		}
		return matrix;
	}



	/*
	 * Returns the specified number of most divergent sequences (given by sequence id)
	 * from the specified set of sequences by parsing the specified matrix.
	 * 
	 * matrix         - matrix containing the pairwise identity values
	 * ids            - set of sequences (given by sequence id)
	 * wantedEntries  - number of most divergent sequences that will be returned
	 */
	private static ArrayList<Integer> getMostDivergent(float[][] matrix, ArrayList<Integer> ids, int wantedEntries) {

		//number of already found most divergent sequences
		int selectedEntries = 0;

		//list of sequence ids (subset of 'ids') that represent the most
		//divergent sequences
		ArrayList<Integer> result = new ArrayList<Integer>();

		float[][] copyMatrix = matrix.clone();
		ArrayList<Integer> copyIds = (ArrayList<Integer>) ids.clone();

		//indices of already found most divergent sequences
		int[] alreadyFound = new int[wantedEntries];

		//get first two entries
		int size = copyMatrix.length;

		double min = Double.MAX_VALUE;
		int rowIndex = -1;
		int columnIndex = -1;

		//only parse right triangular matrix
		for(int i=0; i<size; i++) {

			for(int j=i+1; j<size; j++) {

				int row = i;
				int column = j;
				double value = copyMatrix[i][j];

				if(value<min) {
					min = value;
					rowIndex = row;
					columnIndex = column;
				}					
			}				
		}

		result.add(ids.get(rowIndex));
		result.add(ids.get(columnIndex));
		copyIds.set(rowIndex, Integer.valueOf(-1));		//mark sequence id as already filtered
		copyIds.set(columnIndex,Integer.valueOf(-1));	//mark sequence id as already filtered
		alreadyFound[0] = rowIndex;
		alreadyFound[1] = columnIndex;

		selectedEntries += 2;


		//
		//get remaining entries	by using the following procedure:
		//
		// - all already found sequence ids are compared with each sequence id
		//   that is still left
		// - all pairwise identity (PI) values are summed up
		// - the sequence id with the smallest sum is chosen
		//
		// example: 
		// ids = (2,6,9,15)
		// already found ids: (2,6)
		// calculation 1: (2,6) against 9  => PI(2,9) + PI(6,9) = x
		// calculation 2: (2,6) against 15 => PI(2,15)+ PI(6,15)= y
		// if x < y, then ids = (2,6,9)
		// else ids = (2,6,15)
		//
		while(selectedEntries < wantedEntries) {

			double minSum = Double.MAX_VALUE;
			int index = -1;	//index of next most divergent sequence to be found

			for(int i=0; i<copyIds.size(); i++) {

				Integer id = copyIds.get(i);
				if(id.intValue() == -1) {	//id is already in result list
					continue;
				}

				double sum = 0;
				for(int j=0;j<selectedEntries; j++) {

					int currentIndex = alreadyFound[j];

					if(i<currentIndex) {
						sum += copyMatrix[i][currentIndex];
					}
					else {
						sum += copyMatrix[currentIndex][i];
					}

				}

				if(sum < minSum) {
					minSum = sum;
					index = i;
				}
			}

			result.add(ids.get(index));
			copyIds.set(index,Integer.valueOf(-1));
			alreadyFound[selectedEntries] = index;

			selectedEntries++;
		}

		copyMatrix = null;
		copyIds = null;

		return result;
	}


	/*
	 * Checks which ClustalW alignment files are missing.
	 */
	public static void checkAlignmentFiles(String dir) {
		try {

			int countEmptyFiles = 0;

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequences FROM clusters_mcl_info WHERE cluster_id=? AND cluster_threshold=?");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT sequences FROM fh_clusters_info WHERE code=?");

			Statement stm = CAMPS_CONNECTION.createStatement();

			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold,code,type FROM cp_clusters");	
			int counter = 0;
			while(rs.next()) {
				counter++;
				int clusterID = rs.getInt("cluster_id");
				int clusterThreshold = (int) rs.getFloat("cluster_threshold");
				String code = rs.getString("code");
				String type = rs.getString("type");


				int fullSize = 0;

				if(type.equals("sc_cluster") || type.equals("md_cluster")) {

					pstm1.setInt(1, clusterID);
					pstm1.setFloat(2, clusterThreshold);
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {
						fullSize = rs1.getInt("sequences");
					}
					rs1.close();
				}
				else if(type.equals("fh_cluster")) {

					pstm2.setString(1, code);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						fullSize = rs2.getInt("sequences");
					}
					rs2.close();
				}

				if(fullSize < 8) {
					continue;
				}


				//
				//check if alignment file is available
				//
				File file = new File(dir+code+".aln");

				if(!file.exists()) {
					System.out.println("Alignment file missing for: " +code);
				}
				else{

					BufferedReader br = new BufferedReader(new FileReader(file));
					String line;
					int lineCount = 0;
					while((line = br.readLine()) != null) {

						lineCount++;
					}
					br.close();

					if(lineCount == 0) {
						countEmptyFiles++;
						System.out.println("Empty alignment file for: " +code);
					}					
				}
			}
			rs.close();

			System.out.println("\nNumber of empty alignment files: " +countEmptyFiles);


			pstm1.close();
			pstm2.close();

			stm.close();

		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}


	public static void createTexshadePDFFiles(int start, int length, String alignmentDir, String outdir) {

		Connection conn = null;

		try {

			conn = DBAdaptor.getConnection("camps3");

			PreparedStatement pstm2 = conn.prepareStatement(
					"SELECT distinct begin_aligned,end_aligned " +
							"FROM tms_cores " +
							"WHERE cluster_id=? and cluster_threshold=? " +
					"ORDER BY begin_aligned;");

			PreparedStatement pstm3 = conn.prepareStatement(
					"SELECT distinct begin_aligned,end_aligned " +
							"FROM md_clusters_tms_cores " +
							"WHERE cluster_id=? and cluster_threshold=? " +
					"ORDER BY begin_aligned;");

			PreparedStatement pstm4 = conn.prepareStatement(
					"SELECT distinct begin_aligned,end_aligned " +
							"FROM fh_clusters_tms_cores " +
							"WHERE code=? " +
					"ORDER BY begin_aligned;");


			Statement stm = conn.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold,code,type FROM cp_clusters limit "+start+","+length);	
			int countAlignmentFile = 0;
			while(rs.next()) {

				int clusterID = rs.getInt("cluster_id");
				int clusterThreshold = (int) rs.getFloat("cluster_threshold");
				String code = rs.getString("code");
				String type = rs.getString("type");

				File alignmentFile = new File(alignmentDir+code+".aln");

				if(!alignmentFile.exists()) {
					continue;
				}


				countAlignmentFile++;

				System.out.println("In progress: "+code+" ("+countAlignmentFile+")");

				ArrayList<TMS> cores = new ArrayList<TMS>();

				if(type.equals("sc_cluster")) {	//SC-cluster

					pstm2.setInt(1, clusterID);
					pstm2.setFloat(2, clusterThreshold);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						int beginAligned = rs2.getInt("begin_aligned"); 
						int endAligned = rs2.getInt("end_aligned");

						TMS core = new TMS(beginAligned,endAligned);
						cores.add(core);
					}
					rs2.close();
				}
				else if(type.equals("md_cluster")) {

					boolean notYetFound = true;

					pstm2.setInt(1, clusterID);
					pstm2.setFloat(2, clusterThreshold);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						notYetFound = false;
						int beginAligned = rs2.getInt("begin_aligned"); 
						int endAligned = rs2.getInt("end_aligned");

						TMS core = new TMS(beginAligned,endAligned);
						cores.add(core);
					}
					rs2.close();

					if(notYetFound) {

						pstm3.setInt(1, clusterID);
						pstm3.setFloat(2, clusterThreshold);
						ResultSet rs3 = pstm3.executeQuery();
						while(rs3.next()) {
							notYetFound = false;
							int beginAligned = rs3.getInt("begin_aligned"); 
							int endAligned = rs3.getInt("end_aligned");

							TMS core = new TMS(beginAligned,endAligned);
							cores.add(core);
						}
						rs3.close();
					}
				}
				else if(type.equals("fh_cluster")) {

					pstm4.setString(1, code);
					ResultSet rs4 = pstm4.executeQuery();
					while(rs4.next()) {
						int beginAligned = rs4.getInt("begin_aligned"); 
						int endAligned = rs4.getInt("end_aligned");

						TMS core = new TMS(beginAligned,endAligned);
						cores.add(core);
					}
					rs4.close();
				}

				String texFile = outdir+code+".tex";

				writeTexFile(cores, code, alignmentFile.getAbsolutePath(), texFile);


				//
				//create temporary sh files
				//
				File tmpFile = File.createTempFile(code+"_", ".sh");
				PrintWriter pw = new PrintWriter(new FileWriter(tmpFile));

				pw.println("cd "+outdir);
				pw.println("pdflatex -interaction=batchmode "+texFile);				
				pw.println();
				pw.println("rm "+code+".aux "+code+".log");

				pw.close();


				//
				//run latex
				//
				String cmd = "sh "+tmpFile.getAbsolutePath();
				Process p = Runtime.getRuntime().exec(cmd);
				BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
				String line;
				while((line=br.readLine()) != null) {
					System.err.println(line);			    				    	
				}		    
				p.waitFor();
				p.destroy();				

				br.close();


				tmpFile.delete();
			}


			pstm2.close();
			pstm3.close();
			pstm4.close();

		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (conn != null) {
				try {
					conn.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}


	private static void writeTexFile(ArrayList<TMS> cores, String code, String alignmentFile, String texFile) {
		try {

			PrintWriter pw = new PrintWriter(new File(texFile));

			pw.println("\\documentclass{article}");
			pw.println("\\usepackage[landscape]{geometry}");
			pw.println("\\usepackage{texshade}");
			pw.println("\\pagestyle{empty}");
			pw.println("");
			//pw.println("\\oddsidemargin -0.5in");
			//pw.println("\\evensidemargin -0.5in");
			//pw.println("\\topmargin -0.5in");
			//pw.println("\\headheight -0.5in");
			pw.println("\\addtolength{\\oddsidemargin}{-.875in}");
			pw.println("\\addtolength{\\evensidemargin}{-.875in}");
			pw.println("\\addtolength{\\textwidth}{1.75in}");
			pw.println("\\addtolength{\\topmargin}{-.5in}");
			pw.println("\\addtolength{\\textheight}{1.75in}");
			pw.println("");
			pw.println("\\begin{document}");
			pw.println("  \\begin{texshade}{"+alignmentFile+"}");
			pw.println("    \\textbf{Alignment for "+code.replace("_", "\\_")+"}");
			pw.println("");
			pw.println("    \\seqtype{P}");
			pw.println("    \\topspace{1mm}");
			pw.println("    \\bottomspace{1mm}");
			pw.println("    \\residuesperline*{100}");
			pw.println("");
			pw.println("    \\definecolor{mypink}{cmyk}{0.00, 0.45, 0.45, 0.10}");
			pw.println("    \\definecolor{myblue}{cmyk}{0.86, 0.00, 0.00, 0.30}");
			pw.println("    \\definecolor{mypurple}{cmyk}{0.00, 0.63, 0.00, 0.20}");
			pw.println("");
			pw.println("    \\nomatchresidues{Black}{White}{upper}{up}");
			pw.println("    \\conservedresidues{Black}{myblue}{upper}{up}");
			pw.println("    \\allmatchresidues{Black}{mypurple}{upper}{up}");
			pw.println("    \\similarresidues{Black}{mypink}{upper}{up}");
			pw.println("");
			pw.println("    \\shadingmode[allmatchspecial]{similar}");
			pw.println("");


			String coresStr = "";

			int countCore = 0;
			for(TMS core: cores) {
				countCore++;

				int begin = core.get_start();
				int end = core.get_end();

				pw.println("    \\feature{top}{consensus}{"+begin+".."+end+"}{box[Gray30]:TMH core"+countCore+"[Black]}{}");
				coresStr += ","+begin+".."+end;
			}

			if(countCore>0) {
				coresStr = coresStr.substring(1);
				pw.println("    \\frameblock{consensus}{"+coresStr+"}{Gray30[1.0pt]}");
			}


			pw.println("");
			pw.println("    \\showsequencelogo[chemical]{bottom}");
			pw.println("    \\namesequencelogo{Sequence logo}");
			pw.println("");
			pw.println("    \\bargraphstretch{3}");
			pw.println("    \\hideconsensus");
			pw.println("    \\showlegend");
			pw.println("    \\hidenumbering");
			pw.println("    \\showruler[Black]{top}{consensus}");
			pw.println("    \\gapchar{-}");
			pw.println("");
			pw.println("   \\end{texshade}");
			pw.println("   [Created with \\TeXshade]");
			pw.println("");
			pw.println("\\end{document}");

			pw.close();


		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	/*
	 * Checks the log files from the batch queue runs, if the runs
	 * were finished successfully or not.
	 */
	public static void checkLogFiles(String logDir) {
		try {

			int numJobs = 86;
			int countCorrectJobs = 0;
			int countProcessedJobs = 0;

			int countErrorFiles = 0;
			int countOutputFiles = 0;

			File[] logFiles = new File(logDir).listFiles();

			for(int i=1; i<= numJobs; i++) {

				int countOutput = 0;

				boolean notYetProcessed = true;

				boolean outputOK = false;
				boolean errorOK = true;

				//check output file
				for(File logFile: logFiles) {

					if(logFile.getName().startsWith("alPlot"+i+".o")) {

						countOutput++;

						countOutputFiles++;

						countProcessedJobs++;

						notYetProcessed = false;

						int count = 0;
						boolean startFound = false;
						boolean doneFound = false;

						BufferedReader br = new BufferedReader(new FileReader(logFile));
						String line;
						while((line = br.readLine()) != null) {

							if(line.contains("In progress")) {
								count++;
							}
							else if(line.contains("Start")) {
								startFound = true;
							}
							else if(line.contains("DONE")) {
								doneFound = true;
							}
						}
						br.close();

						//						if(count == 300 && startFound && doneFound) {
						//							outputOK = true;
						//						}
						if(startFound && doneFound) {
							outputOK = true;
						}
					}					
				}


				//check error file
				for(File logFile: logFiles) {

					if(logFile.getName().startsWith("alPlot"+i+".e")) {

						countErrorFiles++;

						BufferedReader br = new BufferedReader(new FileReader(logFile));
						String line;
						while((line = br.readLine()) != null) {

							line = line.trim();

							if(line.equals("This is pdfTeXk, Version 3.141592-1.40.3 (Web2C 7.5.6)")) {
								continue;
							}
							else if(line.equals("%&-line parsing enabled.")) {
								continue;
							}
							else if(line.equals("entering extended mode")) {
								continue;
							}

							System.out.println("\t"+line);
							errorOK = false;
						}
						br.close();					
					}					
				}

				if(notYetProcessed) {

				}
				else{
					if(outputOK && errorOK) {
						countCorrectJobs++;
					}
					else{
						System.out.println("Job"+i+": Output o.k.?: "+outputOK+"  Error empty?: " +errorOK);
					}					
				}

				if(countOutput > 1) {
					System.out.println("Multiple runs for: alPlot" +i);
				}
			}

			System.out.println("\nNumber of successfully finished jobs: " +countCorrectJobs+"/"+countProcessedJobs);
			System.out.println("Number of output files: "+countOutputFiles);
			System.out.println("Number of error files: "+countErrorFiles);

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	/*
	 * Check which texshade plots are missing
	 */
	public static void checkPlotFiles(String dir) {
		try {

			int countMissingPDFFiles = 0;

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequences FROM clusters_mcl_info WHERE cluster_id=? AND cluster_threshold=?");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT sequences FROM fh_clusters_info WHERE code=?");

			Statement stm = CAMPS_CONNECTION.createStatement();

			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold,code,type FROM cp_clusters");	
			int counter = 0;
			while(rs.next()) {
				counter++;
				int clusterID = rs.getInt("cluster_id");
				int clusterThreshold = (int) rs.getFloat("cluster_threshold");
				String code = rs.getString("code");
				String type = rs.getString("type");


				int fullSize = 0;

				if(type.equals("sc_cluster") || type.equals("md_cluster")) {

					pstm1.setInt(1, clusterID);
					pstm1.setFloat(2, clusterThreshold);
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {
						fullSize = rs1.getInt("sequences");
					}
					rs1.close();
				}
				else if(type.equals("fh_cluster")) {

					pstm2.setString(1, code);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						fullSize = rs2.getInt("sequences");
					}
					rs2.close();
				}

				if(fullSize < 8) {
					continue;
				}


				//
				//check if alignment file is available
				//
				File file = new File(dir+code+".pdf");

				if(!file.exists()) {
					countMissingPDFFiles++;
					System.out.println("PDF file missing for: " +code);
				}				
			}
			rs.close();

			System.out.println("Number of missing PDF files (plots): " +countMissingPDFFiles);		

			pstm1.close();
			pstm2.close();

			stm.close();

		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}


	/*
	 * In first run texshade pdf files could not be generated for every
	 * single cluster, because of TeX capacity problems (memory etc).
	 * 
	 * For these clusters the TeXshade program was run again locally so
	 * that my texmf.cnf configuration file is used that contains changes
	 * regarding the capacities.
	 */
	public static void createMissingTexshadePDFFiles(String dir, String logFile) {


		try {

			PrintWriter pwLog = new PrintWriter(new FileWriter(new File(logFile)));

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequences FROM clusters_mcl_info WHERE cluster_id=? AND cluster_threshold=?");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT sequences FROM fh_clusters_info WHERE code=?");

			Statement stm = CAMPS_CONNECTION.createStatement();

			int countMissingPDFFiles = 0;
			int counter = 0;

			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold,code,type FROM cp_clusters");				
			while(rs.next()) {

				int clusterID = rs.getInt("cluster_id");
				int clusterThreshold = (int) rs.getFloat("cluster_threshold");
				String code = rs.getString("code");
				String type = rs.getString("type");


				int fullSize = 0;

				if(type.equals("sc_cluster") || type.equals("md_cluster")) {

					pstm1.setInt(1, clusterID);
					pstm1.setFloat(2, clusterThreshold);
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {
						fullSize = rs1.getInt("sequences");
					}
					rs1.close();
				}
				else if(type.equals("fh_cluster")) {

					pstm2.setString(1, code);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						fullSize = rs2.getInt("sequences");
					}
					rs2.close();
				}

				if(fullSize < 8) {
					continue;
				}


				//
				//check if alignment file is available
				//
				File file = new File(dir+code+".pdf");

				if(!file.exists()) {					

					String texFile = dir+code+".tex";	//tex file already exists from first run

					File tmp1 = new File(texFile);

					if(!tmp1.exists()) {
						System.out.println("\tWARNING: TeX file for "+code+" is missing!");
						continue;
					}

					countMissingPDFFiles++;
					counter++;

					pwLog.println("In progress: " +code);
					System.out.println("In progress: " +code+" ("+counter+")");

					//
					//create temporary sh files
					//
					File tmpFile = File.createTempFile(code+"_", ".sh");
					PrintWriter pw = new PrintWriter(new FileWriter(tmpFile));

					pw.println("cd "+dir);
					pw.println("pdflatex -interaction=batchmode "+texFile);				
					pw.println();
					pw.println("rm "+code+".aux "+code+".log");

					pw.close();


					//
					//run latex
					//
					String cmd = "sh "+tmpFile.getAbsolutePath();
					Process p = Runtime.getRuntime().exec(cmd);
					BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
					String line;
					while((line=br.readLine()) != null) {
						System.err.println(line);			    				    	
					}		    
					p.waitFor();
					p.destroy();				

					br.close();


					tmpFile.delete();

					//
					//check if pdf now exists
					//
					File tmp2 = new File(dir+code+".pdf");
					if(tmp2.exists()) {
						pwLog.println("\tSuccessfully created PDF file for "+code);
					}
					else {
						System.out.println("\tSorry, PDF file could not be generated!");
					}
				}				
			}
			rs.close();

			System.out.println("\n"+countMissingPDFFiles+" PDF files were missing");

			pwLog.close();

			pstm1.close();
			pstm2.close();

			stm.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}



	public static void createTMSAnnotationFiles4Jalview(String alignmentDir) {


		Connection conn = null;

		try {

			conn = DBAdaptor.getConnection("camps3");

			PreparedStatement pstm0 = conn.prepareStatement(
					"SELECT sequenceid FROM sequences2names WHERE name like ?");

			PreparedStatement pstm1 = conn.prepareStatement(
					"SELECT sequenceid FROM sequences2names WHERE name=?");

			PreparedStatement pstm2 = conn.prepareStatement(
					"SELECT begin,end " +
							"FROM tms " +
					"WHERE sequences_sequenceid=? ORDER BY tms_id");




			File[] files = new File(alignmentDir).listFiles();

			int counter = 0;
			for(File file: files) {

				if(file.getName().endsWith("aln")) {

					counter++;

					String code = file.getName().replace(".aln","");

					System.out.println("In progress: " +code+" ("+counter+")");


					ArrayList<String> proteins = new ArrayList<String>();

					BufferedReader br = new BufferedReader(new FileReader(file));
					String line;
					while((line = br.readLine()) != null) {

						line = line.trim();
						String tmp = line.replaceAll("\\:", "").replaceAll("\\.", "").replaceAll("\\*", "").trim();

						if(line.startsWith("CLUSTAL") || line.equals("") || tmp.equals("")) {
							continue;
						}

						String[] data = line.split("\\s+");
						String protein = data[0].trim();

						if(!proteins.contains(protein)) {
							proteins.add(protein);
						}
					}
					br.close();


					PrintWriter pw = new PrintWriter(new FileWriter(new File(alignmentDir+code+"_tms.gff")));

					for(String protein: proteins) {

						String name = protein;
						int sequenceid = -1;

						//somehow Clustal seems to convert names like GI:12345
						//to GI_12345
						if(protein.startsWith("GI_")) {
							name = protein.replace("GI_", "GI:");

							pstm1.setString(1, name);
							ResultSet rs1 = pstm1.executeQuery();
							while(rs1.next()) {

								sequenceid = rs1.getInt("sequenceid");
							}
							rs1.close();
						}
						//Clustal seems to cut protein names if they are too long
						else if(protein.startsWith("fgenesh1_est.C_scaffold") || protein.startsWith("fgenesh4_pm.C_scaffold")) {

							pstm0.setString(1, name+"%");
							ResultSet rs0 = pstm0.executeQuery();
							while(rs0.next()) {

								sequenceid = rs0.getInt("sequenceid");
							}
							rs0.close();
						}
						else{

							pstm1.setString(1, name);
							ResultSet rs1 = pstm1.executeQuery();
							while(rs1.next()) {

								sequenceid = rs1.getInt("sequenceid");
							}
							rs1.close();
						}


						if(sequenceid != -1) {

							pstm2.setInt(1, sequenceid);
							ResultSet rs2 = pstm2.executeQuery();
							while(rs2.next()) {

								int begin = rs2.getInt("begin");
								int end = rs2.getInt("end");

								pw.println(protein+"\tPhobius\ttms\t"+begin+"\t"+end+"\t0.0");
							}
							rs2.close();
						}
						else {
							System.out.println("WARNING: Could not find protein " +protein);							
						}
					}

					pw.close();
				}
			}


			pstm1.close();
			pstm2.close();

			conn.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	public static void createCoreAnnotationFiles4Jalview(String alignmentDir) {


		Connection conn = null;

		try {

			conn = DBAdaptor.getConnection("camps3");


			PreparedStatement pstm1 = conn.prepareStatement(
					"SELECT cluster_id,cluster_threshold " +
							"FROM cp_clusters " +
					"WHERE code=?");

			PreparedStatement pstm2 = conn.prepareStatement(
					"SELECT distinct begin_aligned,end_aligned " +
							"FROM tms_cores " +
					"WHERE cluster_id=? AND cluster_threshold=?");

			PreparedStatement pstm3 = conn.prepareStatement(
					"SELECT distinct begin_aligned,end_aligned " +
							"FROM md_clusters_tms_cores " +
					"WHERE cluster_id=? AND cluster_threshold=?");

			PreparedStatement pstm4 = conn.prepareStatement(
					"SELECT distinct begin_aligned,end_aligned " +
							"FROM fh_clusters_tms_cores " +
					"WHERE code=?");




			File[] files = new File(alignmentDir).listFiles();

			int counter = 0;
			for(File file: files) {

				if(file.getName().endsWith("aln")) {

					counter++;

					String code = file.getName().replace(".aln","");

					System.out.println("In progress: " +code+" ("+counter+")");

					//
					//get alignment length
					//					
					String firstAlignedProteinSequence = "";
					String firstAlignedProteinName = null;

					BufferedReader br = new BufferedReader(new FileReader(file));
					String line;
					while((line = br.readLine()) != null) {

						line = line.trim();
						String tmp = line.replaceAll("\\:", "").replaceAll("\\.", "").replaceAll("\\*", "").trim();

						if(line.startsWith("CLUSTAL") || line.equals("") || tmp.equals("")) {
							continue;
						}

						String[] data = line.split("\\s+");
						String proteinName = data[0].trim();
						String proteinSequence = data[1].trim();

						if(firstAlignedProteinName == null) {
							firstAlignedProteinName = proteinName;
						}

						if(firstAlignedProteinName.equals(proteinName)) {
							firstAlignedProteinSequence += proteinSequence;
						}

					}
					br.close();

					int alignmentLength = firstAlignedProteinSequence.length();

					//
					//get cores
					//


					ArrayList<String> cores = new ArrayList<String>();

					if(code.contains("_FH")) {

						pstm4.setString(1, code);
						ResultSet rs4 = pstm4.executeQuery();
						while(rs4.next()) {

							int coreBegin = rs4.getInt("begin_aligned");
							int coreEnd = rs4.getInt("end_aligned");

							String core = coreBegin+"-"+coreEnd;
							cores.add(core);
						}
						rs4.close();
					}
					else{

						int clusterID = -1;
						float clusterThreshold = -1;

						pstm1.setString(1, code);
						ResultSet rs1 = pstm1.executeQuery();
						while(rs1.next()) {
							clusterID = rs1.getInt("cluster_id");
							clusterThreshold = rs1.getFloat("cluster_threshold");
						}
						rs1.close();

						boolean foundCores = false;

						pstm2.setInt(1, clusterID);
						pstm2.setFloat(2, clusterThreshold);
						ResultSet rs2 = pstm2.executeQuery();
						while(rs2.next()) {
							foundCores = true;

							int coreBegin = rs2.getInt("begin_aligned");
							int coreEnd = rs2.getInt("end_aligned");

							String core = coreBegin+"-"+coreEnd;
							cores.add(core);
						}
						rs2.close();

						if(!foundCores) {

							pstm3.setInt(1, clusterID);
							pstm3.setFloat(2, clusterThreshold);
							ResultSet rs3 = pstm3.executeQuery();
							while(rs3.next()) {
								foundCores = true;

								int coreBegin = rs3.getInt("begin_aligned");
								int coreEnd = rs3.getInt("end_aligned");

								String core = coreBegin+"-"+coreEnd;
								cores.add(core);
							}
							rs3.close();
						}
					}


					PrintWriter pw = new PrintWriter(new FileWriter(new File(alignmentDir+code+"_cores.txt")));

					pw.println("JALVIEW_ANNOTATION");
					pw.println("");

					String coresInfo = "";

					int previousEnd = 1;

					int countCore = 0;
					for(String core: cores) {

						countCore++;

						int coreBegin = Integer.parseInt(core.split("-")[0]);
						int coreEnd = Integer.parseInt(core.split("-")[1]);

						int coreLength = (coreEnd-coreBegin) + 1;

						int loopLength = ((coreBegin-1) - previousEnd) + 1;

						for(int i=1; i<=loopLength; i++) {

							coresInfo += "|";
						}

						for(int i=1; i<=coreLength; i++){

							coresInfo += "H,core"+countCore+"|";
						}

						previousEnd = coreEnd+1;
					}

					//add last loop
					int lastLoopLength = (alignmentLength - previousEnd) + 1;

					for(int i=1; i<=lastLoopLength; i++) {

						coresInfo += "|";
					}

					pw.println("NO_GRAPH\tTMH cores\t"+coresInfo);

					pw.close();
				}
			}


			pstm1.close();
			pstm2.close();
			pstm3.close();
			pstm4.close();

			conn.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	public static void createPfamAnnotationFiles4Jalview(String alignmentDir) {


		Connection conn = null;

		try {

			conn = DBAdaptor.getConnection("camps3");

			PreparedStatement pstm0 = conn.prepareStatement(
					"SELECT sequenceid FROM sequences2names WHERE name like ?");

			PreparedStatement pstm1 = conn.prepareStatement(
					"SELECT sequenceid FROM sequences2names WHERE name=?");

			PreparedStatement pstm2 = conn.prepareStatement(
					"SELECT accession,begin,end " +
							"FROM domains_pfam " +
					"WHERE sequences_sequenceid=? ORDER BY begin");




			File[] files = new File(alignmentDir).listFiles();

			int counter = 0;
			for(File file: files) {

				if(file.getName().endsWith("aln")) {

					counter++;

					String code = file.getName().replace(".aln","");

					System.out.println("In progress: " +code+" ("+counter+")");


					ArrayList<String> proteins = new ArrayList<String>();

					BufferedReader br = new BufferedReader(new FileReader(file));
					String line;
					while((line = br.readLine()) != null) {

						line = line.trim();
						String tmp = line.replaceAll("\\:", "").replaceAll("\\.", "").replaceAll("\\*", "").trim();

						if(line.startsWith("CLUSTAL") || line.equals("") || tmp.equals("")) {
							continue;
						}

						String[] data = line.split("\\s+");
						String protein = data[0].trim();

						if(!proteins.contains(protein)) {
							proteins.add(protein);
						}
					}
					br.close();


					PrintWriter pw = new PrintWriter(new FileWriter(new File(alignmentDir+code+"_pfam.gff")));

					for(String protein: proteins) {

						String name = protein;
						int sequenceid = -1;

						//somehow Clustal seems to convert names like GI:12345
						//to GI_12345
						if(protein.startsWith("GI_")) {
							name = protein.replace("GI_", "GI:");

							pstm1.setString(1, name);
							ResultSet rs1 = pstm1.executeQuery();
							while(rs1.next()) {

								sequenceid = rs1.getInt("sequenceid");
							}
							rs1.close();
						}
						//Clustal seems to cut protein names if they are too long
						else if(protein.startsWith("fgenesh1_est.C_scaffold") || protein.startsWith("fgenesh4_pm.C_scaffold")) {

							pstm0.setString(1, name+"%");
							ResultSet rs0 = pstm0.executeQuery();
							while(rs0.next()) {

								sequenceid = rs0.getInt("sequenceid");
							}
							rs0.close();
						}
						else{

							pstm1.setString(1, name);
							ResultSet rs1 = pstm1.executeQuery();
							while(rs1.next()) {

								sequenceid = rs1.getInt("sequenceid");
							}
							rs1.close();
						}


						if(sequenceid != -1) {

							pstm2.setInt(1, sequenceid);
							ResultSet rs2 = pstm2.executeQuery();
							while(rs2.next()) {

								String accession = rs2.getString("accession");
								int begin = rs2.getInt("begin");
								int end = rs2.getInt("end");

								pw.println(protein+"\tPfam\t"+accession+"\t"+begin+"\t"+end+"\t0.0");
							}
							rs2.close();
						}
						else {
							System.out.println("WARNING: Could not find protein " +protein);							
						}
					}

					pw.close();
				}
			}


			pstm1.close();
			pstm2.close();

			conn.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) {

		try {

			//
			//Create raw alignment files using ClustalW
			//


			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			//			
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");

			int start = Integer.parseInt(args[0]);
			int length = Integer.parseInt(args[1]);
			//int start = 0;
			//int length = 50;
			String dir = args[2];
			//			
			//						
			CreateAlignments ca = new CreateAlignments(start,length, dir);
			ca.createClustalwAlignments();
			//			
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");


			//##################################################################


			//			String alignmentDir = "/scratch/sneumann/Camps3/fullAlignments/";
			//			checkAlignmentFiles(alignmentDir);


			//##################################################################


			//
			//Create alignments plots using TeXshade
			//


			//			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			//			
			//			Date startDate = new Date();
			//			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			//			
			//			int start = Integer.parseInt(args[0]);
			//			int length = Integer.parseInt(args[1]); 
			//			String alignmentDir = args[2];
			//			String outDir = args[3];
			//			
			//						
			//			createTexshadePDFFiles(start, length, alignmentDir, outDir);
			//			
			//			Date endDate = new Date();
			//			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");


			//##################################################################


			//			String logDir = "/home/proj/Camps3/log/fullAlignmentsPlots/";
			//			checkLogFiles(logDir);

			//			String plotDir = "/scratch/sneumann/Camps3/fullAlignmentsPlots/";
			//			checkPlotFiles(plotDir);


			//##################################################################


			//			String logFile = "/scratch/sneumann/Camps3/fullAlignmentsPlots/missingPlotsAfterFirstRun.txt";
			//			createMissingTexshadePDFFiles("/scratch/sneumann/Camps3/fullAlignmentsPlots/", logFile);

			//##################################################################


			//			String logFile = "/scratch/sneumann/Camps3/fullAlignmentsPlots/missingPlotsAfterSecondRun.txt";
			//			createMissingTexshadePDFFiles("/scratch/sneumann/Camps3/fullAlignmentsPlots/", logFile);

			//##################################################################

			//			createTMSAnnotationFiles4Jalview("/webclu/w3/htdocs/download/camps2.0/alignments/");

			//##################################################################

			//			createCoreAnnotationFiles4Jalview("/webclu/w3/htdocs/download/camps2.0/alignments/");

			//##################################################################

			//createPfamAnnotationFiles4Jalview("/webclu/w3/htdocs/download/camps2.0/alignments/");

		} catch(Exception e) {
			e.printStackTrace();
		}	

	}	

}
