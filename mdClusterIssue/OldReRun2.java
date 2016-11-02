package mdClusterIssue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import utils.BitMatrix;
import utils.DBAdaptor;

public class OldReRun2 {
	
private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	private static final double MIN_PERCENTAGE_IDENTITY = 30;
	
	private static final double MIN_PERCENTAGE_OCCURRENCES = 70;
	
	//cluster id of SC-cluster for which MD-clusters should be calculated
	private int clusterID;			
	//cluster threshold of SC-cluster for which MD-clusters should be calculated
	private float clusterThreshold; 
	//matrix containing information whether pair of sequences share at least 30% sequence identity
	private BitMatrix matrix;	
	//mapping between sequence ids and indices in matrix
	private int[] indizes;
	
	private String abcFil = "";
	private int members = 0;
		
	
	public OldReRun2(int clusterID, float clusterThreshold) {
		this.clusterID = clusterID;
		this.clusterThreshold = clusterThreshold;
	}

	
	public void run() {
		try {		
			
			//int batchSize = 50;	
			int batchSize = 1;
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT child_cluster_id, child_cluster_threshold, intersection_size FROM clusters_mcl_track2 WHERE cluster_id=? AND cluster_threshold=?");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT sequences FROM clusters_mcl_nr_info2 WHERE cluster_id=? AND cluster_threshold=?");
			
//			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
//					"INSERT INTO cp_clusters " +
//					"(cluster_id, cluster_threshold, super_cluster_id, super_cluster_threshold, type) " +
//					"VALUES " +
//					"(?,?,?,?,?)");
						
							
			int countMDCluster = 0;			
			int batchCounter = 0;
							
				
			String cluster = clusterID + "#" + clusterThreshold;
			
			System.out.println("In progress: " +cluster);
							
			//
			//store information whether pairs of sequences have identity>=MIN_PERCENTAGE_IDENTITY
			//in triangle bitset matrix
			//
			System.out.println("Setting Matrix");
			setMatrix(clusterID, clusterThreshold,this.abcFil);
			System.out.println("MatrixSet");
			ArrayList<String> clusters = new ArrayList<String>();
			clusters.add(cluster);
			
			while(!clusters.isEmpty()) {
				
				String cluster2Test = clusters.get(0);		
				
				if(isMDCluster(cluster2Test)) {
					
					countMDCluster++;
					
					int mdClusterID = Integer.parseInt(cluster2Test.split("#")[0]);
					float mdClusterThreshold = Float.parseFloat(cluster2Test.split("#")[1]);
					
					System.err.println("\tMD cluster: "+cluster2Test + "\t"+members);
					
					//write to db
					
					//pstm.set ...
					batchCounter++;
					
//						pstm3.setInt(1, mdClusterID);
//						pstm3.setFloat(2, mdClusterThreshold);
//						pstm3.setInt(3, clusterID);
//						pstm3.setFloat(4, clusterThreshold);
//						pstm3.setString(5, "md_cluster");
//																				
//						pstm3.addBatch();
//						
//						if(batchCounter % batchSize == 0) {								
//							pstm3.executeBatch();
//							pstm3.clearBatch();
//						}
					
					
					clusters.remove(0);
				}
				else {
					
					clusters.remove(0);
					
					int clusterID2 = Integer.parseInt(cluster2Test.split("#")[0]);
					float clusterThreshold2 = Float.parseFloat(cluster2Test.split("#")[1]);
					
					pstm1.setInt(1, clusterID2);
					pstm1.setFloat(2, clusterThreshold2);
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {
						
						int childClusterID = rs1.getInt("child_cluster_id");
						float childClusterThreshold = rs1.getFloat("child_cluster_threshold");
						int intersectionSize = rs1.getInt("intersection_size");
						
						int numberMembers = 0;
						pstm2.setInt(1, childClusterID);
						pstm2.setFloat(2, childClusterThreshold);
						ResultSet rs2 = pstm2.executeQuery();
						while(rs2.next()) {
							numberMembers = rs2.getInt("sequences");
						}
						rs2.close();
						
//							//ignore singletons
//							if(numberMembers == 1) {
//								continue;
//							}
						
						double percCoverage = 100 * ((double) intersectionSize/numberMembers);
						
																			
						if(percCoverage >= 90) {							
							String childCluster = childClusterID + "#" + childClusterThreshold;
							clusters.add(childCluster);
							//System.err.println("\t\t### "+childClusterID+"#"+childClusterThreshold+"\t"+percCoverage);
						}
						
					}
					rs1.close();
					
				}
			}			
				
						
			
			pstm1.close();
			pstm2.close();
//			pstm3.executeBatch();	//insert remaining entries
//			pstm3.clearBatch();
//			pstm3.close();
						
			System.out.println("\n\nNumber of MD clusters: " +countMDCluster);
					
			
		} catch(Exception e) {
			System.err.println("Exception in MDClustering2.run(): " +e.getMessage());
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
	
	
	private void setMatrix(int clusterID, float clusterThreshold,String abcFile) {
		try {
			
			BitSet members = new BitSet();
			int maxSequenceid = Integer.MIN_VALUE;			
			
			//
			//get members
			//
			Statement stm1 = CAMPS_CONNECTION.createStatement();
			ResultSet rs1 = stm1.executeQuery("SELECT sequenceid FROM clusters_mcl2 WHERE cluster_id="+clusterID+" and cluster_threshold="+clusterThreshold);
			while(rs1.next()) {
				
				int sequenceid = rs1.getInt("sequenceid");
				members.set(sequenceid);
				
				if(sequenceid > maxSequenceid) {
					maxSequenceid = sequenceid;
				}
			}
			rs1.close();
			rs1 = null;
			stm1.close();
			stm1 = null;
			
						
			this.indizes = new int[maxSequenceid+1];
			
			int index = 0;
			for(int id = members.nextSetBit(0); id>=0; id = members.nextSetBit(id+1)) {
				
				indizes[id] = index; 
				index++;
			}
			
			
			int matrixSize = members.cardinality();
			this.matrix = new BitMatrix(matrixSize);
			
			//
			//get identities and save information if identity >= MIN_PERCENTAGE_IDENTITY
			//in bitset matrix (only use right upper part of matrix, since identity values
			//are symmetric)
			//
			
			//Statement stm2 = CAMPS_CONNECTION.createStatement();
			//stm2.setFetchSize(Integer.MIN_VALUE);
			//ResultSet rs2 = stm2.executeQuery("SELECT seqid_query,seqid_hit,identity from alignments2");
			// writing below file to make a test case so i dont have to read from table again and again... once done it will be commented out
			//BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/testAlgn_CMSC0702")));
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/testAlgn_CMSC0001")));
			
			BufferedReader br = new BufferedReader(new FileReader(new File(abcFile))); 
			String line = "";
			int lineNo = 0;
			while((line=br.readLine())!=null){
			//while(rs2.next()) {
				
				//int sequenceIDQuery = rs2.getInt("seqid_query");
				//int sequenceIDHit = rs2.getInt("seqid_hit");				
				//float identity = 100* rs2.getFloat("identity");****
				//float identity = rs2.getFloat("identity");
				
				lineNo++;
				String[] p = line.split(" ");
				int sequenceIDQuery = Integer.parseInt(p[0].trim());
				int sequenceIDHit = Integer.parseInt(p[1].trim());
				float identity = 0f;
				try{
					identity = Float.parseFloat(p[2]);
				}
				catch(NumberFormatException e){
					identity = 101;
				}
				if(members.get(sequenceIDQuery) && members.get(sequenceIDHit)) {
					
					int rowIndex = indizes[sequenceIDQuery];
					int colIndex = indizes[sequenceIDHit];
					
					if(identity >= MIN_PERCENTAGE_IDENTITY) {
						matrix.set(rowIndex, colIndex);
						
						bw.write(sequenceIDQuery+" "+sequenceIDHit+" "+identity);
						bw.newLine();
					}					
				}
			}
			bw.close();
			br.close();
			System.out.println("Matrix Made");
			
			//rs2.close();
			//rs2 = null;
			//stm2.close();
			//stm2 = null;
			
		}catch(Exception e) {
			System.err.println("Exception in MDClustering2.createMatrix(): " +e.getMessage());
			e.printStackTrace();
			
		}
	}
	
	
	private boolean isMDCluster(String cluster) {
		boolean isMDCluster = false;
					
		try {
			
			int clusterID = Integer.parseInt(cluster.split("#")[0]);
			float clusterThreshold = Float.parseFloat(cluster.split("#")[1]);
			
			BitSet sequenceIDs = new BitSet();
			
			//get cluster members
			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT sequenceid FROM clusters_mcl2 WHERE cluster_id="+clusterID+" and cluster_threshold="+clusterThreshold);
			while(rs.next()) {
				
				int sequenceID = rs.getInt("sequenceid");
				sequenceIDs.set(sequenceID);
				
			}
			rs.close();
			rs = null;
			stm.close();
			
			int numMembers = sequenceIDs.cardinality();
			
			int numValidMembers = 0;
			//double threshold = (numMembers * MIN_PERCENTAGE_OCCURRENCES)/((double) 100); //!!! Check: isn't (numMembers-1) correct here?!
			int threshold = (int) Math.round(((numMembers-1) * MIN_PERCENTAGE_OCCURRENCES)/((double) 100)); 
							
			
			int counter = 0;
			
			for(int sequenceID1 = sequenceIDs.nextSetBit(0); sequenceID1>=0; sequenceID1 = sequenceIDs.nextSetBit(sequenceID1+1)) {
				
				
				counter++;
				
				counter ++;
				if (counter % 100 == 0) {
					System.err.write('.');
					System.err.flush();
				}
				if (counter % 10000 == 0) {
					System.err.write('\n');
					System.err.flush();
				}
				
				int countValidPairs = 0;
				
				
				for(int sequenceID2 = sequenceIDs.nextSetBit(0); sequenceID2>=0; sequenceID2 = sequenceIDs.nextSetBit(sequenceID2+1)) {
					
					
					int rowIndex;
					int colIndex;
					
					if(sequenceID1 == sequenceID2) {
						continue;
					}
					else if(sequenceID1 < sequenceID2) {
						
						rowIndex = this.indizes[sequenceID1];
						colIndex = this.indizes[sequenceID2];
					}
					else {
						
						rowIndex = this.indizes[sequenceID2];
						colIndex = this.indizes[sequenceID1];
					}
					
					//if(this.matrix.get(rowIndex,colIndex)) {
					if(this.matrix.get(rowIndex,colIndex) || this.matrix.get(colIndex,rowIndex)) {
						countValidPairs++;
					}
				}
				
				if(countValidPairs >= threshold) {
					numValidMembers++;
				}								
			}
			
			
			if(numMembers == numValidMembers) {
				isMDCluster = true;
				members = numMembers;
			}
			else {
				System.err.println("\t\tIgnore: " +cluster+"\t"+numMembers +"\t"+numValidMembers);
			}
			
						
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		return isMDCluster;
		
	}
	
	
	/*
	 * Checks if error files from running MD-clustering jobs are o.k.
	 */
	public static void checkJobErrorFiles(String dir) {
		
		try {
			
			Pattern p = Pattern.compile("mdc2\\_\\d+\\.e\\d+");
			
			File[] files = new File(dir).listFiles();
			
			int countFiles = 0;
			int countCorrectFiles = 0;
			
			for(File file: files) {
				
				Matcher m = p.matcher(file.getName());
				
				if(!m.matches()) {
					continue;
				}
				
				countFiles++;
				boolean isCorrectFile = true;
				
				System.out.println("Checking " +file.getName()+" ...");
				
				BufferedReader br = new BufferedReader(new FileReader(file));
				String line;
				while((line=br.readLine()) != null) {
					
					line = line.trim();
					
					if(line.contains("In progress:")) {
						continue;
					}
					else if(line.contains("MD cluster:")) {
						continue;
					}
					else if(line.contains("###")) {
						continue;
					}
					else if(line.contains("Ignore")) {
						continue;
					}
					else if(line.replaceAll("\\.", "").isEmpty()) {
						continue;
					}
					else {
						isCorrectFile = false;
						System.out.println(line);
					}
				}
				br.close();
				
				if(isCorrectFile) {
					countCorrectFiles++;
				}
			}
			
			
			System.out.println();
			System.out.println("Number of files: " +countFiles);
			System.out.println("Number of correct files: " +countCorrectFiles);
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
	}
	
		
	/*
	 * Checks if output files from running MD-clustering jobs are o.k.
	 */
	public static void checkJobOutputFiles(String dir) {
		
		try {
			
			Pattern p = Pattern.compile("mdc2\\_\\d+\\.o\\d+");
			
			Pattern pStart = Pattern.compile("...\\[\\d+:\\d+:\\d+\\]\\s+Start");
			Pattern pDone = Pattern.compile("...DONE\\s+\\[\\d+:\\d+:\\d+\\]");
			Pattern pMD = Pattern.compile("Number of MD clusters:\\s+(\\d+)");
			
			File[] files = new File(dir).listFiles();
			
			int countFiles = 0;
			int countCorrectFiles = 0;
			boolean foundStart = false;
			boolean foundEnd = false;
			
			int countAllMD = 0;
			
			for(File file: files) {
				
				Matcher m = p.matcher(file.getName());
				
				if(!m.matches()) {
					continue;
				}
				
				countFiles++;
				
				
				BufferedReader br = new BufferedReader(new FileReader(file));
				String line;
				while((line=br.readLine()) != null) {
					
					line = line.trim();
					
					Matcher m1 = pStart.matcher(line);
					Matcher m2 = pDone.matcher(line);
					Matcher m3 = pMD.matcher(line);
					
					if(m1.matches()) {
						foundStart = true;
					}					
					else if(m2.matches()) {
						foundEnd = true;
					}					
					else if(m3.matches()) {
						int countMD = Integer.parseInt(m3.group(1));
						countAllMD += countMD;
					}
				}
				br.close();
				
				if(foundStart && foundEnd) {
					countCorrectFiles++;
				}
			}
			
			
			System.out.println();
			System.out.println("Number of files: " +countFiles);
			System.out.println("Number of correct files: " +countCorrectFiles);
			System.out.println("Number of MD-clusters: " +countAllMD);
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	public static void main(String[] args) {
		
		try {
			
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			
			//int clusterID = Integer.parseInt(args[0]);
			//float clusterThreshold = Float.parseFloat(args[1]); 
			
			//
			//TEST: cluster 1931#9
			//int clusterID = 1931;
			//float clusterThreshold = 9;
			
			//
			//TEST: cluster 621#5
			//int clusterID = 621;
			//float clusterThreshold = 5;
			
			//
			//TEST: cluster 419#5
			//int clusterID = 419;
			//float clusterThreshold = 5;
			
			//
			//TEST: cluster 111#5
			//int clusterID = 111;
			//float clusterThreshold = 5;
			
			//
			//TEST: cluster 49#5
			//int clusterID = 49;
			//float clusterThreshold = 5;
			
			//
			//TEST: cluster 4177#26
			//int clusterID = 4177;
			//float clusterThreshold = 26;
			
			//
			//TEST: cluster 7728#40
			
			//int clusterID = 0;
			//float clusterThreshold = 5f;
			
			//int clusterID = 16218;
			//float clusterThreshold = 24f;
			
			//int clusterID = 0;
			//float clusterThreshold = 5f;
			int clusterID = 3;
			float clusterThreshold = 5f;
									
			OldReRun2 md = new OldReRun2(clusterID,clusterThreshold);
			md.abcFil = "F:/SCClusters_alignmentFile.abc";
			//md.abcFil = "F:/testAlgn_CMSC0702";
			md.run();
			
			
			//checkJobErrorFiles("/home/proj/Camps3/log/mdClustering/partBasedOnV2/");
			
			//checkJobOutputFiles("/home/proj/Camps3/log/mdClustering/partBasedOnV2/");
			
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
			
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}	
				
	}
}
