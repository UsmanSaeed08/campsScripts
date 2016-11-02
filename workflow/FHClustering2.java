package workflow;

//import java.io.File;
//import java.io.FileWriter;
//import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.Hashtable;

import utils.DBAdaptor;


/*
 * Sub-Clustering of SC-Clusters based on domain architecture cluster assignments (daa). 
 * This version differs from FHClustering in that:
 * 
 * - not the subclusters of the respective SC-clusters are considered
 * - but the sequences with their daa directly
 * - and the FH-clusters are found by dividing the corresponding SC-cluster
 *   according to the assignments (so that each FH-cluster correspond to one
 *   daa)
 * - by doing so, we avoid that two or more FH-clusters have the same daa 
 *   (which is the case for version FH-Clustering)
 */
public class FHClustering2 {
	
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	private static final double SIGNATURE_THRESHOLD = 70;	
	
	
	public static void run() {
		try {
			int batchSize = 50;	
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid FROM clusters_mcl WHERE cluster_id=? AND cluster_threshold=?");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("SELECT name FROM da_cluster_assignments WHERE sequenceid=?");
			
			
			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO fh_clusters "+
					"(code, sequenceid) " +
					"VALUES " +
					"(?,?)");
			
			PreparedStatement pstm5 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO cp_clusters " +
					"(code, description, super_cluster_id, super_cluster_threshold, type) " +
					"VALUES " +
					"(?,?,?,?,?)");
			
			
						
			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT cluster_id, cluster_threshold,code FROM cp_clusters WHERE type=\"sc_cluster\"");
				
			int totalFHCluster = 0;
			
			int statusCounter = 0;
			int clusterCounter = 0;
			
			int batchCounter1 = 0;
			int batchCounter2 = 0;
			
			while(rs.next()) {
				
				clusterCounter++;
				
				statusCounter ++;
				if (statusCounter % 100 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter % 1000 == 0) {
					System.out.write('\n');
					System.out.flush();
				}
				
				int clusterID = rs.getInt("cluster_id");
				float clusterThreshold = rs.getFloat("cluster_threshold");
				String scCode = rs.getString("code");
				
				
				//
				//get members
				//
				BitSet scMembers = new BitSet();
				pstm1.setInt(1, clusterID);
				pstm1.setFloat(2, clusterThreshold);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					int sequenceid = rs1.getInt("sequenceid");
					scMembers.set(sequenceid);
				}
				rs1.close();
				
				
				//
				//get domain architecture cluster assignments (InterPro)
				//
				Hashtable<String,BitSet> assignment2members = new Hashtable<String,BitSet>();
				for(int sequenceid = scMembers.nextSetBit(0); sequenceid>=0; sequenceid = scMembers.nextSetBit(sequenceid+1)) {
					
					pstm2.setInt(1,sequenceid);
					ResultSet rs2 = pstm2.executeQuery();
					while(rs2.next()) {
						String assignment = rs2.getString("name");
						
						BitSet members = new BitSet();
						if(assignment2members.containsKey(assignment)) {
							members = assignment2members.get(assignment);
						}
						
					    members.set(sequenceid);
					    assignment2members.put(assignment, members);
					}
					rs2.close();
				}
				
				
				//
				//get representative PFAM domain architecture (=signature) for each assignment
				//
				Hashtable<String,ArrayList<String>> signature2assignments = new Hashtable<String,ArrayList<String>>();
				Enumeration<String> en1 = assignment2members.keys();
				int countNA = 0;
				while(en1.hasMoreElements()) {
					String assignment = en1.nextElement();
					BitSet members = assignment2members.get(assignment);
					
					String representativeSignature = getRepresentativePFAMSignature(members);
					
					if(representativeSignature.equals("NA")) {	//treat individually
						countNA++;
						
						representativeSignature = "NA_"+countNA;						
					}
					
					ArrayList<String> assignments = new ArrayList<String>();
					if(signature2assignments.containsKey(representativeSignature)) {
						assignments = signature2assignments.get(representativeSignature);
					}
					assignments.add(assignment);
					signature2assignments.put(representativeSignature, assignments);
				}
				
				
				//
				//merge current clusters if they have same PFAM signature
				//
				Hashtable<String,BitSet> signature2members = new Hashtable<String,BitSet>();
				Enumeration<String> en2 = signature2assignments.keys();
				while(en2.hasMoreElements()) {
					
					String signature = en2.nextElement();
					
					BitSet joinedMembers = new BitSet();
						
					ArrayList<String> assignments = signature2assignments.get(signature);
					for(String assignment: assignments) {
							
						BitSet members = assignment2members.get(assignment);
						joinedMembers.or(members);
					}
						
					signature2members.put(signature, joinedMembers);										
				}
				
				
				//
				//ignore singletons
				//
				ArrayList<String> removeSignatures = new ArrayList<String>();
				Enumeration<String> en3 = signature2members.keys();
				while(en3.hasMoreElements()) {
					String signature = en3.nextElement();
					BitSet members = signature2members.get(signature);
					int size = members.cardinality();
					
					if(size == 1) {
						removeSignatures.add(signature);
					}
				}
				
				for(String signature: removeSignatures) {
					signature2members.remove(signature);
				}
				
				
				
				totalFHCluster += signature2members.size();
				
				
				//
				//subdivide according to daa
				//
				int countFH = 0;
				while(!signature2members.isEmpty()) {
					
					//get largest subcluster
					int max = Integer.MIN_VALUE;
					String signatureMax = null;
					
					Enumeration<String> signatures = signature2members.keys();
					while(signatures.hasMoreElements()) {
						String signature = signatures.nextElement();
						BitSet members = signature2members.get(signature);
						int size = members.cardinality();
										
						
						if(size > max) {
							max = size;
							signatureMax = signature;
						}
					}
					
					countFH++;
					
					String suffix = String.valueOf(countFH);
					while(suffix.length()<3) {
						suffix = "0"+suffix;
					}
					
					String subCode = scCode+"_FH"+suffix;
					String subDescription = null;	//set description to PFAM signature with score information
										
					
					BitSet members = signature2members.get(signatureMax);
					
					String signatureWithScore = getPFAMSignature(members);
					if(signatureWithScore.equals("NA")) {
						subDescription = "NA";
					}
					else {
						
						String[] strList = signatureWithScore.split("#");
						double coverage = 100 * Double.parseDouble(strList[1]);
						double score = Double.parseDouble(strList[2]);
						
						BigDecimal bd1 = new BigDecimal(coverage);
						bd1 = bd1.setScale(2, BigDecimal.ROUND_HALF_UP);
						double coverageRounded = bd1.doubleValue();
						
						BigDecimal bd2 = new BigDecimal(score);
						bd2 = bd2.setScale(2, BigDecimal.ROUND_HALF_UP);
						double scoreRounded = bd2.doubleValue();
						
						subDescription = strList[0]+" [Score: "+scoreRounded+", Coverage: "+coverageRounded+"]";
					}
					
					for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {
						
						batchCounter1++;
						
						pstm4.setString(1, subCode);
						pstm4.setInt(2, sequenceid);
						
						pstm4.addBatch();
						
						if(batchCounter1 % batchSize == 0) {								
							pstm4.executeBatch();
							pstm4.clearBatch();
						}					
					}
					
					
					batchCounter2++;
					
					pstm5.setString(1, subCode);
					pstm5.setString(2, subDescription);
					pstm5.setInt(3, clusterID);
					pstm5.setFloat(4, clusterThreshold);
					pstm5.setString(5, "fh_cluster");
										
					pstm5.addBatch();
					
					if(batchCounter2 % batchSize == 0) {								
						pstm5.executeBatch();
						pstm5.clearBatch();
					}
					
					
					
					signature2members.remove(signatureMax);
				}				
			}
			rs.close();
			
			stm.close();
			pstm1.close();
			pstm2.close();
			pstm4.executeBatch();	//insert remaining entries
			pstm4.clearBatch();
			pstm4.close();
			pstm5.executeBatch();	//insert remaining entries
			pstm5.clearBatch();
			pstm5.close();
						
			System.err.println("\n\nNumber of FH clusters: " +totalFHCluster);
					
			
		} catch(Exception e) {
			System.err.println("Exception in FHClustering2.run(): " +e.getMessage());
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
	
	
	private static String getRepresentativePFAMSignature(BitSet members) {
		
		String representativeSignature = null;
		
		try {
			
			representativeSignature = "NA";
			
			String signature = getPFAMSignature(members);
			
			if(!signature.equals("NA")) {
				
				String[] strList = signature.split("#");
				
				double sequencesWithAssignment =  100 * Double.parseDouble(strList[1]);
				double coverageAllMembersWithSignature = 100 * Double.parseDouble(strList[2]);
				
				if(sequencesWithAssignment >= SIGNATURE_THRESHOLD && coverageAllMembersWithSignature >= SIGNATURE_THRESHOLD) {
					representativeSignature = strList[0];
				}
			}		
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return representativeSignature;
	}
	
	
	private static String getPFAMSignature(BitSet members) {
		
		String result = null;
		
		try {
			
			result = "NA";
			
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"SELECT accession FROM " +
					"domains_pfam " +
					"WHERE sequenceid=? ORDER BY begin");
			
			//
			//get PFAM domains
			//
			int countMembersWithSignature = 0;
			Hashtable<String,Integer> signature2count = new Hashtable<String,Integer>();
			for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {
				
				String signature = "";
				
				pstm.setInt(1, sequenceid);
				ResultSet rs = pstm.executeQuery();
				while(rs.next()) {
					
					String accession = rs.getString("accession");
					signature += " - "+accession;
										
				}
				rs.close();
				
				if(!signature.isEmpty()) {
					countMembersWithSignature++;
					
					signature = signature.substring(3);
					
					int count = 0;
					if(signature2count.containsKey(signature)) {
						count = signature2count.get(signature).intValue();
					}
					
					count = count+1;
					signature2count.put(signature, Integer.valueOf(count));
				}				
			}
			
			//get signature with most occurrences
			int max = Integer.MIN_VALUE;
			String signatureMax = null;
			
			Enumeration<String> en = signature2count.keys();
			while(en.hasMoreElements()) {
				
				String signature = en.nextElement();
				int count = signature2count.get(signature).intValue();
								
				if(count > max) {
					max = count;
					signatureMax = signature;
				}
			}
			
			if(signatureMax != null) {
			
				double sequencesWithAssignment = countMembersWithSignature/((double) members.cardinality());
				double coverageAllMembersWithSignature = max/((double) countMembersWithSignature);
				
				result = signatureMax +"#"+sequencesWithAssignment+"#"+coverageAllMembersWithSignature;
			}
					
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return result;
	}
	

	
//	public static void createPFAMSignature(String outfile) {
//		try {
//			
//			PrintWriter pw = new PrintWriter(new FileWriter(new File(outfile)));
//			pw.println("#SC-Cluster\tFH-Cluster\tFH-Cluster size\tSignature\tSuperkingdom\tCoverage(all members)\tCoverage (members with assignment)");
//			
//			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
//					"SELECT sequences_sequenceid FROM fh_clusters WHERE code=?");
//			
//			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
//					"SELECT accession,subtype FROM " +
//					"domains_pfam " +
//					"WHERE sequences_sequenceid=? ORDER BY begin");
//			
//			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
//					"SELECT distinct(superkingdom) FROM taxonomies " +
//					"WHERE taxonomyid in " +
//					"(SELECT taxonomyid FROM fh_clusters t1, proteins t2 " +
//					"WHERE t1.code=? AND t1.sequences_sequenceid=t2.sequenceid)");
//			
//			Statement stm = CAMPS_CONNECTION.createStatement();
//			ResultSet rs = stm.executeQuery("SELECT code FROM cp_clusters WHERE type=\"fh_cluster_v2\"");
//			while(rs.next()) {
//				
//				String code = rs.getString("code");
//				String scCluster = code.split("_")[0];
//				
//				BitSet members = new BitSet();
//				
//				pstm1.setString(1, code);
//				ResultSet rs1 = pstm1.executeQuery();
//				while(rs1.next()) {
//					
//					int sequenceid = rs1.getInt("sequences_sequenceid");
//					members.set(sequenceid);
//				}
//				rs1.close();
//				
////				if(members.cardinality() < 8) {
////					continue;
////				}
//				
//				//
//				//get PFAM domains
//				//
//				int countMembersWithSignature = 0;
//				Hashtable<String,Integer> signature2count = new Hashtable<String,Integer>();
//				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {
//					
//					String signature = "";
//					
//					pstm2.setInt(1, sequenceid);
//					ResultSet rs2 = pstm2.executeQuery();
//					while(rs2.next()) {
//						
//						String accession = rs2.getString("accession");
//						String subtype = rs2.getString("subtype");
//						if(subtype.equals("soluble")) {
//							signature += " - "+accession+" [S]";
//						}
//						else if(subtype.equals("transmembrane")) {
//							signature += " - "+accession+" [T]";
//						}
//						else if(subtype.equals("hybrid")) {
//							signature += " - "+accession+" [H]";
//						}
//						else {
//							signature += " - "+accession;
//						}
//						
//					}
//					rs2.close();
//					
//					if(!signature.isEmpty()) {
//						countMembersWithSignature++;
//						
//						signature = signature.substring(3);
//						
//						int count = 0;
//						if(signature2count.containsKey(signature)) {
//							count = signature2count.get(signature).intValue();
//						}
//						
//						count = count+1;
//						signature2count.put(signature, Integer.valueOf(count));
//					}				
//				}
//				
//				//get signature with most occurrences
//				int max = Integer.MIN_VALUE;
//				String signatureMax = null;
//				
//				Enumeration<String> en = signature2count.keys();
//				while(en.hasMoreElements()) {
//					
//					String signature = en.nextElement();
//					int count = signature2count.get(signature).intValue();
//									
//					if(count > max) {
//						max = count;
//						signatureMax = signature;
//					}
//				}
//				
//				double coverageAllMembers = (100*max)/((double) members.cardinality());
//				double coverageAllMembersWithSignature = (100*max)/((double) countMembersWithSignature);
//				
//				
//				String superkingdomString = "";
//				pstm3.setString(1, code);
//				ResultSet rs3 = pstm3.executeQuery();
//				while(rs3.next()) {
//					superkingdomString += ","+rs3.getString("superkingdom");
//				}
//				rs3.close();
//				superkingdomString = superkingdomString.substring(1);
//				
//				pw.println(scCluster+"\t"+code+"\t"+members.cardinality()+"\t"+signatureMax+"\t"+superkingdomString+"\t"+coverageAllMembers+"\t"+coverageAllMembersWithSignature);
//			}
//			stm.close();
//			rs.close();
//			
//			pstm1.close();
//			pstm2.close();
//			pstm3.close();
//			
//			pw.close();
//			
//		}catch(Exception e) {
//			e.printStackTrace();
//		}finally {
//			if (CAMPS_CONNECTION != null) {
//				try {
//					CAMPS_CONNECTION.close();					
//				} catch (SQLException e) {					
//					e.printStackTrace();
//				}
//			}
//		}
//	}
	
	
//	public static void main(String[] args) {
//		
//		createPFAMSignature("/home/users/sneumann/PHD/CAMPS3/pfamsignatures4FHclusters_061210.txt");
//	}
}
