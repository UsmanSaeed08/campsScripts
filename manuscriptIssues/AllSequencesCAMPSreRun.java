package manuscriptIssues;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utils.DBAdaptor;

public class AllSequencesCAMPSreRun {

	/**
	 * @param args
	 */
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Running...");
		String fi = "F:/Scratch/mappingCampsToCATHandPDB/allSequences2.txt";
		run(fi);
	}

	private static void run(String filetoMake) {
		// TODO Auto-generated method stub
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(filetoMake)));
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold,code from cp_clusters2 where type=\"sc_cluster\"");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_id=? and cluster_threshold=?");
			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				int clusid = rs1.getInt(1);
				float thresh  = rs1.getFloat(2);
				String code = rs1.getString(3);
				
				pstm2.setInt(1, clusid);
				pstm2.setFloat(2, thresh);
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()){
					int seqid = rs2.getInt(1);
					bw.write(code+"\t"+seqid+"\t"+clusid+"\t"+thresh);
					bw.newLine();
					System.out.println(code+"\t"+seqid+"\t"+clusid+"\t"+thresh);
				}
				rs2.close();
				pstm2.clearParameters();
			}
			rs1.close();
			
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
