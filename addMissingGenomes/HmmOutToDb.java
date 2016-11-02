package addMissingGenomes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utils.DBAdaptor;

public class HmmOutToDb {

	/**
	 * @param args
	 * The missing genomes(viruses) were added by fetching all the sequences from previous run
	 * but not in re-run and passing them through all possible HMMs and classifying them to the
	 * highest scoring HMM. 
	 * The results were saved in file: 
	 * This class reads that file and puts those sequences into table clusters_mcl2.
	 * In order to mark these sequences, the post_meta column is made. For the newly added sequences
	 * from missing genomes. The flag is set to yes and would also have redundant as NULL.  
	 * 
	 */
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4"); //changed to test_camps3
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String hmmOutfile = "F:/Scratch/addMissingGenomes/run/hmmOut.txt";
		run(hmmOutfile);
	}

	private static void run(String hmmOutfile) {
		// TODO Auto-generated method stub
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(hmmOutfile)));
			String l = "";
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("" +
					"select sequenceid from clusters_mcl2 where cluster_id=? and cluster_threshold=?");
			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO clusters_mcl2 " +
							"(cluster_id,cluster_threshold,sequenceid,post_meta) " +
							"VALUES " +
					"(?,?,?,?)");
			
			while((l=br.readLine())!=null){
				if(!l.startsWith("#") && !l.contains("NO_HMM_FOUND")){
					String[] p = l.split("\t");
					//#SeqId 	 taxId 	 HMM 	 Score
					//2737462	94290	5.0_2335	-2.942717619329621
					int seqid = Integer.parseInt(p[0].trim());
					
					String[] hmm = p[2].split("_");
					float thresh = Float.parseFloat(hmm[0].trim());
					int clusid = Integer.parseInt(hmm[1].trim());
					pstm1.setInt(1, clusid);
					pstm1.setFloat(2, thresh);
					ResultSet rs = pstm1.executeQuery();
					int idsIntble = -1;
					boolean found = false;
					while(rs.next()){
						idsIntble = rs.getInt(1);
						if (idsIntble == seqid){
							found = true;
							rs.close();
							break;
						}
					}
					rs.close();
					pstm1.clearParameters();
					
					if(found){ // already in cluster...
						System.err.println(seqid+"\t"+p[2]+"\t"+"Already in table");
					}
					else{
						pstm2.setInt(1, clusid);
						pstm2.setFloat(2, thresh);
						pstm2.setInt(3, seqid);
						pstm2.setString(4, "Yes");
						
						pstm2.execute();
						System.out.println(seqid+"\t"+thresh+"\t"+clusid+"\t"+"Added to table");
						pstm2.clearBatch();
						pstm2.clearParameters();
					}
				}
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
