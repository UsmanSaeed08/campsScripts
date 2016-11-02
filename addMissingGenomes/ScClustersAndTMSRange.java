package addMissingGenomes;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import utils.DBAdaptor;

public class ScClustersAndTMSRange {

	/**
	 * @param args
	 * makes the file scClustersAndTMSRange for the reRun HMMs
	 * 
	 */

	/**
	 * @param args
	 */
	private static final Connection connection = DBAdaptor.getConnection("camps4");

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.print("XxX\n");
		String file_name = "F:/Scratch/addMissingGenomes/run/scClustersAndTMSRangeReRun.txt";
		String pathtohmms = "F:/SC_Clust_postHmm/MetaModelsJune2016/HMMs/CAMPS4_1/";
		makeFile(pathtohmms,file_name);

	}

	private static void makeFile(String pathToHmms, String file_name) {
		// TODO Auto-generated method stub
		try{
			File folder = new File(pathToHmms);
			File[] listOfFiles = folder.listFiles();
			ArrayList<String> files = new ArrayList<String>();
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(file_name)));
			//PreparedStatement pstmGetSc = connection.prepareStatement("SELECT code,cluster_id,cluster_threshold from cp_clusters where type=\"sc_cluster\"");
			PreparedStatement pstmGetTmh = connection.prepareStatement("SELECT tms_range from clusters_mcl_nr_info2 where" +
					" cluster_id=? and cluster_threshold=?");

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

			for (int i =0; i <=files.size()-1;i++){
				String f = files.get(i);
				//cluster_5.0_0.hmm
				//cluster_5.0_46.hmm
				int idx = f.indexOf(".hmm");
				f = f.substring(0,idx);
				System.out.print(f+"\n");
				String Fp[] = f.split("_");
				String threshStr = Fp[1];
				String clusid = Fp[2];
				
				int id = Integer.parseInt(clusid);
				float thresh = Float.parseFloat(threshStr);

				pstmGetTmh.setInt(1, id);
				pstmGetTmh.setFloat(2, thresh);
				ResultSet rsTmh = pstmGetTmh.executeQuery();
				String tmh = "";
				String code = f.split("cluster_")[1].trim();//"notAssigned";
				while(rsTmh.next()){
					tmh = rsTmh.getString(1);
				}
				rsTmh.close();
				// write to file below
				bw.write(code+"\t"+tmh.trim()+"\t"+id+"\t"+thresh);
				System.out.print(code+"\t"+tmh+"\t"+id+"\t"+thresh+"\n");
				bw.newLine();
			}
			bw.close();
			pstmGetTmh.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}


}
