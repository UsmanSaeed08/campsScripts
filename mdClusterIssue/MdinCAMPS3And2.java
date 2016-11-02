package mdClusterIssue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import utils.DBAdaptor;

public class MdinCAMPS3And2 {

	/**
	 * @param args
	 * The class checks, how many md clusters in camps2 are sc clusters and given the file.. calculates same for camps3
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Running...");
		String mdCLusFile = "F:/MDClusterReportReRun_Test12.txt";
		//String mdCLusFile = "F:/MDClusterReportReRun_Test11.txt"; // 35-75 and low identity 15
		//String mdCLusFile = "F:/MDClusterReportReRun_Test7.txt"; //identity 30 and occ 70
		scThatareMdFile(mdCLusFile);
		
		scThatareMdCAMPS2();
	}
	private static void scThatareMdCAMPS2(){
		try{
			HashMap<String,String> sc = new HashMap<String,String>();
			
			Connection CAMPS2_CONNECTION = DBAdaptor.getConnection("CAMPS3");
			PreparedStatement pstm = CAMPS2_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters where type=?");
			pstm.setString(1, "sc_cluster");
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				Integer id = rs.getInt(1);
				Float thres = rs.getFloat(2);
				String k = id.toString()+"_"+thres.toString();
				sc.put(k, "");
			}
			rs.clearWarnings();
			
			int count = 0;
			pstm.setString(1, "md_cluster");
			rs = pstm.executeQuery();
			while(rs.next()){
				Integer id = rs.getInt(1);
				Float thres = rs.getFloat(2);
				String k = id.toString()+"_"+thres.toString();
				if(sc.containsKey(k)){
					count ++;
					System.out.println(k);
				}
			}
			rs.close();
			pstm.close();
			System.out.println("sc clusters that are also md clusters in CAMPS2 are: "+count);
			System.out.println("number of sc clusters: "+sc.size());
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void scThatareMdFile(String f){
		try{
			int count = 0; 
			//mdClusterID+"\t"+mdClusterThreshold+"\t"+CurrentclusSize+"\t"+this.clusterID+"\t"+this.clusterThreshold);
			BufferedReader br = new BufferedReader(new FileReader(new File(f)));
			String l = "";
			while((l=br.readLine())!=null){
				String[] parts = l.split("\t");
				String md = parts[0].trim()+"_"+parts[1].trim();
				String sc = parts[3].trim()+"_"+parts[4].trim();
				if(md.contains(sc)){
					count++;
					System.out.println(l);
				}
			}
			br.close();
			System.out.println("sc clusters that are also md clusters are: "+count);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
