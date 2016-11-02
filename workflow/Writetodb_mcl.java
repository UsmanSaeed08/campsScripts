package workflow;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


import utils.DBAdaptor;

public class Writetodb_mcl {

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	public int clusterid;
	public float currentThresh;
	public String Clusterfile;

	Writetodb_mcl(int c, int ct, String f){
		this.clusterid = c;
		this.currentThresh = ct;
		this.Clusterfile = f;
	}
	
	public Writetodb_mcl(){
		
	}

	public static void Close_connection() {
		// TODO Auto-generated constructor stub
		try {
			//CAMPS_CONNECTION.setAutoCommit(true);
			//CAMPS_CONNECTION.commit();
			CAMPS_CONNECTION.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void run(){

		try{
		BufferedReader reader = new BufferedReader(new FileReader(this.Clusterfile));
		String sCurrentLine;
		PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
				"INSERT INTO clusters_mcl2 " +
						"(cluster_id, cluster_threshold, sequenceid) VALUES " +
				"(?,?,?)");

		while ((sCurrentLine = reader.readLine()) != null){
			float ct_temp = this.currentThresh;
			// every line is a cluster
			//1. split the line for tab to get members of cluster
			//2. insert

			String[] clusteredSequences = sCurrentLine.split("\t");

			for (int i =0 ; i<= clusteredSequences.length-1; i++){	//check if the condition is correct or wrong
				int mem = Integer.parseInt(clusteredSequences[i]);
				pstm1.setInt(1,this.clusterid);	//cluster id	
				//pstm1.setFloat(2, this.currentThresh);		//threshold of current files for which clustering is done
				pstm1.setFloat(2, ct_temp);
				pstm1.setInt(3,mem);				// cluster member

				pstm1.addBatch();	// if confusion then un comment below
				if (i% 1000 == 0){
					pstm1.executeBatch();
					pstm1.clearBatch();
				}
				
			}
			pstm1.executeBatch();
			//pstm1.execute();
			pstm1.clearBatch();
			
			this.clusterid++;	//because increment every cluster
		}
		reader.close();
		pstm1.close();
	}
		catch(Exception e){
			e.printStackTrace();
		}


	}



}
