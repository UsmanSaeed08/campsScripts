package mdClusterIssue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import utils.DBAdaptor;

public class OldReRun {

	/**
	 * @param args
	 */
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	private static final double MIN_PERCENTAGE_IDENTITY = 30;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*
			@param dbname	- name of the MySQL database
		 * @param table		- name of the table 
		 * @param columns	- set of column names the index will be build on
		 * @param indexName	- name of the index
		 */
		SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
		Date startDate = new Date();
		System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
		//DBAdaptor.createIndex("camps4","alignments_initial",new String[]{"seqid_query","seqid_hit","identity"},"alignmentsindex1");
		makeALignmentsTableFile();
		Date endDate = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
	}

	private static void makeALignmentsTableFile() {
		// TODO Auto-generated method stub
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/Camps4AlignmentsTableGreaterThan30")));
			Statement stm2 = CAMPS_CONNECTION.createStatement();
			stm2.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs2 = stm2.executeQuery("SELECT seqid_query,seqid_hit,identity from alignments_initial");
			int records =0;
			while(rs2.next()) {
				records++;
				int sequenceIDQuery = rs2.getInt("seqid_query");
				int sequenceIDHit = rs2.getInt("seqid_hit");				
				float identity = 100* rs2.getFloat("identity");
				if(identity >= MIN_PERCENTAGE_IDENTITY) {
					bw.write(sequenceIDQuery+"\t"+sequenceIDHit+"\t"+identity);
					bw.newLine();
				}
				if(records%10000 == 0){
					System.out.println(records);
				}
			}
			rs2.close();
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
