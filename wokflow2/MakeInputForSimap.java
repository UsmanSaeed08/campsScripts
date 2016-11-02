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

import utils.DBAdaptor;

public class MakeInputForSimap {

	/**
	 * @param args
	 */

	//private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	private static ArrayList<String> ToDoSqArray = new ArrayList<String>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("hey");

		String pathTomapped = "/home/users/saeed/scratch/mappingCampsToCATHandPDB/Camps2PDBmap.txt";
		String pathToSeqInSC = "/home/users/saeed/scratch/mappingCampsToCATHandPDB/allSequeces.txt";

		ReadFile(pathTomapped,pathToSeqInSC);
		String pathtoWriteInputFiles = "/home/users/saeed/scratch/mappingCampsToCATHandPDB/SimapInput";
		MakeNewInputFile(pathtoWriteInputFiles);


	}

	private static void MakeNewInputFile(String pathtoWriteInputFiles) {
		// TODO Auto-generated method stub
		try{
			// 125521 - done excluding not found
			// 275189 - total
			int fNo = 0;
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathtoWriteInputFiles+"_"+fNo)));
			int size = ToDoSqArray.size();
			System.out.println("Sequences to process: "+ size);
			for (int i = 0; i<= size-1 ; i++){
				if(i%10000 == 0){
					bw.flush();
					bw.close();
					fNo = fNo + 1;
					bw = new BufferedWriter(new FileWriter(new File(pathtoWriteInputFiles+"_"+fNo)));
				}
				bw.write(ToDoSqArray.get(i));
				bw.newLine();
			}
			bw.flush();
			bw.close();
			System.out.println("Number of files Written "+fNo);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}


	private static void ReadFile(String pathToMapped, String pathToSeqInSC) {
		// TODO Auto-generated method stub
		try{
			// Get the Done sequences
			HashMap <String, String> DoneSq = new HashMap<String,String>();
			ArrayList<String> DoneSq_array = new ArrayList<String>();

			HashMap <String, String> ToDoSq = new HashMap<String,String>();
			BufferedReader br = new BufferedReader(new FileReader(new File(pathToMapped)));
			String line = "";
			while((line = br.readLine())!=null){
				String parts[] = line.split("\t");
				String sqid = parts[0];
				sqid = sqid.trim();
				if(!DoneSq.containsKey(sqid)){
					DoneSq.put(sqid, "");
					DoneSq_array.add(sqid);
				}

			}
			br.close();

			System.out.println("Number of Done Seq "+ DoneSq.size());
			// get the sequeces not done ---> TotalInSC - Done Seq = Not Done Seq
			//pathToSeqInSC
			BufferedReader br2 = new BufferedReader(new FileReader(new File(pathToSeqInSC)));
			String l2 = "";
			while((l2 = br2.readLine())!=null){
				String parts[] = l2.split("\t");
				String sqid = parts[1].trim();
				if(!DoneSq.containsKey(sqid)){
					if(!ToDoSq.containsKey(sqid)){
						ToDoSq.put(sqid, "");
						ToDoSqArray.add(sqid);
					}
				}
			}
			br2.close();
			System.out.println("Number of Seq to do "+ ToDoSq.size());
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
