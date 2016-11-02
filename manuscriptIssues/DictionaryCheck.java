package manuscriptIssues;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import utils.DBAdaptor;
import workflow.Dictionary3;

public class DictionaryCheck {

	/**
	 * @param args
	 */
	private static HashMap<Integer, Dictionary3> dict = new HashMap<Integer, Dictionary3>();
	static boolean dictbool = false;
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public static void populatedictionary(){
		try{
			Connection CAMPS_CONNECTION_local = DBAdaptor.getConnection("camps4");
			PreparedStatement pstm = null;
			if(dictbool == false){
				int idx =0;
				//for (int i=1; i<=7;i++){		//-- fix loop size -- fixed
				for (int i=1; i<=70;i++){		//-- fix loop size -- fixed		
					//pstm = CAMPS_CONNECTION_local.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments2 limit "+idx+","+100000000);
					pstm = CAMPS_CONNECTION_local.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments2 limit "+idx+","+10000000);
					//pstm = CAMPS_CONNECTION.prepareStatement("SELECT seqid_query, seqid_hit,identity FROM alignments_initial");
					//idx = i*10000000;
					idx = i*1000000;
					System.out.print("\n Hashtable of Alignments In Process "+idx+"\n");
					ResultSet rs = pstm.executeQuery();
					while(rs.next()) {
						int b = rs.getInt("seqid_hit");
						int key = rs.getInt ("seqid_query");
						float c = rs.getFloat("identity");
						
						int key2 = b;
						int b2 = key;						
						/*
						if (dict.containsKey(key)){ // the key already doesnt exist...so that you dont delete the previous info of the key
							Dictionary temp = (Dictionary)dict.get(key);
							temp.set(idhit, identity);
							dict.put(key,temp);
						}
						else{
							Dictionary temp = new Dictionary(idhit, identity);
							dict.put(key,temp);
						}*/
						// New dictionary to go the other way round... hits as keys
						if (dict.containsKey(key)){ // the key already doesnt exist...so that you dont delete the previous info of the key
							Dictionary3 temp = (Dictionary3)dict.get(key);
							temp.set(b, c);
							dict.put(key,temp);
						}
						else{
							Dictionary3 temp = new Dictionary3(b,c);
							dict.put(key,temp);
						}
						// now add in reverse direction
						if (dict.containsKey(key2)){ // the key already exists...so that you dont delete the previous info of the key
							//LEAVING THIS EMPTY MEANS UR DATA IS SYMMETRICAL
							Dictionary3 temp = (Dictionary3)dict.get(key2);
							temp.set(b2, c);
							dict.put(key2,temp);
						}
						else{
							Dictionary3 temp = new Dictionary3(b2,c);
							dict.put(key2,temp);
						}
					}
					// dict population complete
					rs.close();
				}
				dictbool = true;
				pstm.close();
				CAMPS_CONNECTION_local.close();
			}
			System.out.print("\n Hashtable of Alignments Complete\n");
			//System.out.print(" Making Matrix Now\n");
			//System.out.print(" Size of Candidates "+candidates.size()+"\n");
			System.out.print(" Size of hashtable "+dict.size()+"\n");
			System.out.flush();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}






}
