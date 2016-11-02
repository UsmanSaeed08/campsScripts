package extract_proteins;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;

import utils.DBAdaptor;
import utils.IDMapping;
import extract_proteins.ReadHitsThreadNew2;
import datastructures.TMS;
//import de.gsf.mips.simap.lib.database.DbSequence;

public class ExtractSimilarityScoresNew2 {

	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("test_camps3");
	
	
	
	////////////////////////////////////////////////////////////////////////////
	//																		  //
	//				filter parameters										  //
	//      																  //
	////////////////////////////////////////////////////////////////////////////
	
	//evalue threshold to filter out insecure alignments
	private static final double MAX_EVALUE = 1E-5;
		
	
	private static final float MIN_LENGTH_RATIO = 0.5f;
		
	
	/**
	 * Runs the whole program.
	 * @throws Exception 
	 */
	public void run() throws Exception {
		
		Date a = Calendar.getInstance().getTime();
		System.out.println("\n Extract Similarity Score starts at: " + a + "\n");
		
		
			
			a = Calendar.getInstance().getTime();
			System.out.println("\n ID Mapping complete now get TMS coordinates: " + a + "\n");
			
			
			// NO TMS coordinated in array!!! Runtime Access!!! Process intensive but memory saving
			
			System.out.println("\t\t[INFO]: Get TMS coordinates for all sequences.");
			
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("SELECT begin,end FROM mapped_tms WHERE campsId=?");
			
			
			PreparedStatement pstmCount = CAMPS_CONNECTION.prepareStatement("SELECT sequenceid,sequence FROM sequences WHERE sequenceid>98979"); //added where because program crashed and request timed out
		
			ResultSet rsCount = pstmCount.executeQuery();
			ResultSet rs = null;
			
			while(rsCount.next()){ //for every sequenceid in sequences, do below
				
					int seqid = rsCount.getInt("sequenceid");
					//String query = rsCount.getString("sequence");
					System.out.print(seqid + "\n");
				
				
				pstm.setInt(1,seqid); //pick begin and end from mapped_tms table for this sequenceid 
				rs = pstm.executeQuery();
				
				ArrayList<Integer> startArray = new ArrayList<Integer>();
				ArrayList<Integer> endArray = new ArrayList<Integer>();
				int j =0;
				
				
				while(rs.next()) {
					startArray.add(j, rs.getInt("begin"));
					endArray.add(j, rs.getInt("end"));
					
					j++;
					//making an array with start and another array with end... at same index of both we have a start and end of the tm
					
					//so...can i make a temp aray of starts and end..and send it with seq id to read thread
					// in that function retreive the start end of the hit and calculate and submit in alignment table.
				}
				
				//we now have here..the seqidQuery tms coordinates of query..we can now send it for calculation of blast ad so on!
				//rs.close();
				rs = null;
				
				new ReadHitsThreadNew2(seqid, startArray, endArray, CAMPS_CONNECTION, MAX_EVALUE, MIN_LENGTH_RATIO); // in this function, retreive EACH hit for seqid and classify them as tm and non tm and calc param
				
				

			}
			//rs.close();
			rsCount.close();
			
			pstm.close();
			pstm = null;
		
			pstmCount.close();
			pstmCount = null;
			
			a = Calendar.getInstance().getTime();
			System.out.println("\n End time \n" + a);
			//so the above loop finds out the start and end of the tms and stores in the array, then for each mapped sequence
			//this start and end is calculated and subsequently stored in the hashtable called campsid2tms
			

	}
}
