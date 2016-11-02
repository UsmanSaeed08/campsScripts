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
import extract_proteins.ReadHitsThreadNew;
import datastructures.TMS;
//import de.gsf.mips.simap.lib.database.DbSequence;

public class ExtractSimilarityScoresNew {
	private int N_SIMAP_THREADS = 2;
	
	
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
	 */
	public void run(String logfile) {
		
		Date a = Calendar.getInstance().getTime();
		System.out.println("\n Extract Similarity Score starts at: " + a + "\n");
		
		PrintWriter pw = null;
		
		try {
			
			pw = new PrintWriter(new FileWriter(new File(logfile)));
			pw.println("\n Extract Similarity Score starts at: " + a + "\n");			
			System.out.println("\t\t[INFO]: Perform mapping between SIMAP and CAMPS sequence ids.");
			IDMapping idm = new IDMapping();			
			BitSet selected_sequences = idm.getSimapSequenceIDs();
			int[] mapping = idm.getMapping();  //index: SIMAP id, value: CAMPS id
			
			a = Calendar.getInstance().getTime();
			System.out.println("\n ID Mapping complete now get TMS coordinates: " + a + "\n");
			
			System.out.println("\t\t[INFO]: Get TMS coordinates for all sequences.");
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("SELECT begin,end FROM tms WHERE sequences_sequenceid=?");
			Hashtable<Integer,ArrayList<TMS>> campsid2tms = new Hashtable<Integer,ArrayList<TMS>>();
			for(int i=0; i<mapping.length; i++) {
				int campsid = mapping[i];
				
				if(campsid == 0) {  //no valid mapping between SIMAP and CAMPS
					continue;
				}
				
				ArrayList<TMS> tms_arr_query = new ArrayList<TMS>();		//TMS is a self created data structure
				
				pstm.setInt(1,campsid); //pick begin and end from tms table for this sequenceid 
				ResultSet rs = pstm.executeQuery();
				while(rs.next()) {
					int start = rs.getInt("begin");
					int end = rs.getInt("end");
					TMS tms = new TMS(start,end);
					tms_arr_query.add(tms);
				}
				rs.close();
				rs = null;
				
				campsid2tms.put(Integer.valueOf(campsid), tms_arr_query);
			}
			//so the above loop finds out the start and end of the tms and stores in the array, then for each mapped sequence
			//this start and end is calculated and subsequently stored in the hashtable called campsid2tms
			pstm.close();
			pstm = null;
			
			a = Calendar.getInstance().getTime();
			System.out.println("\n Generation of hashtable from TMS complete: " + a + "\n");
			
			/*
			//int[] sequencelengths = DbSequence.getInstance().getSequencelengthArray();//???????
			
			long searchSpaceSize = 0;		
			for (int sequenceid = selected_sequences.nextSetBit(0); sequenceid >=0; sequenceid = selected_sequences.nextSetBit(sequenceid+1)) {
				searchSpaceSize += sequencelengths[sequenceid];
			}*/
			
			System.out.println("\n\t\t[INFO]: Start extracting similarity scores.");
			a = Calendar.getInstance().getTime();
			System.out.println("\n at: " + a + "\n");
			int statusCounter = 0;
			// Get hits for all selected sequence id's		
			for (int sequenceid = selected_sequences.nextSetBit(0); sequenceid >=0; sequenceid = selected_sequences.nextSetBit(sequenceid+1)) {
				statusCounter ++;
				if (statusCounter % 1000 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter % 100000 == 0) {
					System.out.write('\n');
					System.out.flush();
				}
				
				pw.println(sequenceid);
				
				//System.out.println("Number of running threads: " + ReadHitsThread.getThreadCount());
				
				while (ReadHitsThreadNew.getThreadCount() >= N_SIMAP_THREADS) { // a beautiful way to control the number of threads being generated
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				//new ReadHitsThreadNew(sequenceid, selected_sequences, mapping, searchSpaceSize, sequencelengths, CAMPS_CONNECTION, MAX_EVALUE, MIN_LENGTH_RATIO, campsid2tms);
				new ReadHitsThreadNew(sequenceid, selected_sequences, mapping, CAMPS_CONNECTION, MAX_EVALUE, MIN_LENGTH_RATIO, campsid2tms);
			}
			
			//wait until all threads are finished
			while(ReadHitsThreadNew.getThreadCount() > 0) {
				try {
					Thread.sleep(100);
				} catch(InterruptedException e) {
					e.printStackTrace();
				}
			}			
			
			pw.close();
			a = Calendar.getInstance().getTime();
			System.out.println("\n Completion at: " + a + "\n");
			
		} catch(Exception e) {
			System.err.println("Exception in ExtractSimilarityScoresNew.run(): " +e.getMessage());
			e.printStackTrace();
		}
		finally {
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}	
			
			if(pw != null) {
				pw.close();
			}
		}		
	}
}
