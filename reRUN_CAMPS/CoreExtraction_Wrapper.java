package reRUN_CAMPS;

import java.text.SimpleDateFormat;
import java.util.Date;
import general.CreateDatabaseTables;



public class CoreExtraction_Wrapper {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			CreateDatabaseTables.create_tms_cores2();
			System.out.println("Created tms_cores2");
			CreateDatabaseTables.create_tms_blocks2();
			System.out.println("Created tms_blocks2");
			CoreExtraction.populateSeqAndLen();
			CoreExtraction.populatedictionary();
			CoreExtraction[] exarray = new CoreExtraction [42];
			int idx = 0;
			int i = 0;
			for(;i<=925441;){
				int start = i;
				int length = 23150; // for 925936/40 = 23150 --> where 40 is the number of threads to make and
				exarray[idx] = new CoreExtraction(start,length,idx);
				exarray[idx].start();
				i = i + length;
				idx++;
			}
			for(int j = 0;j<=idx-1;j++){
				if (exarray[j].isAlive()){
					exarray[j].join();
					System.out.print("Thread joining: "+j+"\n");
					System.out.flush();
				}
			}						
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
