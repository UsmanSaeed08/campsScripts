package reRUN_CAMPS;

import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ClusterPropertiesMCL_NR2_Wrapper {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
			CreateDatabaseTables.create_table_clusters_mcl_nr_info2() ;
			System.out.println("Table Made...");
			
			ClusterPropertiesMCL_NR2[] array = new ClusterPropertiesMCL_NR2 [42];
			int idx = 0;
			int i = 1;
			for(;i<=925936;){
				int start = i;
				int length = 23150; // for 925936/40 = 23150 --> where 40 is the number of threads to make and
				array[idx] = new ClusterPropertiesMCL_NR2(start,length,idx);
				array[idx].start();
				i = i + length;
				idx++;
			}
			for(int j = 0;j<=idx-1;j++){
				if (array[j].isAlive()){
					array[j].join();
					System.out.print("Thread joining: "+j+"\n");
					System.out.flush();
				}
			}						
			/*
			int start = Integer.parseInt(args[0]);
			int length = Integer.parseInt(args[1]);
									
			ClusterPropertiesMCL_NR cp = new ClusterPropertiesMCL_NR(start,length);
			cp.run();
			*/
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
			
			
//		    addSuperkingdomComposition();
		    
//		    addNumCores();
		    
//		    addAPSI();
			
			
//			addMedianLength();
			
		} catch(Exception e) {
			e.printStackTrace();
		}	

	}

}
