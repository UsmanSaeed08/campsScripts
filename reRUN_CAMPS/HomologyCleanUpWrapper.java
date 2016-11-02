package reRUN_CAMPS;

import java.text.SimpleDateFormat;
import java.util.Date;

public class HomologyCleanUpWrapper {

	/**
	 * @param args
	 */
	/*
	public static void main(String[] args) { // this main is to run without threading
		// TODO Auto-generated method stub
		try {
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");			
			HomologyCleanUp hc ;
			int idx = 0;
			//int i = 1;
			//for(;i<=925936;){
				//int start = i;
				int length = 925936; // for 925936/40 = 23150 --> where 40 is the number of threads to make and
				hc = new HomologyCleanUp(1,length,idx);
				hc.run();
				//i = i + length;
			//}
									
			Date endDate = new Date();
			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
		} catch(Exception e) {
			e.printStackTrace();
		}	
	}*/
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
			Date startDate = new Date();
			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");			
			HomologyCleanUp[] hcarray = new HomologyCleanUp [42];
			int idx = 0;
			int i = 1;
			for(;i<=925936;){
				int start = i;
				int length = 23150; // for 925936/40 = 23150 --> where 40 is the number of threads to make and
				hcarray[idx] = new HomologyCleanUp(start,length,idx);
				hcarray[idx].start();
				i = i + length;
				idx++;
			}
			for(int j = 0;j<=hcarray.length-1;j++){
				if (hcarray[j].isAlive()){
					hcarray[j].join();
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
