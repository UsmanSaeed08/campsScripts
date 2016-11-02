package runs;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;


public class TmsCoresExtractionRUN {

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			
			int start = 0;
			int size = 10000;
			
			//int max = 508655;
			int max = 543886;
			
			int count = 0;

			while(start <=max) {
				count++;
				
				File shFile = new File("/home/users/saeed/coreExtraction/TmsCoreExtraction"+count+".sh");
				PrintWriter pw = new PrintWriter(new FileWriter(shFile));
				pw.println("#$-l vf=17G");
				pw.println("#$-N cores"+count);
				//#$-e /home/users/saeed/mpf0.e
				pw.println("#$-e /home/users/saeed/coreExtraction/out/coreExt"+count+".e");
				pw.println("#$-o /home/users/saeed/coreExtraction/out/coreExt"+count+".o");
				//pw.println("java -Xmx17G -jar /home/users/saeed/all_camps_scripts/TmsCoresExtraction.jar "+start+" "+size);			
				pw.println("java -Xmx17G -jar /scratch/usman/TmsCoresExtraction.jar "+start+" "+size);
				pw.close();
				
				start += size;
			}		
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}

	}

}
