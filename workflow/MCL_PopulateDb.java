package workflow;

import java.io.File;
import java.util.ArrayList;

public class MCL_PopulateDb {
	
	private ArrayList<String> files_results;
	int currentThresh;
	int clusterid;
	boolean flag;
	
	public MCL_PopulateDb(int t,boolean f){
		this.currentThresh = t;
		this.clusterid = 0;
		this.flag = f;
	}
	public void listFilesForFolder(final File folder) {		// to get all the files for this threshold run
		files_results = new ArrayList<String>();
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				listFilesForFolder(fileEntry);
			} else {
				
				if (flag){	// it is 5
					if (fileEntry.getName().toString().endsWith(".00"+this.currentThresh+".out")){
						// to pick up only current threshold files
						//files_results.add(fileEntry.getName().toString());
						files_results.add(fileEntry.getPath());
					}
				}
				else{
					if (fileEntry.getName().toString().endsWith("."+this.currentThresh+".out")){
						// to pick up only current threshold files
						//files_results.add(fileEntry.getName().toString());
						files_results.add(fileEntry.getPath());
					}
				}

				

			}
		}
	}
	public void print(){
		for (int i =0; i<=files_results.size()-1;i++){
			System.out.print(files_results.get(i)+"\t"+i+"\n");
		}
	}
	public void run (){
		Writetodb_mcl objdb1 = new Writetodb_mcl();
		for (int i =0; i<=files_results.size()-1;i++){
			objdb1 = new Writetodb_mcl(this.clusterid,this.currentThresh,files_results.get(i));
			objdb1.run();
			System.out.println("Processed: " + files_results.get(i) + "\t"+i+" of "+files_results.size());
			this.clusterid = objdb1.clusterid;
		}
	}
}
