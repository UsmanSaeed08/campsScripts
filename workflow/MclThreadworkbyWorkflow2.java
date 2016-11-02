package workflow;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import utils.DBAdaptor;

public class MclThreadworkbyWorkflow2 {

	//String NewFilesfolder = "/scratch/usman/mcl/in/";	
	//String MCLresultsfolder = "/scratch/usman/mcl/out/";

	//gefjun-10g
	String NewFilesfolder; //= "/home/users/saeed/mcl_run/in/";	
	String MCLresultsfolder; //= "/home/users/saeed/mcl_run/out/";

	//String NewFilesfolder = "F:/mcl/testJava/in/";
	//String MCLresultsfolder = "F:/mcl/testJava/out/";

	public int clusterid; // resets to zero after for every new threshold
	//float thresh;
	int currentThresh;
	int nextThresh;
	public int lines;
	int availableThread;
	BufferedWriter log;

	MclThreadworkbyWorkflow2(int cid, String i, String o, BufferedWriter w){
		this.clusterid = cid;
		this.NewFilesfolder = i;
		this.MCLresultsfolder = o;
		availableThread = 0;
		this.log = w;
	}




	void run (String file){

		// ************ NEW WAY TO THREAD

		try{	
			BufferedReader reader = new BufferedReader(new FileReader(file));
		//	int counter =0;	//controls the number of threads

			//Writetodb_mcl[] objdb1 = new Writetodb_mcl[10];
			Makenewfile_mcl[] objnf1 = new Makenewfile_mcl[100];
			String sCurrentLine;
			String clus1;
			for(int i =0; i<=99; i++){		
				objnf1[i] = new Makenewfile_mcl();
			}

			while ((sCurrentLine = reader.readLine()) != null){
				clus1 = sCurrentLine;

				if ( this.currentThresh< 100){
					if (Makenewfile_mcl.ActiveThreadCount <=99){
					boolean temp = true;
					while (temp){
						for(int i =0; i<=99; i++){					// this loop to is go through all the threads and check if active or not
							if (!objnf1[i].isAlive()){				// if loop is not alive then make new thread
								this.availableThread = i;			//get first available thread and do below
								objnf1[availableThread] = new Makenewfile_mcl (this.NewFilesfolder,this.clusterid,clus1,availableThread,this.log);	//after finding available thread
								objnf1[availableThread].start();																			//run the thread
								this.clusterid++;
								this.log.write("	Thread started for "+this.clusterid+" at threadId " +i);
								this.log.newLine();
								this.log.flush();
								temp = false;
								break;
							}
						}
					}
					}
					else {
						boolean flag = true;
						while(flag){
							Thread.sleep(600);	// 300,000 -> 5 minutes , 60k 1 min
							for(int i =0; i<=99; i++){			
								if (!objnf1[i].isAlive()){
									this.availableThread = i;
									
									objnf1[availableThread] = new Makenewfile_mcl (this.NewFilesfolder,this.clusterid,clus1,availableThread,this.log);
									objnf1[availableThread].start();
									this.clusterid++;
									this.log.write("	Thread started for "+this.clusterid+" at threadId " +i);
									this.log.newLine();
									this.log.flush();
									//System.out.print("Thread made for index: "+availableThread+"\n");
									flag = false;
									break;
								}
							}
						}
					}

					
				}
			}	// file read complete, So now join all the active threads before ending the script
			//System.out.print("\n \n \n \n");
			for(int i =0; i<=99; i++){			
				if (objnf1[i].isAlive()){
					objnf1[i].join();
					//System.out.print("Thread joining: "+i+"\n");
					this.log.write("Thread joining: "+i);
					this.log.newLine();
					this.log.flush();
				}
			}
			reader.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

}
