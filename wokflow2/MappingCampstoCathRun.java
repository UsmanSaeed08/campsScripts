package wokflow2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import workflow.Makenewfile_mcl;

public class MappingCampstoCathRun {

	/**
	 * @param args
	 * runs the mapping in a parallel way 
	 * checks the running threads and completed threads and thereby generates new thread on empty processor
	 * 
	 */

	//float thresh;

	int availableThread;
	BufferedWriter log;
	static int threads;

	MappingCampstoCathRun(){
		
		availableThread = 0;
		threads = 10;
		
	}

	public static void main(String[] args) {
		System.out.print("Start\n");
		// input files
		// remember to move the processed sequence files to backup folder - also remember to make the already processed true
		
		//String infiles = "/home/proj/check/RunMetaModel_gef/HMMs/CAMPS4_1/";
		//String allseqs = "/home/users/saeed/scratch/mappingCampsToCATHandPDB/allSequeces.txt";
		//String procfile = "/home/users/saeed/scratch/mappingCampsToCATHandPDB/ProcessedSequecesNew.txt";
		//String mapfile = "/home/users/saeed/scratch/mappingCampsToCATHandPDB/Camps2PDBmap.txt";		
		//String logfile = "/home/users/saeed/scratch/mappingCampsToCATHandPDB/log.txt";
		//String backupProcessedSequenceFile = "/home/users/saeed/scratch/mappingCampsToCATHandPDB/backup/ProcessedSequeces/";
		/*
		String infiles = "F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/HMMs/CAMPS4_1/";
		String allseqs = "F:/Scratch/mappingCampsToCATHandPDB/allSequeces.txt";
		String procfile = "F:/Scratch/mappingCampsToCATHandPDB/ProcessedSequeces.txt";
		String mapfile = "F:/Scratch/mappingCampsToCATHandPDB/Camps2PDBmap.txt";		
		String logfile = "F:/Scratch/mappingCampsToCATHandPDB/log.txt";
		String backupProcessedSequenceFile = "F:/Scratch/mappingCampsToCATHandPDB/procs/";
		*/
		//MappingCampsCATH.Initialize(infiles,allseqs,procfile,mapfile,backupProcessedSequenceFile,false);
		
		
		//String mapfile = args[1].trim(); //-map file to make
		//String seqtorun = args[0].trim(); // -file with seqIds to run for simap
		//String seqtorun = "F:/Scratch/mappingCampsToCATHandPDB/allSequeces.txt"; //allSequences2.txt
		
		//String seqtorun = "F:/Scratch/mappingCampsToCATHandPDB/allSequences2.txt"; //allSequences2.txt
		//String mapfile = "F:/Scratch/mappingCampsToCATHandPDB/OutAll2.txt"; //-map file to make
		String seqtorun = "/home/users/saeed/allSequences2.txt"; //allSequences2.txt
		String mapfile = "/home/users/saeed/reRUNCAMPSSeqToPDB.txt"; //-map file to make
		
		MappingCampsCATH.Initialize(mapfile,seqtorun);
		
		System.out.print("Sequences to process "+MappingCampsCATH.SC_seqIds.size()+"\n");
		System.out.print("Starting Mapping\n");
		
		MappingCampstoCathRun ob = new MappingCampstoCathRun(); 
		ob.runwithoutLog();
		MappingCampsCATH.closeFiles2();
		
	}

	private void runwithoutLog() {

		// ************ NEW WAY TO THREAD
		try{	
			int totalSeq = MappingCampsCATH.SC_seqIds.size() - 1 ;
			MappingCampsCATH[] objnf1 = new MappingCampsCATH[threads];
			for(int i =0; i<=threads-1; i++){		
				objnf1[i] = new MappingCampsCATH();
			}
			for(int seqNumber = -1; seqNumber<MappingCampsCATH.SC_seqIds.size()-1;){
				if (MappingCampsCATH.ActiveThreadCount <=threads-1){
					boolean temp = true;
					while (temp){
						for(int i =0; i<=threads-1; i++){					// this loop to is go through all the threads and check if active or not
							if (!objnf1[i].isAlive()){				// if loop is not alive then make new thread
								if(seqNumber < MappingCampsCATH.SC_seqIds.size()-1){
									this.availableThread = i;			//get first available thread and do below
									seqNumber ++;
									int seqId = MappingCampsCATH.SC_seqIds.get(seqNumber);
									String seq = MappingCampsCATH.SeqIdToSeq.get(seqId);
									if(seq != null){
										objnf1[availableThread] = new MappingCampsCATH (seq,seqId);	//after finding available thread
										objnf1[availableThread].start();					//run the thread
										//objnf1[availableThread].run();					//run the thread
									}
									System.out.print("Processing Sequence number "+ seqNumber+" of "+totalSeq+" ** SequenceId: "+ seqId+" \n");
									System.out.flush();
									temp = false;
									break;
								}
							}
						}
					}
				}
				else {
					boolean flag = true;
					while(flag){
						Thread.sleep(60);	// 300,000 -> 5 minutes , 60k 1 min
						for(int i =0; i<=threads-1; i++){			
							if (!objnf1[i].isAlive()){
								if(seqNumber < MappingCampsCATH.SC_seqIds.size()-1){
									seqNumber ++;
									this.availableThread = i;
									int seqId = MappingCampsCATH.SC_seqIds.get(seqNumber);
									String seq = MappingCampsCATH.SeqIdToSeq.get(seqId);
									if(seq != null){
									
									objnf1[availableThread] = new MappingCampsCATH (seq,seqId);
									objnf1[availableThread].start();
									//objnf1[availableThread].run();					//run the thread
								}
									System.out.print("Processing Sequence number  "+ seqNumber+" of "+totalSeq+" ** SequenceId: "+ seqId+" \n");
									System.out.flush();
									flag = false;
									break;
								}
							}
						}
					}
				}
			}	// file read complete, So now join all the active threads before ending the script
			//System.out.print("\n \n \n \n");
			for(int i =0; i<=threads-1; i++){			
				if (objnf1[i].isAlive()){
					objnf1[i].join();
					System.out.print("Thread joining: "+i+"\n");
					System.out.flush();
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private void run(String lg) {

		// ************ NEW WAY TO THREAD
		try{	
			log = new BufferedWriter(new FileWriter(new File(lg)));
			int totalSeq = MappingCampsCATH.SC_seqIds.size();
			MappingCampsCATH[] objnf1 = new MappingCampsCATH[threads];

			for(int i =0; i<=threads-1; i++){		
				objnf1[i] = new MappingCampsCATH();
			}

			for(int seqNumber = -1; seqNumber<=MappingCampsCATH.SC_seqIds.size()-1;){

				if (MappingCampsCATH.ActiveThreadCount <=threads-1){
					boolean temp = true;
					while (temp){
						for(int i =0; i<=threads-1; i++){					// this loop to is go through all the threads and check if active or not
							if (!objnf1[i].isAlive()){				// if loop is not alive then make new thread
								if(seqNumber <= MappingCampsCATH.SC_seqIds.size()-1){
									this.availableThread = i;			//get first available thread and do below
									seqNumber ++;

									int seqId = MappingCampsCATH.SC_seqIds.get(seqNumber);
									String seq = MappingCampsCATH.SeqIdToSeq.get(seqId);

									if(seq != null){
										objnf1[availableThread] = new MappingCampsCATH (seq,seqId);	//after finding available thread
										objnf1[availableThread].start();					//run the thread
									}
									this.log.write("	Thread started for seqid "+seqId+" at threadId " +i);
									System.out.print("Processing Sequence number "+ seqNumber+" of "+totalSeq+"\n");
									System.out.flush();
									this.log.newLine();
									this.log.flush();
									temp = false;
									break;
								}
							}
						}
					}
				}
				else {
					boolean flag = true;
					while(flag){
						Thread.sleep(60);	// 300,000 -> 5 minutes , 60k 1 min
						for(int i =0; i<=threads-1; i++){			
							if (!objnf1[i].isAlive()){
								if(seqNumber <= MappingCampsCATH.SC_seqIds.size()-1){
									seqNumber ++;
									this.availableThread = i;
									int seqId = MappingCampsCATH.SC_seqIds.get(seqNumber);
									String seq = MappingCampsCATH.SeqIdToSeq.get(seqId);
									if(seq != null){
									}
									objnf1[availableThread] = new MappingCampsCATH (seq,seqId);
									objnf1[availableThread].start();

									this.log.write("	Thread started for seqid "+seqId+" at threadId " +i);
									this.log.newLine();
									this.log.flush();
									System.out.print("Processing Sequence number  "+ seqNumber+"of "+totalSeq+"\n");
									System.out.flush();
									flag = false;
									break;
								}
							}
						}
					}
				}
			}	// file read complete, So now join all the active threads before ending the script
			//System.out.print("\n \n \n \n");
			for(int i =0; i<=threads-1; i++){			
				if (objnf1[i].isAlive()){
					objnf1[i].join();
					//System.out.print("Thread joining: "+i+"\n");
					this.log.write("Thread joining: "+i);
					this.log.newLine();
					this.log.flush();
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}


}
