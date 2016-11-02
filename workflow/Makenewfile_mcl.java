package workflow;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Hashtable;

public class Makenewfile_mcl extends Thread{

	static int nextThresh;
	static int currentThresh;
	static String dictP;
	static int ActiveThreadCount=0;
	//static ArrayList<Dictionary> dict = new ArrayList<Dictionary>();
	static Hashtable<Integer, Dictionary3> dict = new Hashtable<Integer, Dictionary3>();

	public String NewFilesfolder;

	public int cid;
	public String cluster;
	public int threadIndex;
	public boolean threadStatus;	// true ---> running      false --> finished
	BufferedWriter log;

	Makenewfile_mcl(String nf, int clusid, String f, int ti, BufferedWriter w){
		//Makenewfile_mcl.ActiveThreadCount =0;

		this.NewFilesfolder = nf;
		this.cid = clusid;
		this.cluster = f;
		this.threadIndex = ti;
		this.threadStatus = true;
		this.log = w;
	}
	Makenewfile_mcl(){

	}
	/*
	public static final void makedict(){
		int line =0;
		try{
			BufferedWriter wr = new BufferedWriter(new FileWriter(getDictPath()+"dictionary."+getNextThresh()+".abc")); //arg4

			BufferedReader br = new BufferedReader(new FileReader(getDictPath()+"dictionary."+getCurrentThresh()+".abc"));//arg4

			String sCurrentLine = ""; 

			float c = 0f;
			int nT = getNextThresh();

			while ((sCurrentLine = br.readLine()) != null) {
				try{
					String[] abc = sCurrentLine.split(" ");
					line++;
					//System.out.print(abc[0]+" "+abc[1]+" "+abc[2]+"\n");
					Integer key = Integer.parseInt(abc[0]);
					Integer b = Integer.parseInt(abc[1]);
					try{
						c = Float.parseFloat(abc[2]);
					}
					catch(NumberFormatException e){
						c = 101;

					}
					if (c > nT){ 	// Next Thresh
						wr.write(abc[0] + " " + abc[1] + " " + c);
						wr.newLine();

						// problem here!!! 1. the b column is not taken into account. if some protein only exists in b then
						// its key does not exist
						// 2. problem is that why am i searching for the links outside of the cluster... the last problem demostrated
						// that each cluster is further given in with all its own specific relationships. not the whole of data for all the 
						// occurances of that protein. 
						if (Makenewfile_mcl.dict.containsKey(key)){ // the key already doesnt exist...so that you dont delete the previous info of the key
							Dictionary temp = (Dictionary)dict.get(key);
							temp.set(b, c);
							Makenewfile_mcl.dict.put(key,temp);
						}
						else{
							Dictionary temp = new Dictionary(b,c);
							Makenewfile_mcl.dict.put(key,temp);
						}

					}
				}
				catch(Exception e){
					System.out.print("101 exception in file at line "+line);
					e.printStackTrace();
				}
			}
			br.close();
			wr.close();


			// NOW TO TRAVERSE THE FILE TO FIX THE ISSUE MENTIONED IN NUMBER 1 ABOVE...I.E. THE PROBLEM OF B COLUMN KEYS
			BufferedReader br2 = new BufferedReader(new FileReader(getDictPath()+"dictionary."+getCurrentThresh()+".abc"));//arg4

			while ((sCurrentLine = br2.readLine()) != null) {
				try{
					String[] abc = sCurrentLine.split(" ");

					// this time key is at abc 1
					Integer key = Integer.parseInt(abc[1]);
					Integer b = Integer.parseInt(abc[0]);
					try{
						c = Float.parseFloat(abc[2]);
					}
					catch(NumberFormatException e){
						c = 101;

					}
					if (c > nT){ 	// Next Thresh

						if (Makenewfile_mcl.dict.containsKey(key)){ // the key already exists...so that you dont delete the previous info of the key
							//LEAVING THIS EMPTY MEANS UR DATA IS SYMMETRICAL
							Dictionary temp = (Dictionary)dict.get(key);
							temp.set(b, c);
							Makenewfile_mcl.dict.put(key,temp);
							/*
							if (temp.protein.contains(b)){
								int idx = temp.protein.indexOf(b);
								float sc = temp.score.get(idx);
								if (sc != c){
									temp.set(b, c);
									Makenewfile_mcl.dict.put(key,temp);
								}
							}
						}
						else{
							Dictionary temp = new Dictionary(b,c);
							Makenewfile_mcl.dict.put(key,temp);
						}

					}
				}
				catch(Exception e){
					System.out.print("101 exception in file at line "+line);
					e.printStackTrace();
				}
			}
			br2.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.print("102 exception in file at line "+line);
		}
	}*/
	
	public static final void makedict(){
		int line =0;
		try{
			BufferedWriter wr = new BufferedWriter(new FileWriter(getDictPath()+"dictionary."+getNextThresh()+".abc")); //arg4

			BufferedReader br = new BufferedReader(new FileReader(getDictPath()+"dictionary."+getCurrentThresh()+".abc"));//arg4

			String sCurrentLine = ""; 

			float c = 0f;
			int nT = getNextThresh();

			while ((sCurrentLine = br.readLine()) != null) {
				try{
					String[] abc = sCurrentLine.split(" ");
					line++;
					//System.out.print(abc[0]+" "+abc[1]+" "+abc[2]+"\n");
					int key = Integer.parseInt(abc[0]);
					int b = Integer.parseInt(abc[1]);

					int key2 = b;
					int b2 = key;
					try{
						c = Float.parseFloat(abc[2]);
					}
					catch(NumberFormatException e){
						c = 101;

					}
					if (c > nT){ 	// Next Thresh
						wr.write(abc[0] + " " + abc[1] + " " + c);
						wr.newLine();

						// problem here!!! 1. the b column is not taken into account. if some protein only exists in b then
						// its key does not exist
						// 2. problem is that why am i searching for the links outside of the cluster... the last problem demostrated
						// that each cluster is further given in with all its own specific relationships. not the whole of data for all the 
						// occurances of that protein. 
						if (Makenewfile_mcl.dict.containsKey(key)){ // the key already doesnt exist...so that you dont delete the previous info of the key
							Dictionary3 temp = (Dictionary3)dict.get(key);
							temp.set(b, c);
							Makenewfile_mcl.dict.put(key,temp);
						}
						else{
							Dictionary3 temp = new Dictionary3(b,c);
							Makenewfile_mcl.dict.put(key,temp);
						}
						// now add in reverse direction
						if (Makenewfile_mcl.dict.containsKey(key2)){ // the key already exists...so that you dont delete the previous info of the key
							//LEAVING THIS EMPTY MEANS UR DATA IS SYMMETRICAL
							Dictionary3 temp = (Dictionary3)dict.get(key2);
							temp.set(b2, c);
							Makenewfile_mcl.dict.put(key2,temp);
						}
						else{
							Dictionary3 temp = new Dictionary3(b2,c);
							Makenewfile_mcl.dict.put(key2,temp);
						}
					}
				}
				catch(Exception e){
					System.out.print("101 exception in file at line "+line);
					e.printStackTrace();
				}
			}
			br.close();
			wr.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.print("102 exception in file at line "+line);
		}
	}

	public void run(){
		//this.threadStatus = true;
		Makenewfile_mcl.ActiveThreadCount++;
		try{

			String sCurrentLine = this.cluster; 

			// every line is a cluster
			//1. split the line for tab to get members of cluster

			String[] clusteredSequences = sCurrentLine.split("\t");
			if(clusteredSequences.length>2){	
				makenewInputFiles(clusteredSequences,this.cid);
				this.log.write("		File generation Complete for "+this.cid+" using "+this.threadIndex);
				this.log.newLine();
				this.log.flush();
			}
			//this.cid++;
		}
		catch(Exception e){
			e.printStackTrace();
		}

		Makenewfile_mcl.ActiveThreadCount--;
		this.threadStatus = false;
		//this.stop();

	}
	/*public static synchronized Dictionary3 DictionaryGet(int k){
		return (Dictionary3)Makenewfile_mcl.dict.get(k);
		//return null;
		}
	*/
	void makenewInputFiles(String [] cluster,int clusid) throws IOException{
		// for each cluster makes new input file

		//---> Get threshNew
		BufferedWriter wr = new BufferedWriter(new FileWriter(this.NewFilesfolder+"thresh"+clusid+"."+getNextThresh()+".abc"));	//PATH TO INPUTFILE + NAME

		for (int i =0; i<=cluster.length-1; i++){

			int key = Integer.parseInt(cluster[i]);

			Dictionary3 temp = (Dictionary3)Makenewfile_mcl.dict.get(key);
			//Dictionary3 temp = DictionaryGet(key);
			if (temp != null){
				// using j = i because the data is symmetrical and i have made the dictionary with every possible key -- i.e. from seqidQ or seqidH
				for (int j =i ; j<= cluster.length-1; j++){	//IF I MAKE J = I THEN I AM ASSUMING SYMMETRIC DATA, ELSE NOT 
					
					// e -5. So it is fast 
					// make new way to find the relationships

					// still now i am checking combinations, and not permutations...i.e. assuming it to be symmetrical
					// plus...also have to include the exception case in which no index is found
					// if no index found, then no eval found either

					if (i != j){
						int clus_mem = Integer.parseInt(cluster[j]);	//find all the occurances of this member in mcl file of
						//int index = temp.protein.indexOf(clus_mem);
						if(temp.prot2score.containsKey(clus_mem)){
						//int index = temp.protein.indexOf(clus_mem);
						//if (index != -1){
							//float eval = temp.score.get(index);
							float eval = temp.prot2score.get(clus_mem);
							wr.write(key + " " + clus_mem + " "+eval);
							wr.newLine();
						//}
					}
						else{
							//System.out.print(key + " " + clus_mem + "\n");
						}
					}

				}
			}
		}
		wr.close();
	}

	public static int getNextThresh() {
		return nextThresh;
	}

	public static void setNextThresh(int nextThresh) {
		Makenewfile_mcl.nextThresh = nextThresh;
	}
	public static int getCurrentThresh() {
		return currentThresh;
	}
	public static void setCurrentThresh(int nextThresh) {
		Makenewfile_mcl.currentThresh = nextThresh;
	}

	public static String getDictPath() {
		return dictP;
	}
	public static void setDictPath(String d) {
		Makenewfile_mcl.dictP = d;
	}

}
