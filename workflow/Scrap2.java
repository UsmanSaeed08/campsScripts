package workflow;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import utils.DBAdaptor;

public class Scrap2 {

	/**
	 * @param args
	 */
	// code in main make camps parts... i.e. write camps sequences in fasta format to file in sets of 350.. i.e 350 sequences in each file

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	//private static ArrayList<Double> sequences = new ArrayList<Double>();

	public static void run(){
		// gets all the sequences of sc_clusters 
		// put sequence in one file - fasta
		// put the tm in other file
		int counter = 0;
		int subfolder = 1;
		String filePathFasta = "/scratch/usman/IBClustering/"+subfolder+"/fasta/";
		String filePathTm = "/scratch/usman/IBClustering/"+subfolder+"/tm/";

		try{

			PreparedStatement pr = CAMPS_CONNECTION.prepareStatement("select cluster_id,cluster_threshold from cp_clusters where " +
					"type=?");

			PreparedStatement pr_clus_members = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl " +
					"where cluster_id=? AND cluster_threshold=?");

			PreparedStatement pr_seq = CAMPS_CONNECTION.prepareStatement("select sequence from sequences2 where " +
					"sequenceid=?");

			PreparedStatement pr_tm = CAMPS_CONNECTION.prepareStatement("select begin,end,length from tms where " +
					"sequenceid=? order by tms_id");

			pr.setString(1, "sc_cluster");
			ResultSet rs = pr.executeQuery();
			int id = 0;
			float thresh = 0f;
			while(rs.next()){
				// get the clustes
				id = rs.getInt(1);
				thresh = rs.getFloat(2);

				// get the cluster_members
				pr_clus_members.setInt(1, id);
				pr_clus_members.setFloat(2, thresh);
				ResultSet rs_members = pr_clus_members.executeQuery();
				while(rs_members.next()){
					int seqid = rs_members.getInt(1);
					// for every member, get the sequence
					pr_seq.setInt(1, seqid);
					ResultSet rs_sequence = pr_seq.executeQuery();
					String seq = "";
					while(rs_sequence.next()){
						seq = rs_sequence.getString(1);
					}
					rs_sequence.close();

					// for every member, get the tm and coordinates
					pr_tm.setInt(1, seqid);
					ResultSet rs_tm = pr_tm.executeQuery();
					ArrayList<Integer> begin = new ArrayList<Integer>();
					ArrayList<Integer> end = new ArrayList<Integer>();
					ArrayList<Integer> length = new ArrayList<Integer>();
					while(rs_tm.next()){
						begin.add(rs_tm.getInt(1));
						end.add(rs_tm.getInt(2));
						length.add(rs_tm.getInt(3));
					}
					rs_tm.close();
					/*
					System.out.print("SequenceId\t"+ seqid+"\n");
					System.out.print("ClusterId\t"+ id+"\n");
					System.out.print("Threshold\t"+ thresh+"\n");
					for(int i=0;i<=begin.size()-1;i++){
						System.out.print(begin.get(i)+"\t"+ end.get(i)+"\t"+length.get(i)+"\n");
					}
					System.out.print("****************************************************");
					 */
					// write fasta file


					BufferedWriter bw = new BufferedWriter(new FileWriter(new File(filePathFasta+seqid+"_"+id+"_"+thresh+".fasta")));
					bw.write(">"+seqid+"_"+id+"_"+thresh);
					bw.newLine();
					seq = seq.toUpperCase();
					bw.write(seq);
					bw.newLine();
					bw.close();

					// write TM file 

					BufferedWriter bwTM = new BufferedWriter(new FileWriter(new File(filePathTm+seqid+"_"+id+"_"+thresh+".fasta.tm")));
					bwTM.write("ID   "+seqid+"_"+id+"_"+thresh);
					bwTM.newLine();
					for(int i=0;i<=begin.size()-1;i++){
						//System.out.print(begin.get(i)+"\t"+ end.get(i)+"\t"+length.get(i)+"\n");
						bwTM.write("FT   "+"TOPO_DOM    "+0+"    "+10+"    "+"NON CYTOPLASMIC");
						bwTM.newLine();
						bwTM.write("FT   "+"TRANSMEM    "+begin.get(i)+"    "+end.get(i));
						bwTM.newLine();

					}
					bwTM.write("FT   "+"TOPO_DOM    "+0+"    "+10+"    "+"NON CYTOPLASMIC");
					bwTM.newLine();
					bwTM.close();

					//34400
					counter ++;
					if (counter % 34400 ==0){
						subfolder ++;
						// create the subfolders... and then adjust path
						// the folders are created already
						filePathFasta = "/scratch/usman/IBClustering/"+subfolder+"/fasta/";
						filePathTm = "/scratch/usman/IBClustering/"+subfolder+"/tm/";
						//	System.exit(0);
					}

					if (counter%100 == 0){
						System.out.print("Processed "+counter+ "\n");
						System.out.flush();
					}
				}
				rs_members.close();


				//System.out.print("\n"+id+"\t"+thresh+"\n");
			}
			rs.close();
			pr.close();
			rs.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}
	public static void run_for_free(){
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File("F:/Scratch/100510_14_5.0.fasta.o3am")));
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/Scratch/100510_14_5.0.fasta.o3am.forFree")));
			String line ="";
			while((line = br.readLine())!=null){
				if (!(line.contains(">"))){
					//line.replace("/[^a-z]/g", "");
					line = line.replaceAll("[^a-z]", "");
					bw.write(line);
					bw.newLine();
				}
			}
			br.close();
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	// the function checks the sequences which have been computed for hhblits and prints those with entire path which have not been processed
	// those sequences are also printed for which the file has been made but the file size is zero
	private static void run2(String i){
		// 
		String fasta_path = "/scratch/usman/IBClustering/"+i+"/fasta/";
		String hhblits_path = "/scratch/usman/IBClustering/"+i+"/hhblits/";

		//String fasta_path = "F:/virus_project/1/faa/";
		//String hhblits_path = "F:/virus_project/1/tm/";

		ArrayList<String> fasta_fileNames = listfiles(fasta_path);
		ArrayList<String> hhblits_fileNames = listfiles(hhblits_path);

		for (int j =0;j<=fasta_fileNames.size()-1;j++){
			String temp = fasta_fileNames.get(j);
			if (!hhblits_fileNames.contains(temp)){
				System.out.print(fasta_path+temp+"\n");
			}
		}


	}

	private static ArrayList<String> listfiles(String path){
		try{
			final File folder = new File(path);
			ArrayList<String> files_results = new ArrayList<String>();
			for (final File fileEntry : folder.listFiles()) {
				if (fileEntry.isDirectory()) {
					//listFilesForFolder(fileEntry);
					System.out.print("/n Sub Folders ignored /n");
				} else {

					String fname = fileEntry.getName();
					if (fname.endsWith(".fasta")){
						//System.out.print(fileEntry.getPath()+fileEntry.getName()+"\n");
						files_results.add(fname);
					}
					else if (fname.endsWith(".fasta.o3am")){
						//System.out.print(fileEntry.getPath()+fileEntry.getName()+"\n");
						if(fileEntry.length()>0){
							int s = fname.length();
							fname = fname.substring(0, s-5);
							files_results.add(fname);
						}
					}
				}
			}

			//System.out.println("\n total files added " + files_results.size() + " \n");
			return files_results;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		run2(args[0]);
		
		System.exit(0); // so the rest is not called

		//The function below writes the o3am file into freecontact readable
		//run_for_free();
		//System.exit(0); // so the rest is not called
		try{



			PreparedStatement pr = CAMPS_CONNECTION.prepareStatement("select sequenceid,sequence from sequences2");
			ResultSet rs = pr.executeQuery();
			double CountX = 0;
			int batchNumber =1;
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("/home/users/saeed/FHClustering/in/Camps_"+batchNumber+".fasta")));
			///home/users/saeed/FHClustering/inAll_ReRun
			while (rs.next()){
				int id = rs.getInt(1);
				String seq = rs.getString(2);
				bw.write(">"+id+"\n");
				bw.write(seq+"\n");
				CountX++;
				if (CountX%350==0){
					bw.close();
					batchNumber ++;
					bw = new BufferedWriter(new FileWriter(new File("/home/users/saeed/FHClustering/in/Camps_"+batchNumber+".fasta")));
					System.out.print("Done - "+batchNumber+"\n");
					System.out.flush();
				}

			}
			bw.close();
			rs.close();
			pr.close();
			CAMPS_CONNECTION.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

}
