package workflow;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import utils.DBAdaptor;

public class Scrap {
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");


	public Scrap(){

	}
	//this method compares the ppi data and checks if it is TM or not

	// for seq_id3
	public void run6(String in){
		try{

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"SELECT entry_name FROM camps2uniprot");
			BufferedReader br = new BufferedReader(new FileReader(in));
			//every line is a file
			String line;
			int count =0;
			int lcount=0;

			Hashtable<String, Integer> proteins = new Hashtable();
			
			List<String> db = new ArrayList<String>();;
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				db.add(rs.getString("entry_name"));
			}
			rs.close();

			while ((line = br.readLine()) != null){

				line = line.trim();
				/*
				int st = line.indexOf(" id=");
				line = line.substring(st+4);
				int end = line.indexOf(" />");
				line = line.substring(1, end-1);
				 */
				//int st = line.indexOf(" ");
				//line = line.substring(0, st);
				
				if (!line.contains("NoData")){


					System.out.print(line+"\n");
					if (!proteins.containsKey(line)){
						proteins.put(line, 0);
						lcount++;
						for(int i=0; i<= db.size()-1; i++){
							String t = db.get(i);
							if (t.contains(line)){
								count++;
								continue;
							}
						}
						
						
						
					}
				}
				// 0 accession 1 seq


			}
			System.out.print("total seq "+lcount);
			System.out.print("\nMapped seq "+count);


			br.close();
			//pstm.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			try {
				CAMPS_CONNECTION.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
	// for seq_id 2 and seq_id
	public void run5(String in){
		try{

			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid FROM camps2uniprot WHERE accession=?");
			
			BufferedReader br = new BufferedReader(new FileReader(in));
			//every line is a file
			String line;
			int count =0;
			int lcount=0;

			Hashtable<String, Integer> proteins = new Hashtable();
			//proteins.put("", 0);
			while ((line = br.readLine()) != null){

				line = line.trim();
				/*
				int st = line.indexOf(" id=");
				line = line.substring(st+4);
				int end = line.indexOf(" />");
				line = line.substring(1, end-1);
				 */
				
				if (!line.contains("NoData")){
					System.out.print(line+"\n");
					if (!proteins.containsKey(line)){
						proteins.put(line, 0);
						lcount++;
						pstm.setString(1, line);
						ResultSet rs = pstm.executeQuery();
						while(rs.next()){
							count++;
						}
						rs.close();
					}
				}
				// 0 accession 1 seq


			}
			System.out.print("total seq "+lcount);
			System.out.print("\nMapped seq "+count);


			br.close();
			//pstm.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			try {
				CAMPS_CONNECTION.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
	// this code joins together the files
	// the function took bo's data, uniprot ids and got camps corresponding ids and printed them to file
	public void run4(String in){
		int n =0;
		try{
			//File fileWrite = new File("F:/TmpSeqNotinCamps_Map");	// tail trembl
			//BufferedWriter wr = new BufferedWriter(new FileWriter(fileWrite));
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequenceid FROM sequences2 WHERE sequence=?");
			BufferedReader br = new BufferedReader(new FileReader(in));
			//every line is a file
			String line;
			while ((line = br.readLine()) != null){
				line = line.trim();
				String parts[] = line.split("\t");
				// 0 accession 1 seq

				pstm.setString(1, parts[1]);
				ResultSet rs = pstm.executeQuery();
				//if(!rs.next()){
				//wr.write(line+"\t"+"NOT_MAPPED"+"\n");
				//}
				//rs.beforeFirst();
				while(rs.next()){
					String temp = rs.getString("sequenceid");
					//wr.write(parts[0]+"\t"+temp+"\n");
					System.out.print(parts[0]+"\t"+temp+"\n");
				}
				System.out.print("\n seq no \n"+n);
				n++;
				rs.close();





			}
			//wr.close();
			br.close();
			pstm.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			try {
				CAMPS_CONNECTION.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
	public void run3(){
		/*
		 * /scratch/usman/mappingCamps/MappedTrembl13
/scratch/usman/mappingCamps/MappedTrembl5
/scratch/usman/mappingCamps/MappedTrembl11
/scratch/usman/mappingCamps/MappedTrembl9
/scratch/usman/mappingCamps/MappedTrembl2
/scratch/usman/mappingCamps/MappedTrembl3
/scratch/usman/mappingCamps/MappedTrembl4
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl4.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl13.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl16.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl6.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl0
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl3.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl10.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl1.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl14.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl5.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl2.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl8.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl15.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl7.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl12.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl9.1
/scratch/usman/mappingCamps/mappingDone08_07_2014/MappedTrembl11.1
/scratch/usman/mappingCamps/MappedTrembl10
/scratch/usman/mappingCamps/MappedTrembl1
/scratch/usman/mappingCamps/MappedTrembl12
/scratch/usman/mappingCamps/MappedTrembl7
/scratch/usman/mappingCamps/mappingDone_Files/MappedTrembl0
/scratch/usman/mappingCamps/mappingDone_Files/MappedTrembl1
/scratch/usman/mappingCamps/MappedTrembl8
/scratch/usman/mappingCamps/MappedTrembl14
/scratch/usman/mappingCamps/MappedTrembl6
		 */
		try{
			File fileWrite = new File("/scratch/usman/mappingCamps/MAPPEDCAMPS_GEPARD");	// tail trembl
			BufferedWriter wr = new BufferedWriter(new FileWriter(fileWrite));

			BufferedReader br = new BufferedReader(new FileReader("/scratch/usman/mappingCamps/filenames"));
			//every line is a file
			String line;
			while ((line = br.readLine()) != null){
				//each line has file name to join
				BufferedReader reader_mappedFile = new BufferedReader(new FileReader(line));
				String temp = "";
				while((temp = reader_mappedFile.readLine()) != null){
					wr.write(temp);
					wr.newLine();
				}
				reader_mappedFile.close();
			}
			br.close();
			wr.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}



	// The class makes mpf script files to run on grid and also makes parts of input file(uniprot) 200 seq each file	
	public void run(){
		try{

			//BufferedReader br = new BufferedReader(new FileReader("F:/yeast.fasta"));
			BufferedReader br = new BufferedReader(new FileReader("/scratch/usman/mappingCamps_tempFiles/tailTrembl1.fasta"));

			String sCurrentLine;
			int seqCounter = 0;
			int fileId =0;

			Boolean f = (new File("F:/test/tailsDir"+fileId)).mkdir();	// tail trembl
			//Boolean f = (new File("/scratch/usman/mappingCamps_tempFiles/tailsDir"+fileId)).mkdir();	// tail trembl

			//File fileWrite = new File("/scratch/usman/mappingCamps_tempFiles/tailsDir"+fileId+"/tail"+fileId+".fasta");	// tail trembl
			File fileWrite = new File("F:/test/tailsDir"+fileId+"/tail"+fileId+".fasta");	// tail trembl
			BufferedWriter wr = new BufferedWriter(new FileWriter(fileWrite));
			boolean flag = true;
			while ((sCurrentLine = br.readLine()) != null){
				if(sCurrentLine.startsWith(">")){
					seqCounter++;
					flag =true;
				}
				if (seqCounter%200 ==0 && flag == true){
					wr.close();
					System.out.print("File made for " + fileId + "\n");
					fileId++;

					//f = (new File("/scratch/usman/mappingCamps_tempFiles/tailsDir"+fileId)).mkdir();	// tail trembl
					//fileWrite = new File("/scratch/usman/mappingCamps_tempFiles/tailsDir"+fileId+"/tail"+fileId+".fasta");	// tail trembl

					f = (new File("F:/test/tailsDir"+fileId)).mkdir();	// tail trembl
					fileWrite = new File("F:/test/tailsDir"+fileId+"/tail"+fileId+".fasta");	// tail trembl

					wr = new BufferedWriter(new FileWriter(fileWrite));
					flag =false;
				}
				wr.write(sCurrentLine);
				wr.newLine();


			}
			br.close();
			wr.close();



		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	public void run2(){

		try {

			for (int i =0; i<=26; i++){

				File fileWrite = new File("F:/test/mpf"+i);	// tail trembl
				BufferedWriter wr = new BufferedWriter(new FileWriter(fileWrite));
				wr.write("#!/bin/sh");
				wr.newLine();
				//new line
				wr.write("#$-e /home/users/saeed/mpf"+i+".e");
				wr.newLine();
				wr.write("#$-o /home/users/saeed/mpf"+i+".o");
				wr.newLine();
				String flock = "/usr/bin/flock /scratch/usman/mappingCamps_tempFiles/tailsDir";
				wr.write(flock + i +" -c \'/usr/bin/rsync -avz /scratch/usman/mappingCamps_tempFiles/tailsDir"+i+"/ /tmp/tailsDir"+i+"/\'");
				wr.newLine();
				wr.write("java -jar /home/users/saeed/all_camps_scripts/maptouniprot_file.jar /tmp/tailsDir"+i+"/tail"+i+".fasta " + i);
				wr.newLine();
				wr.close();

			}




		}
		catch (Exception e){
			e.printStackTrace();
		}


	}

}
