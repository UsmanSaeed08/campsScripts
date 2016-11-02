package wokflow2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class JoinPdbMappedFiles {

	/**
	 * @param args
	 */
	private static HashMap <String, String> ListOfPdbTms = new HashMap<String,String>();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Gosh I want this work to finish");
		// get all the mapped files
		
		String path = "/home/users/saeed/scratch/mappingCampsToCATHandPDB/FinalMappedFiles/";
		String outfile = "/home/users/saeed/scratch/mappingCampsToCATHandPDB/MappedPdbtoCampsSC";
		String extension = ".pdbmap"; // with "."
		String pdbtmFile = "/home/users/saeed/scratch/mappingCampsToCATHandPDB/pdbtm_alpha.list";
		GetPdbTms(pdbtmFile);
		
		File folder = new File(path);
		ArrayList<String> files = listFilesForFolder(folder,extension);
		
		// join them
		join(files,outfile);

	}
	private static void GetPdbTms(String pdbtmFile) {
		// TODO Auto-generated method stub
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(pdbtmFile)));
			String line = "";
			while((line=br.readLine())!=null){
				line.trim();
				if(!line.contains(" ") && !line.isEmpty()){
					if(!ListOfPdbTms.containsKey(line)){
						ListOfPdbTms.put(line, "");
					}
				}
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void join(ArrayList<String> files, String outfile) {
		// TODO Auto-generated method stub
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outfile)));
			BufferedReader br;
			for(int i =0; i<=files.size()-1;i++){
				System.out.println("\n Joining File " + files.get(i) + " \n");
				br = new BufferedReader(new FileReader(new File(files.get(i))));
				String line = "";
				while((line = br.readLine())!=null){
					if (line != "" && !line.isEmpty()){
						// check if is a pdbtm or not
						String [] parts = line.split("\t");
						if(ListOfPdbTms.containsKey(parts[1].trim())){ // if is a pdbtm write to file - else Not
							line.trim();
							bw.write(line);
							bw.newLine();
						}
					}
				}
				br.close();
			}
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static ArrayList<String> listFilesForFolder(File folder,String ext) {
		int count = 0; 
		ArrayList<String> files_results = new ArrayList<String>();
		for (final File fileEntry : folder.listFiles()) {
			String fileName = fileEntry.getName();
			if (fileEntry.isDirectory()) {
				listFilesForFolder(fileEntry,ext);
			} 
			else if (fileName.endsWith(ext)) {
				count++;
				//files_results.add(fileEntry.getName().toString());
				files_results.add(fileEntry.getAbsolutePath());
			}
		}
		System.out.println("\n total file " + count + " \n");
		return files_results;
	}
}
