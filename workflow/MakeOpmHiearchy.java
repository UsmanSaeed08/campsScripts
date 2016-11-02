package workflow;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;

public class MakeOpmHiearchy {

	/**
	 * @param args
	 * Makes the opm hiearchy file for input into hierchy table
	 * This file is made from data available on opm website
	 * The tcdb is used to map opm proteins and get a description
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("Hello");
		makegpcrdb_hierarchy();
		System.exit(0);
		
		ArrayList<String> processed = new ArrayList<String>();
		try{
			BufferedReader brOpm = new BufferedReader(new FileReader(new File("F:/Scratch/opm_tm.txt"))); // parse required
			BufferedReader brOpmpoly = new BufferedReader(new FileReader(new File("F:/Scratch/opm_polytopic.txt"))); // direct to file copy
			BufferedReader brOpmbi = new BufferedReader(new FileReader(new File("F:/Scratch/opm_bitopic.txt")));  // direct to file copy

			BufferedWriter wr = new BufferedWriter(new FileWriter(new File("F:/Scratch/opm_hiearchy.txt")));  // direct to file copy
			// first copy the no parse required
			wr.write("#Type\tHierchy\tDescription");
			wr.newLine();
			String line = "";			

			// process bitopic file
			while((line = brOpmbi.readLine())!= null){

				String[] l = line.split("\t");
				if (l[0].contains("1.3")) // skip beta
					continue;
				if(!processed.contains(l[0].trim())){
					processed.add(l[0]);
					wr.write("Superfamily\t"+l[0].trim()+"\t"+l[1].trim());
					wr.newLine();
				}
			}
			brOpmbi.close();
			// process polytopic file
			while((line = brOpmpoly.readLine())!= null){
				String[] l = line.split("\t");
				if (l[0].contains("1.3")) // skip beta
					continue;

				String type = "";
				int typint = getType(l[0]);
				if (typint == 2){
					type = "Class";
					wr.write(type+"\t"+l[0]+"\t"+l[1]);
					wr.newLine();
				}
				else if (typint == 3){
					type = "Superfamily";
					if(!processed.contains(l[0].trim())){
						processed.add(l[0]);
						wr.write(type+"\t"+l[0]+"\t"+l[1]);
						wr.newLine();
					}
				}
			}
			brOpmpoly.close();
			// process last family file
			while((line = brOpm.readLine())!= null){
				String[] l = line.split("\t");
				if (l[0].contains("1.3.")) // skip beta
					continue;

				String type = "Family";
				String pdb = l[2];
				String hiearchy = l[0];
				String description = findDescript(pdb.toUpperCase());
				if(!processed.contains(hiearchy.trim())){
					processed.add(hiearchy);
					wr.write(type+"\t"+hiearchy+"\t"+description);
					wr.newLine();
				}
			}
			brOpm.close();
			wr.close();	
		}
		catch(Exception e){
			e.printStackTrace();
		}	
	}

	private static void makegpcrdb_hierarchy() {
		// TODO Auto-generated method stub
		try{
			BufferedReader brGpcr = new BufferedReader(new FileReader(new File("F:/Scratch/gpcr_hierarchies.txt")));
			BufferedWriter wrGpcr = new BufferedWriter(new FileWriter(new File("F:/Scratch/gpcr_hierarchy_formatted.txt")));
			String line = "";
			ArrayList<String> done = new ArrayList<String>();
			
			while((line = brGpcr.readLine())!= null){
				String[] l = line.split("\t");
				String key = l[1].trim();
				String des = l[2].trim();				
				if(!done.contains(l[1])){
					done.add(key);
					wrGpcr.write("No_Hierarchy"+"\t"+key+"\t"+des);
					wrGpcr.newLine();
				}
			}
			brGpcr.close();
			wrGpcr.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static String findDescript(String pdb) {
		// TODO Auto-generated method stub
		String descript = "-";
		try{
			BufferedReader brTcdb = new BufferedReader(new FileReader(new File("F:/Scratch/tcdbAndpdb.tsv")));
			String line = "";			
			// find this pdb in tcdb file
			while((line = brTcdb.readLine())!= null){
				String[] l = line.split("\t");
				if (l[0].contains(pdb)){
					descript = l[2].trim()+"**"+l[1].trim();
					break;
				}
			}
			brTcdb.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return descript;
	}

	private static int getType(String x){
		int count = 0;
		for(int i =0;i<=x.length()-1;i++){
			char a = x.charAt(i);
			if(a == '.')
				count++;
		}
		return count;
	}

}
