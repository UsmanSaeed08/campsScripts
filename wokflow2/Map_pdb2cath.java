package wokflow2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;

public class Map_pdb2cath {

	/**
	 * @param args
	 */

	/*
	 * 		

CathFile:

1oaiA00     1    10     8    10     1     1     1     1     1    59 1.000
1go5A00     1    10     8    10     1     1     1     1     2    69 999.000
3frhA01     1    10     8    10     2     1     1     1     1    58 1.200

Pdb2Camps File:

19821	2gfp_B	28.9	1.47E-41	18	395	2	3732gfp Multidrug resistance protein D
19821	2gfp_A	28.9	1.47E-41	18	395	2	3732gfp Multidrug resistance protein D
17074	2gfp_B	28.1	3.42E-40	18	382	2	3642gfp Multidrug resistance protein D
17074	2gfp_A	28.1	3.42E-40	18	382	2	3642gfp Multidrug resistance protein D

To do:
Have to get cath classification for each pdb sequence?
pdb1 1 -- seqid --> find 
problem: 1 pdb can have multiple folds

	 * 
	 * 
	 * 
	 */
	private static HashMap <String, String> CathDomainList = new HashMap<String,String>();

	private static HashMap <String, String> CathFoldsMappedToCamps = new HashMap<String,String>();
	private static HashMap <String, String> CampsSeqIdsMappedToPDB = new HashMap<String,String>();
	private static HashMap <String, String> CampsSeqMappedToCATH = new HashMap<String,String>();
	private static HashMap <String, String> PdbChainsMappedToCamps = new HashMap<String,String>();
	private static HashMap <String, String> DescriptionCath = new HashMap<String,String>();


	// --> have to add description from names file

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// the programming is odne.. now get the stats for paper when u populate it.. like how many unique pdbs.. at what thresholds
		// what are the number of unique cath folds etc

		System.out.println("Statring Now");
		// so have two files
		// poulate cath file
		/*		
		String NamesFileForDescrition = "F:/Scratch/mappingCampsToCATHandPDB/CathNames.v4.0.0.txt";
		String pathtoCathFile = "F:/Scratch/mappingCampsToCATHandPDB/CathDomainList.v4.0.0";
		String pathtoCampsPdbMapFile = "F:/Scratch/mappingCampsToCATHandPDB/MappedPdbtoCampsSC";
		String outfile_map = "F:/Scratch/mappingCampsToCATHandPDB/Camps2Cath_Min30Identity.txt";
		 */
		// 
		//using below paths for reRun
		//F:\Scratch\reRunCAMPS_CATH_PDB\cath
		String NamesFileForDescrition = "F:/Scratch/reRunCAMPS_CATH_PDB/cath/CathNames.v4.0.0.txt";
		String pathtoCathFile = "F:/Scratch/reRunCAMPS_CATH_PDB/cath/CathDomainList.v4.0.0";
		String pathtoCampsPdbMapFile = "F:/Scratch/reRunCAMPS_CATH_PDB/reRUNCAMPSSeqToPDBTMOnly.txt";
		String outfile_map = "F:/Scratch/reRunCAMPS_CATH_PDB/reRUNCamps2Cath_Min30Identity.txt";


		GetDescriptions(NamesFileForDescrition);
		GetCathFile(pathtoCathFile);
		MapPdbTOCath(pathtoCampsPdbMapFile,outfile_map);

		System.out.println("CathFoldsMappedToCamps "+CathFoldsMappedToCamps.size());
		System.out.println("CampsSeqMappedToCATH "+CampsSeqMappedToCATH.size());

		System.out.println("CampsSeqIdsMappedToPDB "+CampsSeqIdsMappedToPDB.size());
		System.out.println("PdbChainsMappedToCamps "+PdbChainsMappedToCamps.size());

	}

	private static void GetDescriptions(String namesFileForDescrition) {
		// TODO Auto-generated method stub
		try{
			String line = "";
			BufferedReader br = new BufferedReader(new FileReader(new File(namesFileForDescrition)));
			while((line = br.readLine())!=null){
				if(!line.startsWith("#")){
					line = line.trim();
					String [] parts = line.split(":");
					if(parts.length>1){	// because if description is empty the parts length is 1
						if(!parts[1].trim().isEmpty()){
							String family = parts[0].split(" ")[0].trim();
							DescriptionCath.put(family, parts[1].trim());
						}
					}
				}
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
			System.exit(0);
		}
	}

	private static void MapPdbTOCath(String pathtoCampsPdbMapFile, String outfile) {
		// TODO Auto-generated method stub
		String line = "";
		String cathLine = "";
		try{
			// campsid pdbid identity eval qst qend hitst hitend description
			// 19821	2gfp_B	28.9	1.47E-41	18	395	2	3732gfp Multidrug resistance protein D
			BufferedReader br = new BufferedReader(new FileReader(new File(pathtoCampsPdbMapFile)));
			BufferedWriter bw = new BufferedWriter (new FileWriter(new File(outfile)));
			int c = 0;			
			//String line = "";
			while((line = br.readLine())!=null){
				line = line.trim();
				if(!line.isEmpty()){
					String parts[] = line.split("\t");
					String campsId = parts[0].trim();
					String PdbId  = parts[1].trim();
					String description = parts[7].trim();

					Float Identity = Float.parseFloat(parts[2].trim());
					if(Identity > 29){
						//String description = parts[6].trim();
						// get some stats
						if(!CampsSeqIdsMappedToPDB.containsKey(campsId)){
							CampsSeqIdsMappedToPDB.put(campsId, null);
						}
						if(!PdbChainsMappedToCamps.containsKey(PdbId)){
							PdbChainsMappedToCamps.put(PdbId, null);
						}				
						if(CathDomainList.containsKey(PdbId)){
							c++;
							// write to file
							cathLine = CathDomainList.get(PdbId);
							if(cathLine.contains("#")){
								// multi domain
								String cathLines[] = cathLine.split("#");
								for(int i = 0; i < cathLines.length; i++){
									String toProcess = cathLines[i].trim();
									// single white space problem 
									//String parsedString[] = toProcess.split(" ");
									String parsedStringWithGaps[] = toProcess.split(" ");;
									String parsedString[] = new String[13];
									int x = 0;
									for(int j = 0; j < parsedStringWithGaps.length; j++){
										parsedStringWithGaps[j] = parsedStringWithGaps[j].trim();

										if(!parsedStringWithGaps[j].isEmpty()){
											parsedString[x] = parsedStringWithGaps[j];
											x++;
										}
									}
									String domain = parsedString[0].trim();
									String classification = parsedString[1].trim() +"."+parsedString[2].trim()+"."+parsedString[3].trim()+"."+parsedString[4].trim();
									String domainLen = parsedString[10].trim();
									String resolution = parsedString[11].trim();
									if(DescriptionCath.containsKey(classification)){
										String d = DescriptionCath.get(classification);
										if(!d.isEmpty()){
											description = d;
										}
									} 
									String fold = parsedString[1].trim() +"."+parsedString[2].trim()+"."+parsedString[3].trim();
									String l = campsId + "\t" + PdbId + "\t" + classification + "\t" + domain + "\t" + domainLen + "\t" + resolution + "\t" + description;
									bw.write(l);
									bw.newLine();
									if(!CampsSeqMappedToCATH.containsKey(campsId)){
										CampsSeqMappedToCATH.put(campsId, null);
									}
									if(!CathFoldsMappedToCamps.containsKey(fold)){
										CathFoldsMappedToCamps.put(fold, null);
									}
								}
							}
							else{
								// simple case online one line.. just split and put it in file
								System.out.println(c);
								String parsedStringWithGaps[] = cathLine.split(" ");
								String parsedString[] = new String[13];
								int x = 0;
								for(int j = 0; j < parsedStringWithGaps.length; j++){
									parsedStringWithGaps[j] = parsedStringWithGaps[j].trim();
									if(!parsedStringWithGaps[j].isEmpty()){
										parsedString[x] = parsedStringWithGaps[j];
										x++;
									}
								}
								String domain = parsedString[0].trim();
								String classification = parsedString[1].trim() +"."+parsedString[2].trim()+"."+parsedString[3].trim()+"."+parsedString[4].trim();
								String domainLen = parsedString[10].trim();
								String resolution = parsedString[11].trim();
								if(DescriptionCath.containsKey(classification)){
									String d = DescriptionCath.get(classification);
									if(!d.isEmpty()){
										description = d;
									}
								}
								String fold = parsedString[1].trim() +"."+parsedString[2].trim()+"."+parsedString[3].trim();
								String l = campsId + "\t" + PdbId + "\t" + classification + "\t" + domain + "\t" + domainLen + "\t" + resolution+ "\t" + description;
								bw.write(l);
								bw.newLine();
								if(!CampsSeqMappedToCATH.containsKey(campsId)){
									CampsSeqMappedToCATH.put(campsId, null);
								}
								if(!CathFoldsMappedToCamps.containsKey(fold)){
									CathFoldsMappedToCamps.put(fold, null);
								}
							}
						}
						if(c%100 == 0){
							bw.flush();
							System.out.println("Processed Lines "+ c);
						}
					} // if of the identity
				}
			}	
			br.close();
			bw.close();
		}
		catch(Exception e){
			System.err.println(line);
			System.err.println(cathLine);
			e.printStackTrace();
		}
	}

	private static void GetCathFile(String pathtoCathFile) {
		// TODO Auto-generated method stub
		try{
			int count =0;
			//1oaiA00     1    10     8    10     1     1     1     1     1    59 1.000
			BufferedReader br = new BufferedReader(new FileReader(new File(pathtoCathFile)));
			String line = "";
			while((line = br.readLine())!=null){
				if(!line.startsWith("#")){
					line = line.trim();
					String parts[] = line.split("    ");
					String domain = parts[0].trim();
					//line =line.replaceAll("    ", ":");
					String pdb = domain.substring(0, 4);
					String chain = domain.substring(4, 5);
					String key = pdb+"_"+chain;
					key.trim();
					if(CathDomainList.containsKey(key)){
						//flag
						System.err.print("multiple pdb\n");
						count++;
						String temp = CathDomainList.get(key);
						temp = temp + "#" + line;
						temp = temp.trim();
						CathDomainList.put(key,temp);
					}
					else{
						//CathDomainList.put(pdb, line);
						CathDomainList.put(key, line);
					}
				}
			}
			br.close();
			System.out.println("Total unique pdbs" + CathDomainList.size());
			System.out.println("Multi domain "+ count);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
