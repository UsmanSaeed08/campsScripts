package mdClusterIssue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

public class CompareAlignments {

	/**
	 * @param args
	 * This class compares the alignments of SC clusters between CAMPS3 and CAMPS2
	 * However, One issue is that for clusters with > 400 members, the alignment is 
	 * for only the 400 most divergent members. 
	 */
	private static Integer gaps =0;
	private static Integer mAndmm =0;
	private static Integer NumberOfSeq =0;
	private static long NumberOfResidues =0;
	private static String clus;
	private static Integer CountMatchGreaterThan30Camps3 = 0;
	private static Integer CountMatchGreaterThan30Camps2 = 0;
	private static Integer TotalNumberOfSeq3 = 0;
	private static Integer TotalNumberOfSeq2 = 0;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("Ssshh\n");
		//list files in folder for camps3
		///scratch/usman/CAMPSAlignments/scClusts -- camps3
		//F:\Scratch\CampsAlignments\Camps3Alignments\CAMPSAlignments\scClusts
		//F:\Scratch\CampsAlignments\Camps2Alignments\Camps2Alignments
		String camp3 = "F:/Scratch/CampsAlignments/Camps3Alignments/CAMPSAlignments/scClusts/";

		String camp2 = "F:/Scratch/CampsAlignments/Camps2Alignments/Camps2Alignments/"; 

		ArrayList<String> c1 = listFilesForFolder(new File(camp3));

		System.out.print("\nCAMPS 3 processing \n");
		for(int i =0;i<=c1.size()-1;i++){
			//
			CountGapsandM(c1.get(i));
			//clusterName+NumberOfResidues+NumberOfSeq+Gaps+Match or MM
			System.out.print(clus+"\t"+NumberOfResidues+"\t"+NumberOfSeq+"\t"+ gaps+"\t"+ mAndmm+"\n");
			Float fMatch = ((float)gaps/(float)NumberOfResidues)*100f;
			if(fMatch>=30f){
				CountMatchGreaterThan30Camps3++;
			}
			TotalNumberOfSeq3 = TotalNumberOfSeq3 + NumberOfSeq; 
			gaps =0;
			mAndmm =0;
			NumberOfSeq =0;
			NumberOfResidues =0;
		}

		System.out.print("\nCAMPS 2 processing \n");
		//list files in folder for camps2
		ArrayList<String> c2 = listFilesForFolder(new File(camp2));
		// for every cluster calculate gaps and matches for camps3
		for(int i =0;i<=c2.size()-1;i++){
			//
			CountGapsandMCamps2(c2.get(i));
			//clusterName+NumberOfResidues+NumberOfSeq+Gaps+Match or MM
			System.out.print(clus+"\t"+NumberOfResidues+"\t"+NumberOfSeq+"\t"+ gaps+"\t"+ mAndmm+"\n");
			Float fMatch = ((float)gaps/(float)NumberOfResidues)*100f;
			if(fMatch>=25f){
				CountMatchGreaterThan30Camps2++;
			}
			TotalNumberOfSeq2 = TotalNumberOfSeq2 + NumberOfSeq;

			gaps =0;
			mAndmm =0;
			NumberOfSeq =0;
			NumberOfResidues =0;
		}

		System.out.print("\n\n");
		System.out.print("Number of clusters in CAMPS3 with gaps >25 are: "+CountMatchGreaterThan30Camps3+" and inTotalSeq: "+TotalNumberOfSeq3+"\n");
		System.out.print("Number of clusters in CAMPS2 with gaps >25 are: "+CountMatchGreaterThan30Camps2+" and inTotalSeq: "+TotalNumberOfSeq2+"\n");

	}

	public static ArrayList<String> listFilesForFolder(final File folder) {
		ArrayList<String> files_results = new ArrayList<String>();
		for (final File fileEntry : folder.listFiles()) {
			if(fileEntry.getName().endsWith(".aln")){
				files_results.add(fileEntry.getAbsolutePath());
			}
		}
		return files_results;
	}

	private static void CountGapsandM(String x){ // counts the gaps and number of matches or mismatches in a cluster
		try{
			File f = new File(x);
			BufferedReader br = new BufferedReader(new FileReader( f));
			clus = f.getName();
			String l = "";
			while((l=br.readLine())!=null){
				if(l.startsWith(">")){
					NumberOfSeq ++;
				}
				else{
					for(int i=0;i<=l.length()-1;i++){
						NumberOfResidues++;
						if(l.charAt(i) == '-'){
							gaps ++;
						}
						else{
							mAndmm++;
						}
					}
				}
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void CountGapsandMCamps2(String x){ // counts the gaps and number of matches or mismatches in a cluster
		try{
			HashMap<String,String> ids = new HashMap<String,String>();

			File f = new File(x);
			BufferedReader br = new BufferedReader(new FileReader( f));
			clus = f.getName();
			String l = "";
			while((l=br.readLine())!=null){
				if(!(l.contains("CLUSTAL 2.05")) && !(l.isEmpty())){
					String[] l2 = l.split(" ");
					if(l2.length>0){
						if(!l2[0].isEmpty()){
							if(!ids.containsKey(l2[0].trim())){
								ids.put(l2[0].trim(), "");
							}
						}
					}
					for(int j =1;j<=l2.length-1;j++){
						l2[j] = l2[j].trim();
						if(!l2[j].contains(" ")){
							if(!l2[j].isEmpty()){
								for(int i=0;i<=l.length()-1;i++){
									NumberOfResidues++;
									if(l.charAt(i) == '-'){
										gaps ++;
									}
									else{
										mAndmm++;
									}
								}
							}
						}
					}

				}
			}
			NumberOfSeq = ids.size()-1;

			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}

