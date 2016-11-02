package manuscriptIssues;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class TmDistribution6 {

	/**
	 * @param args
	 * The class works on the files produced by TmDistribution5. 
	 * Those files contain all the rejected single tm proteins, and their total hits and 
	 * the number of hits rejected for each given reason. The stats are for two types of files
	 * 1. total hits and number of hits rejected for each reason
	 * 2. total hits and individual reason of hit rejection, after which other conditions may not be checked.
	 * 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// will use the indivisual reason of rejection file, because it correspond total hit count
		System.out.println("Running...");
		String f = "C:/Users/Usman Saeed/Documents/CAMPS_PaperDraft/results/Tmdistribution/reRun/thedistributionProblem/rejectedHits/RejectedHitsFound_Individual_1_TMProteins.matrix";
		run(f);
	}
	private static void run(String f){
		try{
			//ProteinId 	 TotalHits 	 RejectByEvalue 	 RejectByAlignmentCoverage 	 RejectByTMCoverageQuery 	 RejectByTMCoverageHit
			BufferedReader br = new BufferedReader(new FileReader(new File(f)));
			String l = "";
			long totalhits = 0;

			int ProteinsRejectedByEval = 0;
			int ProteinsRejectedByAln = 0;
			int ProteinsRejectedByTmCover = 0;
			int c = 0;
			while((l = br.readLine())!=null){
				if(!l.startsWith("#")){
					c++;
					String p[] = l.split("\t");
					int hits = Integer.parseInt(p[1].trim());
					int eval = Integer.parseInt(p[2].trim());
					int AlnCoverage = Integer.parseInt(p[3].trim());
					int TmCoverageQ = Integer.parseInt(p[4].trim());
					int TmCoverageH = Integer.parseInt(p[5].trim());

					totalhits = totalhits + hits;

					if (eval>0){
						ProteinsRejectedByEval++;
					}
					if (AlnCoverage>0){
						ProteinsRejectedByAln++;
					}
					if (TmCoverageQ > 0 || TmCoverageH > 0){
						ProteinsRejectedByTmCover++;
					}
				}
			}

			System.out.println("Proteins Rejected by Eval \t"+ProteinsRejectedByEval);
			System.out.println("Proteins Rejected by Alignment Coverage \t"+ProteinsRejectedByAln);
			System.out.println("Proteins Rejected by TM Coverage \t"+ProteinsRejectedByTmCover);
			System.out.println("Total Hits \t"+totalhits);
			System.out.println("Number of Proteins \t"+c);
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
