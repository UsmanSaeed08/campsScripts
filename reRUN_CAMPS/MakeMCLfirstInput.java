package reRUN_CAMPS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DecimalFormat;

public class MakeMCLfirstInput {

	/**
	 * @param args
	 * The class makes the first input file for mcl at threshold e-5 in abc format
	 * This same file would also be used as the first dictionary. 
	 */
	private static String similarityMatrixFile = "/localscratch/CAMPS/camps_seq_file_filtered.matrix"; // running in localscratch of thor-10g
	private static String outFile = "/localscratch/CAMPS/thresh.005.abc";
	
	private static final DecimalFormat SIMILARITY_FORMAT = new DecimalFormat("0.000");

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("Making e-5 threshold file...");
		run();
	}

	private static void run() { // since we know that all the hits in alignments filtered are better than e-5 therefore.. dont need any comparison
		// TODO Auto-generated method stub
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(similarityMatrixFile)));
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outFile)));
			String line = "";
			int countexceptions = 0;
			while((line = br.readLine())!=null){
				String p[] = line.split("\t");
				int seqidq = Integer.parseInt(p[0].trim());
				int seqidhit = Integer.parseInt(p[1].trim());
				double eval;
				try{
					eval = Double.parseDouble(p[8].trim());
					bw.write(seqidq+" "+seqidhit+" "+SIMILARITY_FORMAT.format(-Math.log10(eval)));
					bw.newLine();
				}
				catch(NumberFormatException e){
					countexceptions++;
					System.out.print("Caught parsing exception for " + seqidq +" and "+ seqidhit +" at eval: " +p[8]);
					System.out.print("Exception Count: "+countexceptions);
					e.printStackTrace();
				}
			}
			br.close();
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
