package addMissingGenomes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utils.DBAdaptor;

public class GetSequencesOfMissingGenomes {

	/**
	 * @param args
	 * The class gets the protein sequences of all the seqids of missing viral genomes///
	 * uses the file ToAddSequences to red seqIds 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("Running...");
		String infile = "F:/Scratch/addMissingGenomes/run/ToAddSequences.txt";
		String outfile = "F:/Scratch/addMissingGenomes/run/SeqId2Seq.txt";

		run(infile,outfile);

	}

	private static void run(String infile, String outf) {
		// TODO Auto-generated method stub
		try{
			Connection connection = DBAdaptor.getConnection("camps4");
			PreparedStatement pstm = connection.prepareStatement("SELECT sequence from sequences2 where sequenceid=?");
			
			BufferedReader br = new BufferedReader(new FileReader(new File(infile)));

			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outf)));
			bw.write("#SeqId \t Sequence");
			bw.newLine();
			String l = "";
			while((l = br.readLine())!=null){
				if(!l.startsWith("#")){
					String[] parts = l.split("\t");
					int seqid = Integer.parseInt(parts[0].trim());
					pstm.setInt(1, seqid);
					ResultSet rs = pstm.executeQuery();
					String seq = "NOTFOUND";
					while(rs.next()){
						seq = rs.getString(1);
					}
					rs.close();
					pstm.clearParameters();
					
					if(seq.contains("NOTFOUND")||seq.isEmpty()){
						System.err.print("SeqNotFound");
						System.exit(0);
					}
					seq = seq.toUpperCase();
					bw.write(seqid+"\t"+seq);
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

}
