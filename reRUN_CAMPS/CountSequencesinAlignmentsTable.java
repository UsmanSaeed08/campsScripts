package reRUN_CAMPS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

import utils.DBAdaptor;

public class CountSequencesinAlignmentsTable {

	/**
	 * @param args
	 */
	private static HashMap<Integer,Integer> seqid2taxid = new HashMap<Integer,Integer>();
	private static ArrayList<Integer> taxids = new ArrayList<Integer>();
	
	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4"); //changed to test_camps3
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Running");
		String similarityFile = "/localscratch/CampsSimilarityScores/camps_seq_file_filtered.matrix";
		getSeqid2Taxid();
		ReadSimilarityFile(similarityFile);
	}
	private static void ReadSimilarityFile(String file){
		try{
			HashMap<Integer,Integer> inalignments = new HashMap<Integer,Integer>();
			HashMap<Integer,Integer> taxainalignments = new HashMap<Integer,Integer>();
			
			BufferedReader br = new BufferedReader(new FileReader(new File(file)));
			String line = "";
			while((line = br.readLine())!=null){
				String p[] = line.split("\t");
				int seqidq = Integer.parseInt(p[0].trim());
				int seqidhit = Integer.parseInt(p[1].trim());
				
				int taxidQ = seqid2taxid.get(seqidq);
				int taxidH = seqid2taxid.get(seqidhit);
				
				if(!inalignments.containsKey(seqidq)){
					inalignments.put(seqidq, null);
				}
				if(!inalignments.containsKey(seqidhit)){
					inalignments.put(seqidhit, null);
				}
				if(!taxainalignments.containsKey(taxidQ)){
					taxainalignments.put(taxidQ, null);
				}
				if(!taxainalignments.containsKey(taxidH)){
					taxainalignments.put(taxidH, null);
				}
			}
			br.close();
			System.out.println("Total Sequences: "+ inalignments.size());
			System.out.println("Total taxas in alignments: "+ taxainalignments.size());
			ArrayList<Integer> missing = new ArrayList<Integer>();
			for(int i =0;i<=taxids.size()-1;i++){
				int t = taxids.get(i);
				if(!taxainalignments.containsKey(t)){
					if(!missing.contains(t)){
						missing.add(t);
					}
				}
			}
			System.out.println("Number of missing taxa: "+ missing.size());
			
			if(missing.size()>0){
				for(int i =0;i<=missing.size()-1;i++){
					int t = missing.get(i);
					PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select taxonomy,superkingdom from taxonomies2 where taxonomyid=?");
					pstm1.setInt(1, t);
					ResultSet rs = pstm1.executeQuery();
					while(rs.next()){
						System.out.println("Missing taxa:"+t+":"+rs.getString(1)+"\t"+rs.getString(1));
					}
					rs.close();
					pstm1.close();
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void getSeqid2Taxid(){
		try{
			
			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement("select sequenceid,taxonomyid from proteins2 " +
					"where taxonomyid in (select taxonomyid from taxonomies2)"); // taxid 164328 is an exception.. it is fungi like

			ResultSet rs1 = pstm1.executeQuery();
			while(rs1.next()){
				Integer seqid = rs1.getInt(1);
				Integer taxid = rs1.getInt(2);
				if(!seqid2taxid.containsKey(seqid)){
					seqid2taxid.put(seqid, taxid);
					if(!taxids.contains(taxid)){
						taxids.add(taxid);
					}
				}
			}
			rs1.close();
			pstm1.close();
			System.out.println("Initial Size: "+ seqid2taxid .size());
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
