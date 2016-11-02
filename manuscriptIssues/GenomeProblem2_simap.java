package manuscriptIssues;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import utils.DBAdaptor;




public class GenomeProblem2_simap {

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4"); //changed to test_camps3
	/**
	 * @param args
	 * runs simap for given genome and extracts the hits
	 */
	
	private static HashMap<Integer,Integer> seqIdtoTMno = new HashMap<Integer,Integer>();
	
	private static HashMap<Integer,ArrayList<Integer>> blastOut = new HashMap<Integer,ArrayList<Integer>>(); // key is query and value is first 10 hits
	private static ArrayList<Integer> QueriesInBlastResults = new ArrayList<Integer>();
	private static ArrayList<Integer> Allquerys = new ArrayList<Integer>();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// since simap is not working any more... have to use blast
		// download seqences for all camps - sc cluster
		// download 1 genome make fasta
		// phase 1
		
		//System.out.println("Getting CAMPS Seq...");
		//getCAMPSSeq();
		
		//Integer taxid = 663202;
		//Integer taxid = 383855;
		//Integer taxid = 663331;
		//System.out.println("Getting Taxa Seq...");
		//getfilteredOutGenomeSeq(taxid); // gets the sequences for this taxid
		// blast format camps
		// run blast
		
		// So after running blast for the above taxid ... now have to parse the results and see how many proteins gave a hit
		// does the number of tm proteins in query and hit euqal or now
		
		// phase 2
		
		//String blastOutFile = "F:/Scratch/missingGenomes/blast/663202.blastout";
		//String blastOutFile = "F:/Scratch/missingGenomes/blast/383855.blastout";
		String blastOutFile = "F:/Scratch/missingGenomes/blast/663331.blastout";
		//String blastOutFile = args[0];
		parseBlast(blastOutFile);
		
		GetQuerysWithNoHits(blastOutFile+"_noHits.txt");
		
		//getTMsNumber();
		
		//CheckTMSandPrint();
		

	}

	private static void GetQuerysWithNoHits(String file) {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select entry_name,accession,description,subset from camps2uniprot where sequenceid=?");
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(file)));
			bw.write("#CAMPSId\tEntryName\tAccession\tDescription\tSource");
			bw.newLine();
			for(int i =0; i<=Allquerys.size()-1;i++){
				Integer id = Allquerys.get(i);
				if(!QueriesInBlastResults.contains(id)){
					//System.out.println(id);
					pstm.setInt(1, id);
					ResultSet rs = pstm.executeQuery();
					while(rs.next()){
						String entryName = rs.getString(1);
						String acc = rs.getString(2);
						String descrp = rs.getString(3);
						String source = rs.getString(4);
						System.out.println(id.toString()+"\t"+entryName+"\t"+acc+"\t"+descrp+"\t"+source);
						bw.write(id.toString()+"\t"+entryName+"\t"+acc+"\t"+descrp+"\t"+source);
						bw.newLine();
					}
				}
			}
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void CheckTMSandPrint() {
		// TODO Auto-generated method stub
		try{
			int count = 0;
			for(int i=0;i<=QueriesInBlastResults.size()-1;i++){
				int q = QueriesInBlastResults.get(i);
				int q_tm = seqIdtoTMno.get(q); // not of tm in query
				int hit = blastOut.get(q).get(0);
				int hit_tm = seqIdtoTMno.get(hit); // not of tm in hit
				if(q_tm!=hit_tm){
					count ++;
				}
			}
			System.out.println("Disagreeing tms: " + count +" out of "+ QueriesInBlastResults.size());
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void parseBlast(String blastOutFile) {
		// TODO Auto-generated method stub
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(blastOutFile)));
			String line = "";
			int hitcount = 0;
			//int query;
			ArrayList<Integer> hits = new ArrayList<Integer>();
			
			Integer query = 0;
			boolean asign = false;
			
			while((line = br.readLine())!=null){
				
				if(line.startsWith("# BLASTP 2.2.21")){
					// new entry
					//packup
					hitcount = 0;
					if(asign){
						blastOut.put(query, hits);
						asign = false;
						QueriesInBlastResults.add(query);
					}
				}
				else if(line.startsWith("# Query:")){
					String id = line.split(" ")[2].split("-")[0].trim();
					query = Integer.parseInt(id);
					
					Allquerys.add(query);
				}
				else if (line.startsWith("# Fields:") || line.startsWith("# Database:")){
					continue;
				}
				else{
					if(hitcount<=5){
						int hit = Integer.parseInt(line.split("\t")[1].trim());
						hits.add(hit);
						
						hitcount++;
						asign = true;
					}
					else{
						continue;
					}
				}
			}
			br.close();
			System.out.println("TotalQuerys: "+Allquerys.size());
			System.out.println("Querys with at-least 1 hit: "+QueriesInBlastResults.size());
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void getTMsNumber() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid from sequences2 where in_SC=\"Yes\"");
			ArrayList<Integer> ids = new ArrayList<Integer>();
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				Integer seqid = rs.getInt(1);
				ids.add(seqid);
				}
			rs.close();
			pstm.close();
			for(int x=0;x<=Allquerys.size()-1;x++){
				int xx = Allquerys.get(x);
				ids.add(xx);
			}
			System.out.println("Number of proteins to obtain tm size is "+ids.size());
			PreparedStatement ptms = CAMPS_CONNECTION.prepareStatement("select max(tms_id) from tms where sequenceid=?");
			
			for(int i =0;i<=ids.size()-1;i++){
				if(i%10000 == 0){
					//System.out.println(".");
				}
				int seqid = ids.get(i);
				ptms.setInt(1, seqid);
				ResultSet rtms = ptms.executeQuery();
				Integer TMNo= 0 ;
				while(rtms.next()){
					TMNo = rtms.getInt(1);
				}
				rtms.close();
				ptms.clearParameters();
				
				seqIdtoTMno.put(seqid,TMNo);
			}
				
			
			
			ptms.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void getfilteredOutGenomeSeq(Integer taxid) {
		// TODO Auto-generated method stub
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/Scratch/missingGenomes/"+taxid.toString()+".fasta")));
			
			PreparedStatement p = CAMPS_CONNECTION.prepareStatement("select sequenceid from proteins2 where taxonomyid=?");
			PreparedStatement pgetSeq = CAMPS_CONNECTION.prepareStatement("select sequence from sequences2 where sequenceid=?");
			
			p.setInt(1, taxid);
			ResultSet r = p.executeQuery();
			while(r.next()){
				Integer seqid = r.getInt(1);
				
				pgetSeq.setInt(1, seqid);
				ResultSet rs = pgetSeq.executeQuery();
				String seq = "";
				while(rs.next()){
					seq = rs.getString(1).toUpperCase();
				}
				rs.close();
				pgetSeq.clearParameters();
				bw.write(">"+seqid+"-"+taxid);
				bw.newLine();
				bw.write(seq);
				bw.newLine();
			}
			pgetSeq.close();
			
			r.close();
			p.close();
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void getCAMPSSeq() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement p = CAMPS_CONNECTION.prepareStatement("select sequenceid,sequence from sequences2 where in_SC=\"Yes\"");
			ResultSet r = p.executeQuery();
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/Scratch/missingGenomes/campsSequences.fasta")));
			while(r.next()){
				Integer seqid = r.getInt(1);
				String seq = r.getString(2).toUpperCase().trim();
				bw.write(">"+seqid);
				bw.newLine();
				bw.write(seq);
				bw.newLine();
			}
			r.close();
			p.close();
			bw.close();
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	

}

