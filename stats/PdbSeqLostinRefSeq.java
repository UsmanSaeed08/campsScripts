package stats;

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

import mips.gsf.de.simapclient.client.SimapAccessWebService;
import mips.gsf.de.simapclient.datatypes.HitProtein;
import mips.gsf.de.simapclient.datatypes.HitSet;
import mips.gsf.de.simapclient.datatypes.ResourceTypes;
import mips.gsf.de.simapclient.datatypes.ResultParser;

import utils.DBAdaptor;

public class PdbSeqLostinRefSeq {

	/**
	 * @param args
	 */
	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");

	public static ArrayList <String> InCamps = new ArrayList<String>();// key is fh clusid and value is arraylist of seqids
	public static HashMap <String, String> NotInCamps = new HashMap<String, String>();// key is fh clusid and value is arraylist of seqids
	public static ArrayList <String> NotInCamps_IdArray = new ArrayList <String>();// key is fh clusid and value is arraylist of seqids

	public static HashMap <String, String> FastaFile = new HashMap<String, String>();// key is id and value is seq
	private static BufferedWriter bw ;




	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("\ncheck\n");

		RunSimapScrap2();
		System.exit(0);

		//String pdbFile = "F:/Scratch/pdbtm_alpha.seq";
		String pdbFile = "/home/proj/check/otherdatabase/pdbtm/pdbtm_alpha.seq";

		try{
			//bw = new BufferedWriter(new FileWriter(new File("F:/Scratch/pdbtmsNotInRefSeq")));
			bw = new BufferedWriter(new FileWriter(new File("/home/users/saeed/pdbtmsNotInRefSeq_pedant3")));
		}
		catch(Exception e){
			e.printStackTrace();
		}

		// get pdb sequences in camps
		Get_seqInCamps();

		// read the input fasta file
		ReadPdbFasta(pdbFile);

		// get pdb sequences not in camps by compairing sequences in camps and all pdb
		Get_NotInCamps();
		try{
			for(int i =0;i<=NotInCamps_IdArray.size()-1;i++){
				String id = NotInCamps_IdArray.get(i);
				String seq = NotInCamps.get(id);
				RunSimap(seq,id);
				if (i%50==0){
					System.out.println("Processing "+i + " of " +NotInCamps_IdArray.size()+"\n");
					bw.flush();
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}

		// ******* outPut *******
		System.out.print("Number of total PDBs "+FastaFile.size()+"\n");
		System.out.print("Number of PDBs In CAMPS "+InCamps.size()+"\n");
		System.out.print("Number of PDB's to be tested "+NotInCamps.size()+"\n");
		System.out.print("Number of PDB's to be tested acc to array "+NotInCamps_IdArray.size()+"\n");

		// close connection
		closeConnection();
	}



	private static void RunSimapScrap2() {
		// TODO Auto-generated method stub
		try {
			SimapAccessWebService simap=new SimapAccessWebService();
			String se = "MTNIRKSHPLMKIVNNAFIDLPAPSNISSWWNFGSLLGICLILQILTGLFLAMHYTSDTTTAFSSVTHICRDVNYGWIIRYMHANGASMFFICLYMHVGRGLYYGSYTFLETWNIGVILLLTVMATAFMGYVLPWGQMSFWGATVITNLLSAIPYIGTNLVEWIWGGFSVDKATLTRFFAFHFILPFIIMAIAMVHLLFLHETGSNNPTGISSDVDKIPFHPYYTIKDILGALLLILALMLLVLFAPDLLGDPDNYTPANPLNTPPHIKPEWYFLFAYAILRSIPNKLGGVLALAFSILILALIPLLHTSKQRSMMFRPLSQCLFWALVADLLTLTWIGGQPVEHPYITIGQLASVLYFLLILVLMPTAGTIENKLLKW";
			//String se = "MGRLPFPADAELTGPDRLVLSVMQKQYEVHELTadvdaaeesddsaKKRSEFVAPSPediqdqeqqaieYLKNLNDPKSNSFEPTVFSSVDLPDIKNAFFHDNIVLPYTKWARTVVRNEADVVMITHLLLYSCTTLPSAIYLFYNFHWWHAILHGVMQGWYAGAYTLLKHQHIHARGVLSKKYAVVDELFPYVLDPLMGHTWNSYYFHHVKHHHVEGNGPNDLSSTVRYQRDDVWDFLKYVGRFYFLIWFDLPRYFLRTRKPMMALKAGGCEIGNYAFLYFLFNYVNTRATIFVFLIPLALMRLALMTGNWGQHALVDELEPDSDFRSSITLIDVPSNRYCYNDGYHTSHHLNPLRHWREHPVAFLSQKKQYSDEHALVFYNIDYMFLTINLLRKNYDHVARCLVPMGAQMNLTHEERVAMLKRKTRKFTEEEIAAKWGKQYARLK";
			se = se.toUpperCase();
			String md5=simap.computeMD5(se);
			// Setting up the database Id
			//int dbid=474;
			int dbid = 0;
			// we get all available databases to look up the dataset-id we want
			
			ArrayList<Hashtable> databases=simap.getAllDatabases();
			for (Hashtable mydatab : databases) {
				System.out.println(mydatab.get("taxon_id")+"\t"+mydatab.get("name")+"\t"+mydatab.get("id")+"\t"+mydatab.get("source"));
				System.out.println(mydatab.get("name")+"\t"+mydatab.get("id")+"\t"+mydatab.get("source"));
				if (mydatab.get("name").equals("PDB")) {
					System.out.println(mydatab.get("id"));
					dbid=new Integer((String)mydatab.get("id"));
					break;
				}
			}
			
			simap.addDatabase(dbid);

			simap.setMd5(md5);
			simap.setMax_evalue(1e-30);
			//simap.setMax_number_hits(10);
			simap.alignments(true);
			simap.sequences(true);

			//String xm = simap.getHitsXML();
			ArrayList<HitSet> result = new ArrayList<HitSet>();
			try{
				result =ResultParser.parseResult(simap.getHitsXML());
			}
			catch(Exception e){
				System.out.print("No Result Found\n");
			}
			if(result.size()>=1){
				for(int i =0;i<=result.size()-1;i++){
					System.out.println("Printing Result "+i+" of "+result.size());
					HitSet second=result.get(i);

					double identity = second.getHitAlignment().getIdentity();
					System.out.println("Identity = "+identity);

					double eval = second.getHitAlignment().getEvalue();
					System.out.println("Eval = "+eval);

					int queryStart = second.getHitAlignment().getQuery_start();
					int queryEnd = second.getHitAlignment().getQuery_stop();
					int hitStart = second.getHitAlignment().getHit_start();
					int hitEnd = second.getHitAlignment().getHit_stop();
					System.out.println("from: "+queryStart+" to "+queryEnd+ " in query sequence");
					System.out.println("from: "+hitStart+" to "+hitEnd+ " in hit sequence");

					int x =0;
					for (HitProtein o : second.getHitData().getProteins()) {
						//System.out.println(o.getTitle()+" Description "+o.getDatabase_description()+" Tax-Node"+o.getTax_node());
						x++;
						System.out.println("Hit number "+ x);
						//System.out.println("Protein in PdbTm and Not in RefSeq: "+ id);
						System.out.println(o.getTitle());
						System.out.println(o.getSequence_id());
						System.out.println(o.getDatabase_name()+"-- "+o.getDescription());


					}
					System.out.println("----");
				}
				BufferedWriter br = new BufferedWriter(new FileWriter(new File("F:/simap.xml")));
				br.write(simap.getHitsXML());
				br.close();
			}
		} catch (Exception e){
			e.printStackTrace();
		}
	}



	private static void RunSimap(String se, String id){

		try {
			SimapAccessWebService simap=new SimapAccessWebService();
			//String se = "MTNIRKSHPLMKIVNNAFIDLPAPSNISSWWNFGSLLGICLILQILTGLFLAMHYTSDTTTAFSSVTHICRDVNYGWIIRYMHANGASMFFICLYMHVGRGLYYGSYTFLETWNIGVILLLTVMATAFMGYVLPWGQMSFWGATVITNLLSAIPYIGTNLVEWIWGGFSVDKATLTRFFAFHFILPFIIMAIAMVHLLFLHETGSNNPTGISSDVDKIPFHPYYTIKDILGALLLILALMLLVLFAPDLLGDPDNYTPANPLNTPPHIKPEWYFLFAYAILRSIPNKLGGVLALAFSILILALIPLLHTSKQRSMMFRPLSQCLFWALVADLLTLTWIGGQPVEHPYITIGQLASVLYFLLILVLMPTAGTIENKLLKW";
			String md5=simap.computeMD5(se);
			//String md5="d675f14e17db67a63bfe360524dc2aeb";
			simap.setMd5(md5);
			//simap.setMax_evalue(10e-25);
			simap.setMax_number_hits(10);


			simap.alignments(true);
			simap.sequences(true);

			//PEDANT3_INCOMPLETE_GENOMES=13
			//PEDANT3_COMEPLETE_GENOMES
			//simap.addSource(ResourceTypes.PEDANT3_COMEPLETE_GENOMES);
			simap.addSource(ResourceTypes.PEDANT3_INCOMPLETE_GENOMES);
			//simap.addSource(ResourceTypes.PEDANT2_INCOMPLETE_GENOMES);


			ArrayList<HitSet> result =ResultParser.parseResult(simap.getHitsXML());

			if(result.size()>=1){
				// we print out the second best hit
				for(int i =0;i<=result.size()-1;i++){
					//System.out.print("\n");
					//System.out.println("----");
					bw.newLine();
					bw.newLine();
					bw.write("----");
					bw.newLine();

					HitSet second=result.get(i);
					double identity = second.getHitAlignment().getIdentity();
					double eval = second.getHitAlignment().getEvalue();
					//if (identity>=70 && eval < 1e-30){
					//if (identity>=40){
					if (true){
						/*
						System.out.println("Identity\t"+identity+" %");
						System.out.println("E-Value\t"+eval);

						// print out data of the protein instances
						System.out.println("Instances:");
						bw.write("Identity\t"+identity+" %");
						bw.newLine();
						bw.write("E-Value\t"+eval);
						bw.newLine();
						bw.write("Instances:");
						bw.newLine();
						 */
						for (HitProtein o : second.getHitData().getProteins()) {
							if(o.getDatabase_name().contains("uniprot_swissprot")){
								//System.out.println(o.getTitle()+" Description "+o.getDatabase_description()+" Tax-Node"+o.getTax_node());
								/*
								System.out.println("Protein in PdbTm and Not in RefSeq: "+ id);
								System.out.println(o.getSequence_id());
								System.out.println(o.getDatabase_name());
								System.out.println(o.getLinkoutUrl());
								System.out.println(o.getTaxonomy());
								System.out.println("----");
								 */

								bw.write("Protein in PdbTm and Not in RefSeq: "+ id);
								bw.newLine();
								bw.write("Identity\t"+identity+" %");
								bw.newLine();
								bw.write("E-Value\t"+eval);
								bw.newLine();
								bw.newLine();
								bw.write(o.getSequence_id());
								bw.newLine();
								bw.write(o.getDatabase_name());
								bw.newLine();
								bw.write(o.getLinkoutUrl());
								bw.newLine();
								//bw.write(o.getTaxonomy());

							}
						}
					}

				}
				bw.write("----");
				bw.newLine();

			}

		} catch (Exception e){
			e.printStackTrace();

		}
	}



	private static void RunSimapScrap(){

		try {
			SimapAccessWebService simap=new SimapAccessWebService();
			/*
			 * >2A06:A|PDBID|CHAIN|SEQUENCE


			 */
			//String se = "MTNIRKSHPLMKIVNNAFIDLPAPSNISSWWNFGSLLGICLILQILTGLFLAMHYTSDTTTAFSSVTHICRDVNYGWIIRYMHANGASMFFICLYMHVGRGLYYGSYTFLETWNIGVILLLTVMATAFMGYVLPWGQMSFWGATVITNLLSAIPYIGTNLVEWIWGGFSVDKATLTRFFAFHFILPFIIMAIAMVHLLFLHETGSNNPTGISSDVDKIPFHPYYTIKDILGALLLILALMLLVLFAPDLLGDPDNYTPANPLNTPPHIKPEWYFLFAYAILRSIPNKLGGVLALAFSILILALIPLLHTSKQRSMMFRPLSQCLFWALVADLLTLTWIGGQPVEHPYITIGQLASVLYFLLILVLMPTAGTIENKLLKW";
			String se = "TATYAQALQSVPETQVSQLDNGLRVASEQSSQPTCTVGVWIDAGSRYESEKNNGAGYFVEHLAFKGTKNRPGNALEKEVESMGAHLNAYSTREHTAYYIKALSKDLPKAVELLADIVQNCSLEDSQIEKERDVILQELQENDTSMRDVVFNYLHATAFQGTPLAQSVEGPSENVRKLSRADLTEYLSRHYKAPRMVLAAAGGLEHRQLLDLAQKHFSGLSGTYDEDAVPTLSPCRFTGSQICHREDGLPLAHVAIAVEGPGWAHPDNVALQVANAIIGHYDCTYGGGAHLSSPLASIAATNKLCQSFQTFNICYADTGLLGAHFVCDHMSIDDMMFVLQGQWMRLCTSATESEVLRGKNLLRNALVSHLDGTTPVCEDIGRSLLTYGRRIPLAEWESRIAEVDARVVREVCSKYFYDQCPAVAGFGPIEQLPDYNRIRSGMFWLRF";
			String md5=simap.computeMD5(se);
			//String md5="d675f14e17db67a63bfe360524dc2aeb";

			// Setting up the database Id
			int dbid=0;
			// we get all available databases to look up the dataset-id we want
			ArrayList<Hashtable> databases=simap.getAllDatabases();
			for (Hashtable mydatab : databases) {
				//System.out.println(mydatab.get("taxon_id")+"\t"+mydatab.get("name")+"\t"+mydatab.get("id")+"\t"+mydatab.get("source"));
				System.out.println(mydatab.get("name")+"\t"+mydatab.get("id")+"\t"+mydatab.get("source"));
				if (mydatab.get("name").equals("PDB")) {
					System.out.println(mydatab.get("id"));
					dbid=new Integer((String)mydatab.get("id"));
					break;
				}
			}
			simap.addDatabase(dbid);
			System.exit(0);



			simap.setMd5(md5);
			simap.setMax_evalue(1e-30);
			//simap.setMax_number_hits(10);


			simap.alignments(true);
			simap.sequences(true);

			//PEDANT3_INCOMPLETE_GENOMES=13
			//PEDANT3_COMEPLETE_GENOMES
			//simap.addSource(ResourceTypes.PEDANT3_COMEPLETE_GENOMES);
			//simap.addSource(ResourceTypes.PEDANT3_INCOMPLETE_GENOMES);




			ArrayList<HitSet> result =ResultParser.parseResult(simap.getHitsXML());
			HitSet second=result.get(0);

			// a hit consists out of alignment data and hit data
			System.out.println(second.getHitAlignment().getAlignment_hit());
			System.out.println(second.getHitAlignment().getAlignment_markup());
			System.out.println(second.getHitAlignment().getAlignment_query());

			System.out.println("Bitscore\t"+second.getHitAlignment().getBits());
			System.out.println("E-Value\t"+second.getHitAlignment().getEvalue());
			System.out.println("Coverage in Hit\t"+second.getHitAlignment().getHit_coverage());
			System.out.println("Coverage in Query\t"+second.getHitAlignment().getQuery_coverage());
			System.out.println("Percantage matched residues\t"+second.getHitAlignment().getPositives()+" %");
			System.out.println("Score ratios: Hit,Query\t"+second.getHitAlignment().getHit_ScoreRatio()+","+second.getHitAlignment().getQuery_ScoreRatio());

			System.out.println("Identity\t"+second.getHitAlignment().getIdentity()+" %");
			System.out.println("in "+second.getHitAlignment().getOverlap()+" aa overlap");
			System.out.println("from: "+second.getHitAlignment().getQuery_start()+" to "+second.getHitAlignment().getQuery_stop()+ " in query sequence");
			System.out.println("from: "+second.getHitAlignment().getHit_start()+" to "+second.getHitAlignment().getHit_stop()+ " in hit sequence");			

			// print out data concerning the hit-sequence
			System.out.println("\nLength of the sequence:\t"+second.getHitData().getLength());
			System.out.println("Selfscore\t"+second.getHitData().getSelfscore());
			System.out.println("Number of Hits in SIMAP\t"+second.getHitData().getNumber_hits());
			System.out.println("Sequence:\n"+second.getHitData().getSequence());
			System.out.println("with checksum:\t"+second.getHitData().getMd5());

			// print out data of the protein instances
			System.out.println("Instances:");
			for (HitProtein o : second.getHitData().getProteins()) {
				System.out.println(o.getTitle()+" Description "+o.getDatabase_description()+" Tax-Node"+o.getTax_node());
				System.out.println(o.getLinkoutUrl());
				System.out.println(o.getTaxonomy());
				System.out.println("----");
			}


			if(result.size()>=1){
				// we print out the second best hit
				/*
			HitSet second=result.get(1);
			// a hit consists out of alignment data and hit data
			System.out.println(second.getHitAlignment().getAlignment_hit());
			System.out.println(second.getHitAlignment().getAlignment_markup());
			System.out.println(second.getHitAlignment().getAlignment_hit());
			System.out.println("Bitscore\t"+second.getHitAlignment().getBits());
			System.out.println("E-Value\t"+second.getHitAlignment().getEvalue());
			System.out.println("Coverage in Hit\t"+second.getHitAlignment().getHit_coverage());
			System.out.println("Coverage in Query\t"+second.getHitAlignment().getQuery_coverage());
			System.out.println("Percantage matched residues\t"+second.getHitAlignment().getPositives()+" %");
			System.out.println("Score ratios: Hit,Query\t"+second.getHitAlignment().getHit_ScoreRatio()+","+second.getHitAlignment().getQuery_ScoreRatio());

			System.out.println("Identity\t"+second.getHitAlignment().getIdentity()+" %");
			System.out.println("in "+second.getHitAlignment().getOverlap()+" aa overlap");
			System.out.println("from: "+second.getHitAlignment().getQuery_start()+" to "+second.getHitAlignment().getQuery_stop()+ " in query sequence");
			System.out.println("from: "+second.getHitAlignment().getHit_start()+" to "+second.getHitAlignment().getHit_stop()+ " in hit sequence");			

			// print out data concerning the hit-sequence
			System.out.println("\nLength of the sequence:\t"+second.getHitData().getLength());
			System.out.println("Selfscore\t"+second.getHitData().getSelfscore());
			System.out.println("Number of Hits in SIMAP\t"+second.getHitData().getNumber_hits());
			System.out.println("Sequence:\n"+second.getHitData().getSequence());
			System.out.println("with checksum:\t"+second.getHitData().getMd5());
				 */ /*
				System.out.print("\n");
				HitSet second=result.get(0);
				System.out.println("Identity\t"+second.getHitAlignment().getIdentity()+" %");
				System.out.println("E-Value\t"+second.getHitAlignment().getEvalue());
				//second.getHitData().getProteins().get(1).
				// print out data of the protein instances
				System.out.println("Instances:");
				for (HitProtein o : second.getHitData().getProteins()) {
					if(o.getDatabase_name().contains("uniprot_swissprot")){
						System.out.println(o.getTitle()+" Description "+o.getDatabase_description()+" Tax-Node"+o.getTax_node());
						System.out.println(o.getDatabase_name());
						System.out.println(o.getLinkoutUrl());
						System.out.println(o.getTaxonomy());
						System.out.println("----");
					}
				}


				System.out.print("\n");
				second=result.get(1);
				System.out.println("Identity\t"+second.getHitAlignment().getIdentity()+" %");
				System.out.println("E-Value\t"+second.getHitAlignment().getEvalue());
				System.out.print("\n");
				second=result.get(2);
				System.out.println("Identity\t"+second.getHitAlignment().getIdentity()+" %");
				System.out.println("E-Value\t"+second.getHitAlignment().getEvalue());
				System.out.print("\n");
				second=result.get(3);
				System.out.println("Identity\t"+second.getHitAlignment().getIdentity()+" %");
				System.out.println("E-Value\t"+second.getHitAlignment().getEvalue());
				System.out.print("\n");
				second=result.get(4);
				System.out.println("Identity\t"+second.getHitAlignment().getIdentity()+" %");
				System.out.println("E-Value\t"+second.getHitAlignment().getEvalue());
				System.out.print("\n");
				  */
			}


		} catch (Exception e){
			e.printStackTrace();

		}
	}



	private static void Get_NotInCamps() {
		// TODO Auto-generated method stub

		for(int i =0; i<=InCamps.size()-1; i++){
			String temp = InCamps.get(i);
			if (NotInCamps.containsKey(temp)){
				NotInCamps.remove(temp);
				NotInCamps_IdArray.remove(temp);
			}

		}
	}
	private static void Get_seqInCamps(){
		// select distinct pdb_name from camps2pdb;
		try{

			PreparedStatement prepst;
			ResultSet r = null;
			prepst = CAMPS_CONNECTION.prepareStatement("select distinct pdb_name from camps2pdb");
			r = prepst.executeQuery();
			while(r.next()){
				String pdb_name = r.getString(1);
				// use this if i need sequences of camps... first make a hashmap and then un comment below
				//if(!InCamps.containsKey(pdb_name)){
				//	InCamps.put(pdb_name, "");
				//}
				if (!InCamps.contains(pdb_name)){
					InCamps.add(pdb_name);
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void ReadPdbFasta(String PdbfileToRead){
		// WARNING: Not handling Exceptions here!!! Script read good fasta files
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(PdbfileToRead)));
			String line = "";
			String seq = "";
			String id ="xxx";
			while((line = br.readLine())!=null){
				if (line.startsWith(">")){
					// first sequence or new sequence
					if (id.equals("xxx")){
						id = line.substring(1);
					}
					else{
						// have sequence and id ---> save sequence and id and save new id
						FastaFile.put(id, seq);
						NotInCamps.put(id, seq);
						NotInCamps_IdArray.add(id);

						id = line.substring(1);
						seq = "";
					}
				}
				else if (!line.startsWith(" ")){	// avoid spaces
					line = line.replaceAll(" ", "");
					seq = seq + line.trim();
				}
			}
			// save the last sequence
			FastaFile.put(id, seq);
			NotInCamps.put(id, seq);
			NotInCamps_IdArray.add(id);

			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void closeConnection() {
		// TODO Auto-generated method stub
		try{
			CAMPS_CONNECTION.close();
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
