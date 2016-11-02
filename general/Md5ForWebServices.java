package general;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import utils.DBAdaptor;

public class Md5ForWebServices {

	/**
	 * The class is being used to make a file with all the CAMPS SC sequences and their respective md5 and CAMPS clusters
	 * However, initially it is being used to digest md5 and see if this same digestion algorithm was used while generating the 
	 * md5 for sequences in CAMPS tables
	 * 
	 * The idea is to make a file with seqid,md5 and code of structural cluster. 
	 * 
	 * then, for each query sequence.. compute the md5 and find it in file and return the code of sc cluster.
	 *  
	 * @param args
	 */
	
	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*
		//System.out.print("Check");
		String seq = "MpdpacpdsssppGElrlllriaapialaqiaqmamGVTDSVllgglgadalaigglSTMLFFTLLVMLQANLGAGGVLIAQARGSGDEGRIASIHamllvvalllcvpflallTQAGPLLRLMHQPATLVGPVTSFLHILMWGVPPALIGTGVVEVVLPALDAQGvllrvmpvvavvngvlnaglIHGWFGLPAMGLRGSALATTLTMWGAALVLLAMVHSrphlrlllwpprprAADMAVLLRLGVPMMMATGAEIMLFQVTALQAATLGPHALAAHQIVLNLTATTYMAIMALGQAANVRVAYWTGaarpararhaawvaVGTAIAGMVASGCLIYLFRARLVAFYLDPSVPANAESTHIamaalllaaVFQVADGTQAVLVGALRGRGDAIVPMVLAVLGYWGIGFPLGTWLAFRCglgvvglwggvacalvavalmlgvraaRTLGDRGPAAVSVSRCRSA";
		String originalmd5 = "c84006dba523594c5e7aef8755252aee";
		String md5 = computeMD5Hash(seq);
		if(originalmd5.equals(md5)){
			System.out.println("Same original and computed md5");
			System.out.println(md5);
		}
		*/
		//System.out.println("Writing File:");
		//makeFileFormd5s();
		//System.out.println("Done");
		/*
		 * Now to test the file for each sequence and the digestion algorithm 
		 */
		//System.out.println("Testing Sequences: ");
		//TestSequences();
		// so now all the sequences have been tested and the md5s are all right
		// now make the functions as required on the server.. 
		// so a functions, where the query sequence is sent, the function get md5 of it.. and then searches it in the file.. 
		// return the cluster code if found... else returns NotFound -- below function would be used in srv07
		String qu = "NRYNKSALTSIERRYQLTTFRVGILQSCFHIGNLSVILFVSYFGSKWHRPRVIAIGSLIMASSCVISSLPQFVGGRYGARYLKCQQPLSITNSATSTYLYNSSLYIFIAAGNFLKGVGHAPLHPLGISFIDDYATPANSPVYIGVISALSLLGPAFGFMLGSLTASVWVDIGYRPTIHPSWVGAWWVGYLAIAFLLMLATFPLFMYQRSFPERFVCERVLSDSSYNRPLKTVLNASIISSDFGLTLKRLLRDAVFLSITTGFVSLTSFLAASITYIAKYMETQFDLTASFANLLHGCVNLPMAVIGNLLGGWLIRRKNLNVQKTLSVIIMGLIMTVVLVGLLFMFGCDEGDIYGLLDGDRYTNVLNILTSRCSKSCGCSDIFAPVCDISTNVTYRSACHAGCRKTDARSGTRIFYDCLCTKPDIGNTTSKLVLGSCEHSPCWKELIIFLIlmaaasmsaglsaTPS";
		String md5 = computeMD5Hash(qu);
		String clusCode = findmd5inFile(md5);		
		if(clusCode.equals("NotFound")){
			System.out.println("NotFound in camps SC");
		}
		else{
			System.out.println("Match Found: "+ clusCode);
		}
	}
	

	/*
	 * The function gets sequences in SC cluster and computes their md5s and then compares them to the md5s in file
	 * this is just to make sure that no sequence is missing out or no sequence has false md5s
	 */
	private static void TestSequences() {
		// TODO Auto-generated method stub
		try{
			int count = 0 ;
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid,md5,sequence from sequences2 where in_SC=\"Yes\"");
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				Integer seqid = rs.getInt(1);
				String originalmd5 = rs.getString(2).trim();
				String sequence = rs.getString(3).trim().toUpperCase();
				String md5 = computeMD5Hash(sequence);
				String clusCode = findmd5inFile(md5);
				if(clusCode.contains("NotFound")){
					System.out.println(sequence+"\t"+clusCode+"\t"+originalmd5);
					System.err.println("MD5 Not Found Exception. Exiting");
					System.exit(0);
				}
				else{
					count++;
					if(count % 1000 == 0){
						System.out.println("Processed enteries "+ count);
					}
					//System.out.println(sequence+"\t"+clusCode);
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}


	private static String findmd5inFile(String md5) {
		// TODO Auto-generated method stub
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File("F:/Scratch/campsWebsite/md5SumsCampsSC")));
			//BufferedReader br = new BufferedReader(new FileReader(new File("/home/users/saeed/md5SumsCampsSC")));
			String l = "";
			while((l=br.readLine())!=null){
				String[] p = l.split("\t");
				if(p[1].trim().equals(md5)){
					br.close();
					return p[2].trim();
				}
			}
			br.close();
			return "NotFound";
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	private static void makeFileFormd5s() {
		// TODO Auto-generated method stub
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/Scratch/campsWebsite/md5SumsCampsSC")));
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select sequenceid,md5,sc_code from sequences2names where in_SC=\"Yes\"");
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				Integer seqid = rs.getInt(1);
				String md5 = rs.getString(2);
				String sc_code = rs.getString(3);
				bw.write(seqid.toString()+"\t"+md5.trim()+"\t"+sc_code.trim());
				bw.newLine();
				System.out.println(seqid.toString()+"\t"+md5.trim()+"\t"+sc_code.trim());
			}
			rs.close();
			pstm.close();
			bw.close();
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}


	private static String computeMD5Hash(String sequence) {
		// Get md5 digest of the sequence
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			System.out.println("MD5 digest algorithm unavailable.");
			System.exit(1);
		}
		md5.update(sequence.toUpperCase().getBytes());
		BigInteger md5hash = new BigInteger(1, md5.digest());
		String sequence_md5 = md5hash.toString(16);
		// leading zeros are added to make a 32 digit String
		while (sequence_md5.length() < 32) {
			sequence_md5 = "0" + sequence_md5;
		}
		return sequence_md5.toLowerCase();
	}


}
