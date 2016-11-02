package workflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;

import datastructures.TMS;

import utils.DBAdaptor;

public class CreateTopologyFigures {
	
	private int start;
	private int length;
	
	public CreateTopologyFigures(int start, int length) {
		this.start = start;
		this.length = length;
	}
	
	
	public void run(String texDir, String pngDir) {
		
		Connection conn = null;
		
		try {
			
			System.out.println("Run "+this.start+" " +this.length);
									
			conn = DBAdaptor.getConnection("camps3");
			Statement stm = conn.createStatement();
			
			PreparedStatement pstm1 = conn.prepareStatement("SELECT n_term FROM topology WHERE sequences_sequenceid=?");
			PreparedStatement pstm2 = conn.prepareStatement("SELECT begin,end FROM tms WHERE sequences_sequenceid=? ORDER BY begin");
			PreparedStatement pstm3 = conn.prepareStatement("SELECT begin,end FROM elements WHERE sequences_sequenceid=? AND type=\"signal_peptide\"");
			
			ArrayList<File> tmpFiles = new ArrayList<File>();
			
			ResultSet rs = stm.executeQuery("SELECT sequenceid,sequence,length FROM sequences limit "+this.start+","+this.length);
			while(rs.next()) {
							
				int sequenceid = rs.getInt("sequenceid");
				String sequence = rs.getString("sequence").toUpperCase();  //VERY IMPORTANT!!! Textopo can't handle lower case letters!!!
				int seqLength = sequence.length();
				
				if(seqLength > 2000) {
					continue;
				}
				
				String topology = "";
				pstm1.setInt(1, sequenceid);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {					
					
					String nterm = rs1.getString("n_term");
					if(nterm.equals("in")) {
						topology = "intra";
					}
					else if(nterm.equals("out")) {
						topology = "extra";
					}
				}
				rs1.close();
				
				
				String tms = "";
				ArrayList<TMS> tmsArr = new ArrayList<TMS>();
				pstm2.setInt(1, sequenceid);
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()) {
					
					int begin = rs2.getInt("begin");
					int end = rs2.getInt("end");
					
					TMS tmsObject = new TMS(begin,end);
					tmsArr.add(tmsObject);
					
					tms += ","+begin+".."+end;
				}
				rs2.close();
				
				tms = tms.substring(1);
				
				
				String sp = "";
				pstm3.setInt(1, sequenceid);
				ResultSet rs3 = pstm3.executeQuery();
				while(rs3.next()) {
					
					int begin = rs3.getInt("begin");
					int end = rs3.getInt("end");
					
					sp = begin+".."+end;
				}
				rs3.close();
				
				
				//
				//create TeX file
				//
				String texFile = sequenceid + ".tex";
				createTexFile(texDir+texFile, sequence, tms, topology, sp);
				//createTexFile2(texDir+texFile, sequence, tmsArr, topology, sp);
				
				//
				//create temporary sh files
				//
				File tmpFile = File.createTempFile(String.valueOf(sequenceid)+"_", ".sh");
				PrintWriter pw = new PrintWriter(new FileWriter(tmpFile));
				
				String dviFile = texFile.replace(".tex", ".dvi");
				String epsFile = texFile.replace(".tex", ".eps");
				String pngFile = texFile.replace(".tex", ".png");
				
				pw.println("cd "+texDir);
				pw.println("pdflatex -interaction=batchmode "+texFile);
				pw.println("latex -interaction=batchmode "+texFile);
				pw.println("dvips -q -E -D 1000 "+dviFile+" -o "+epsFile);
				pw.println("convert "+epsFile+" "+pngFile);
				pw.println();
				pw.println("mv "+pngFile+" "+pngDir);
				pw.println("rm "+String.valueOf(sequenceid)+".*");
								
				pw.close();
				
				
				tmpFiles.add(tmpFile);
				
				
				
			}
			rs.close();
			
			pstm1.close();
			pstm2.close();
			pstm3.close();
			
			if (conn != null) {
				try {
					conn.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			
			System.out.println("\tFinished reading from db!");
			
			
			int statusCounter = 0;			
			for(File tmpFile: tmpFiles) {
				
				statusCounter ++;
				if (statusCounter % 10 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter % 100 == 0) {
					System.out.write('\n');
					System.out.flush();
				}
				
				String id = tmpFile.getName().split("_")[0];
				
				System.out.println("\tIn progress: " +id+" ["+statusCounter+"]");
				
				//
				//run latex
				//
				String cmd = "sh "+tmpFile.getAbsolutePath();
			    Process p = Runtime.getRuntime().exec(cmd);
			    BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			    String line;
			    while((line=br.readLine()) != null) {
			    	System.err.println(line);			    				    	
			    }		    
				p.waitFor();
				p.destroy();				
				
				br.close();
				
				
				tmpFile.delete();
				
			}
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (conn != null) {
				try {
					conn.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}
	
	
	/*
	 * Creates TeX file to run textopo that is creating a topology diagram.
	 */
	private static void createTexFile(String texFile, String sequence, String tms, String topology, String sp) {
		try {
			
			PrintWriter pw = new PrintWriter(new FileWriter(new File(texFile)));
			
			pw.println("\\documentclass{article}");
			pw.println("\\usepackage[landscape]{geometry}");
			pw.println("\\usepackage{texshade}");
			pw.println("\\usepackage{textopo}");
			pw.println("\\thispagestyle{empty}");
			pw.println();
			pw.println();
			pw.println("\\begin{document}");
			pw.println();
			pw.println("  \\begin{figure}");
			pw.println("  \\begin{textopo}");
			pw.println("    \\sequence{"+sequence+"}");
			pw.println("    \\MRs{"+tms+"}");
			pw.println("    \\Nterm{"+topology+"}");
			pw.println("    \\scaletopo{+1}");
			
			if(!sp.isEmpty()) {
				pw.println("    \\labelstyle{black}{circ}{Black}{Black}{White}{signal peptide}");
				pw.println("    \\labelregion{"+sp+"}{black}{}");
			}
						
			pw.println("    \\labeloutside[left]{extra}");
			pw.println("    \\labelinside[left]{intra}");
			
			pw.println("    \\membranecolors{Black}{Gray10}");
			
			pw.println("    \\applyshading{functional}{hydropathy}");
			pw.println("    \\setsize{legend}{Large}");
			
			pw.println("  \\end{textopo}");
			pw.println("  \\end{figure}");
			pw.println();
			pw.println("\\end{document}");
			
			pw.close();
			
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/*
	 * Same as createTexFile, but the TMS coordinates are not given with thye \MRs
	 * command, but as brackets within the protein sequence.
	 */
	private static void createTexFile2(String texFile, String sequence, ArrayList<TMS> tmsArr, String topology, String sp) {
		try {
			
			
			int numTMS = tmsArr.size();
			String newSequence = "";
			
			
			int previousTmsEnd = 0;
			int countTMS = 0;
			for(TMS tms: tmsArr) {
				
				countTMS++;
				
				int tmsBegin = tms.get_start();
				int tmsEnd = tms.get_end();
				
				if(countTMS == 1) {
					
					String loopSequence = sequence.substring(0, tmsBegin-1);
					newSequence += loopSequence;
					
				}
				else {					
					
					String loopSequence = sequence.substring(previousTmsEnd, tmsBegin-1);
					newSequence += loopSequence;
				}
				
				newSequence += "["+sequence.substring(tmsBegin-1, tmsEnd)+"]";
				
				if(countTMS == numTMS) {
					
					String lastLoopSequence = sequence.substring(tmsEnd, sequence.length());
					newSequence += lastLoopSequence;
				}
				
				previousTmsEnd = tmsEnd;
				
			}
			
			PrintWriter pw = new PrintWriter(new FileWriter(new File(texFile)));
			
			pw.println("\\documentclass{article}");
			pw.println("\\usepackage[landscape]{geometry}");
			pw.println("\\usepackage{texshade}");
			pw.println("\\usepackage{textopo}");
			pw.println("\\thispagestyle{empty}");
			pw.println();
			pw.println();
			pw.println("\\begin{document}");
			pw.println();
			pw.println("  \\begin{figure}");
			pw.println("  \\begin{textopo}");
			pw.println("    \\sequence{"+newSequence+"}");
			pw.println("    \\Nterm{"+topology+"}");
			pw.println("    \\scaletopo{+1}");
			
			if(!sp.isEmpty()) {
				pw.println("    \\labelstyle{black}{circ}{Black}{Black}{White}{signal peptide}");
				pw.println("    \\labelregion{"+sp+"}{black}{}");
			}
						
			pw.println("    \\labeloutside[left]{extra}");
			pw.println("    \\labelinside[left]{intra}");
			
			pw.println("    \\membranecolors{Black}{Gray10}");
			
			pw.println("    \\applyshading{functional}{hydropathy}");
			pw.println("    \\setsize{legend}{Large}");
			
			pw.println("  \\end{textopo}");
			pw.println("  \\end{figure}");
			pw.println();
			pw.println("\\end{document}");
			
			pw.close();
			
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	
	/*
	 * Store pngs (topology diagrams) in database
	 */
	public static void save2DB(String pngDir) {
		
		Connection conn = null;
		
		try {
			
			conn = DBAdaptor.getConnection("camps3");
			
			PreparedStatement pstm = conn.prepareStatement(
					"INSERT INTO topology_images " +
					"(sequences_sequenceid, image) " +
					"VALUES " +
					"(?,?)");
			
			File[] images = new File(pngDir).listFiles();
			for(File image: images) {
				
				String fname = image.getName();
				int sequenceid = Integer.parseInt(fname.replace(".png", ""));
				
				pstm.setInt(1,sequenceid);
				
				FileInputStream fis = new FileInputStream(image);
				pstm.setBinaryStream(2, (InputStream)fis, (int)(image.length()));

				int s = pstm.executeUpdate();
				if(s == 0) {
					System.out.println("WARNING: Could not save "+fname+" to database!");
				}	
				
				fis.close();	//VERY IMPORTANT! Do not forget.
			}
			
			pstm.close();
			
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (conn != null) {
				try {
					conn.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}
	
	
	/*
	 * Checks the log files from the batch queue runs, if the runs
	 * were finished successfully or not.
	 */
	public static void checkLogFiles(String logDir) {
		try {
			
			int numJobs = 990;
			int countCorrectJobs = 0;
			int countProcessedJobs = 0;
			
			int countErrorFiles = 0;
			int countOutputFiles = 0;
			
			File[] logFiles = new File(logDir).listFiles();
			
			for(int i=1; i<= numJobs; i++) {
				
				int countOutput = 0;
				
				boolean notYetProcessed = true;
				
				boolean outputOK = false;
				boolean errorOK = true;
				
				//check output file
				for(File logFile: logFiles) {
					
					if(logFile.getName().startsWith("topo"+i+".o")) {
						
						countOutput++;
						
						countOutputFiles++;
						
						countProcessedJobs++;
						
						notYetProcessed = false;
						
						int count = 0;
						boolean startFound = false;
						boolean doneFound = false;
						
						BufferedReader br = new BufferedReader(new FileReader(logFile));
						String line;
						while((line = br.readLine()) != null) {
							
							if(line.contains("In progress")) {
								count++;
							}
							else if(line.contains("Start")) {
								startFound = true;
							}
							else if(line.contains("DONE")) {
								doneFound = true;
							}
						}
						br.close();
						
//						if(count == 500 && startFound && doneFound) {
//							outputOK = true;
//						}
						if(startFound && doneFound) {
							outputOK = true;
						}
					}					
				}
				
				
				//check error file
				for(File logFile: logFiles) {
					
					if(logFile.getName().startsWith("topo"+i+".e")) {
						
						countErrorFiles++;
						
						BufferedReader br = new BufferedReader(new FileReader(logFile));
						String line;
						while((line = br.readLine()) != null) {
							
							line = line.trim();
							
							if(line.equals("This is pdfTeXk, Version 3.141592-1.40.3 (Web2C 7.5.6)")) {
								continue;
							}
							else if(line.equals("%&-line parsing enabled.")) {
								continue;
							}
							else if(line.equals("entering extended mode")) {
								continue;
							}
							
							System.out.println("\t"+line);
							errorOK = false;
						}
						br.close();					
					}					
				}
				
				if(notYetProcessed) {
					
				}
				else{
					if(outputOK && errorOK) {
						countCorrectJobs++;
					}
					else{
						System.out.println("Job"+i+": Output o.k.?: "+outputOK+"  Error empty?: " +errorOK);
					}					
				}
				
				if(countOutput > 1) {
					System.out.println("Multiple runs for: topo" +i);
				}
			}
			
			System.out.println("\nNumber of successfully finished jobs: " +countCorrectJobs+"/"+countProcessedJobs);
			System.out.println("Number of output files: "+countOutputFiles);
			System.out.println("Number of error files: "+countErrorFiles);
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
		
	
	/*
	 * Checks which diagrams are still missing and returns the min and
	 * max length of the sequence for which a diagram is missing.
	 */
	public static void checkPngFile(String pngDir) {
		
		Connection conn = null;
		
		try {
			
			int countMissingPngs = 0;
			
			int maxLengthFinished = Integer.MIN_VALUE;
			int minLengthUnfinished = Integer.MAX_VALUE;
			int maxLengthUnfinished = Integer.MIN_VALUE;
			
			conn = DBAdaptor.getConnection("camps3");
			Statement stm = conn.createStatement();
			
			ResultSet rs = stm.executeQuery("SELECT sequenceid,length FROM sequences");
			while(rs.next()) {
				
				int id = rs.getInt("sequenceid");
				int length = rs.getInt("length");
				
				File pngFile = new File(pngDir+id+".png");
				
				if(!pngFile.exists()) {
					
					countMissingPngs++;
					System.out.println("No topology diagram for: "+id);
					
					if(length> maxLengthUnfinished) {
						maxLengthUnfinished = length;
					}
					if(length< minLengthUnfinished) {
						minLengthUnfinished = length;
					}
				}
				else{
					if(length> maxLengthFinished) {
						maxLengthFinished = length;
					}
				}
			}
			rs.close();
			stm.close();
			
			conn.close();
			
			System.out.println("\nNumber of missing diagrams: " +countMissingPngs);
			System.out.println("Diagram for longest sequence:"+maxLengthFinished);
			System.out.println("Min length of unfinished sequence:"+minLengthUnfinished);
			System.out.println("Max length of unfinished sequence:"+maxLengthUnfinished);
			
		}catch(Exception e) {
			e.printStackTrace();
		}		
	}
	
	
	/*
	 * Create topology diagrams that are still missing. Available diagrams
	 * are scanned in the given directory.
	 */	
	public static void createMissingDiagrams(String pngDir, String texDir, String shDir) {
		
		Connection conn = null;
		
		try {
			
			conn = DBAdaptor.getConnection("camps3");
			Statement stm = conn.createStatement();
			
			BitSet finishedIds = new BitSet();
			File[] pngFiles = new File(pngDir).listFiles();
			for(File pngFile: pngFiles) {
				
				String fname = pngFile.getName();
				int id = Integer.parseInt(fname.replace(".png", ""));
				if(finishedIds.get(id)) {
					System.out.println("Several pngs for: " +id);
				}
				finishedIds.set(id);
			}
			
			System.out.println("Number of available diagrams: " +finishedIds.cardinality());
			
			
			BitSet missingIds = new BitSet(); 
			
			ResultSet rs = stm.executeQuery("SELECT sequenceid FROM sequences");
			while(rs.next()) {
				
				int id = rs.getInt("sequenceid");
				
				if(!finishedIds.get(id)) {
					
					missingIds.set(id);
				}
			}
			rs.close();
			stm.close();
			
			System.out.println("Number of missing diagrams: " +missingIds.cardinality());
			
			
			PreparedStatement pstm1 = conn.prepareStatement("SELECT sequence FROM sequences WHERE sequenceid=?");
			PreparedStatement pstm2 = conn.prepareStatement("SELECT n_term FROM topology WHERE sequences_sequenceid=?");
			PreparedStatement pstm3 = conn.prepareStatement("SELECT begin,end FROM tms WHERE sequences_sequenceid=? ORDER BY begin");
			PreparedStatement pstm4 = conn.prepareStatement("SELECT begin,end FROM elements WHERE sequences_sequenceid=? AND type=\"signal_peptide\"");
			
			
			for(int id = missingIds.nextSetBit(0); id>=0; id = missingIds.nextSetBit(id+1)) {
				
				String sequence = "";
				pstm1.setInt(1, id);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					sequence = rs1.getString("sequence").toUpperCase();
				}
				rs1.close();
				
				if(sequence.length() > 500) {
					continue;
				}
				
				String topology = "";
				pstm2.setInt(1, id);
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()) {					
					
					String nterm = rs2.getString("n_term");
					if(nterm.equals("in")) {
						topology = "intra";
					}
					else if(nterm.equals("out")) {
						topology = "extra";
					}
				}
				rs2.close();
				
				
				String tms = "";
				pstm3.setInt(1, id);
				ResultSet rs3 = pstm3.executeQuery();
				while(rs3.next()) {
					
					int begin = rs3.getInt("begin");
					int end = rs3.getInt("end");
					
					tms += ","+begin+".."+end;
				}
				rs3.close();
				
				tms = tms.substring(1);
				
				
				String sp = "";
				pstm4.setInt(1, id);
				ResultSet rs4 = pstm4.executeQuery();
				while(rs4.next()) {
					
					int begin = rs4.getInt("begin");
					int end = rs4.getInt("end");
					
					sp = begin+".."+end;
				}
				rs4.close();
				
				
				//
				//create TeX file
				//
				String texFile = id + ".tex";
				createTexFile(texDir+texFile, sequence, tms, topology, sp);
				
				//
				//create sh files
				//
				File shFile = new File(shDir + "topo_seq"+id+ ".sh");
				PrintWriter pw = new PrintWriter(new FileWriter(shFile));
				
				String dviFile = texFile.replace(".tex", ".dvi");
				String epsFile = texFile.replace(".tex", ".eps");
				String pngFile = texFile.replace(".tex", ".png");
				
				pw.println("cd "+texDir);
				pw.println("pdflatex -interaction=batchmode "+texFile);
				pw.println("latex -interaction=batchmode "+texFile);
				pw.println("dvips -q -E -D 1000 "+dviFile+" -o "+epsFile);
				pw.println("convert "+epsFile+" "+pngFile);
				pw.println();
				pw.println("mv "+pngFile+" "+pngDir);
				pw.println("rm "+String.valueOf(id)+".*");
								
				pw.close();
			}
			
			System.out.println("Finished writing sh files!");
			
			
			StringBuffer sb = new StringBuffer();
			
			int fileCount = 0;
			int blockCount = 0;
			File[] shFiles = new File(shDir).listFiles();
			for(File shFile: shFiles) {
				
				fileCount++;
				
				sb.append("sh "+shFile.getAbsolutePath()+"\n");
				
				if(fileCount % 50 == 0) {
					
					blockCount++;
					
					PrintWriter pw = new PrintWriter(new FileWriter(new File(shDir+"RunTopo"+blockCount+".sh")));
					pw.println(sb.toString());
					pw.close();
					
					sb = new StringBuffer();
				}			 
			}
			
			blockCount++;
			
			PrintWriter pw = new PrintWriter(new FileWriter(new File(shDir+"RunTopo"+blockCount+".sh")));
			pw.println(sb.toString());
			pw.close();
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (conn != null) {
				try {
					conn.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}
	
	
	/*
	 * Same as createMissingDiagrams, but with difference that pdflatex is 
	 * only run if the respective dvi file exists.
	 * 
	 * Comment:
	 * For some sequences, the tex file cannot be compiled successfully (reason
	 * not clear). Thus, the dvi file is missing and dvips returns an error. 
	 */
	public static void createMissingDiagrams2(String pngDir, String texDir, String shDir) {
		
		Connection conn = null;
		
		try {
			
			conn = DBAdaptor.getConnection("camps3");
			Statement stm = conn.createStatement();
			
			BitSet finishedIds = new BitSet();
			File[] pngFiles = new File(pngDir).listFiles();
			for(File pngFile: pngFiles) {
				
				String fname = pngFile.getName();
				int id = Integer.parseInt(fname.replace(".png", ""));
				if(finishedIds.get(id)) {
					System.out.println("Several pngs for: " +id);
				}
				finishedIds.set(id);
			}
			
			System.out.println("Number of available diagrams: " +finishedIds.cardinality());
			
			
			BitSet missingIds = new BitSet(); 
			
			ResultSet rs = stm.executeQuery("SELECT sequenceid FROM sequences");
			while(rs.next()) {
				
				int id = rs.getInt("sequenceid");
				
				if(!finishedIds.get(id)) {
					
					missingIds.set(id);
				}
			}
			rs.close();
			stm.close();
			
			System.out.println("Number of missing diagrams: " +missingIds.cardinality());
			
			
			PreparedStatement pstm1 = conn.prepareStatement("SELECT sequence FROM sequences WHERE sequenceid=?");
			PreparedStatement pstm2 = conn.prepareStatement("SELECT n_term FROM topology WHERE sequences_sequenceid=?");
			PreparedStatement pstm3 = conn.prepareStatement("SELECT begin,end FROM tms WHERE sequences_sequenceid=? ORDER BY begin");
			PreparedStatement pstm4 = conn.prepareStatement("SELECT begin,end FROM elements WHERE sequences_sequenceid=? AND type=\"signal_peptide\"");
			
			
			for(int id = missingIds.nextSetBit(0); id>=0; id = missingIds.nextSetBit(id+1)) {
				
				String sequence = "";
				pstm1.setInt(1, id);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					sequence = rs1.getString("sequence").toUpperCase();
				}
				rs1.close();
				
//				if(sequence.length() > 500) {
//					continue;
//				}
				
				String topology = "";
				pstm2.setInt(1, id);
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()) {					
					
					String nterm = rs2.getString("n_term");
					if(nterm.equals("in")) {
						topology = "intra";
					}
					else if(nterm.equals("out")) {
						topology = "extra";
					}
				}
				rs2.close();
								
				
				String tms = "";
				ArrayList<TMS> tmsArr = new ArrayList<TMS>();
				pstm3.setInt(1, id);
				ResultSet rs3 = pstm3.executeQuery();
				while(rs3.next()) {
					
					int begin = rs3.getInt("begin");
					int end = rs3.getInt("end");
					
					TMS tmsObject = new TMS(begin,end);
					tmsArr.add(tmsObject);
					
					tms += ","+begin+".."+end;
				}
				rs3.close();
				
				tms = tms.substring(1);
				
				
				String sp = "";
				pstm4.setInt(1, id);
				ResultSet rs4 = pstm4.executeQuery();
				while(rs4.next()) {
					
					int begin = rs4.getInt("begin");
					int end = rs4.getInt("end");
					
					sp = begin+".."+end;
				}
				rs4.close();
				
				
				//
				//create TeX file
				//
				String texFile = id + ".tex";
				//createTexFile(texDir+texFile, sequence, tms, topology, sp);
				createTexFile2(texDir+texFile, sequence, tmsArr, topology, sp);
								
			}
			
			int count = 0;
			File[] texFiles = new File(texDir).listFiles();
			for(File texFile: texFiles) {
				
				count++;
				
				String texFileName = texFile.getName();	
				int id = Integer.parseInt(texFileName.replace(".tex",""));
				
				System.out.println("In progress: "+id+" ("+count+")");
				
				String dviFileName = texFileName.replace(".tex", ".dvi");
				String epsFileName = texFileName.replace(".tex", ".eps");
				String pngFileName = texFileName.replace(".tex", ".png");
				
				
				File shFile1 = new File(shDir + "topo_seq"+id+ "_part1.sh");
				PrintWriter pw1 = new PrintWriter(new FileWriter(shFile1));
				
				pw1.println("cd "+texDir);
				pw1.println("pdflatex -interaction=batchmode "+texFileName);
				pw1.println("latex -interaction=batchmode "+texFileName);
				pw1.close();
				
				Process p1 = Runtime.getRuntime().exec("sh "+shFile1.getAbsolutePath());
				BufferedReader br1 = new BufferedReader(new InputStreamReader(p1.getInputStream()));
				String line1;
				while((line1=br1.readLine()) != null) {
				    //System.out.println("\t"+line1);	
				}
				p1.waitFor();
				p1.destroy();				
				br1.close();
				
				
				File dviFile = new File(texDir + dviFileName);
				if(dviFile.exists()) {
					
					File shFile2 = new File(shDir + "topo_seq"+id+ "_part2.sh");
					PrintWriter pw2 = new PrintWriter(new FileWriter(shFile2));
					
					pw2.println("cd "+texDir);
					pw2.println("dvips -q -E -D 1000 "+dviFileName+" -o "+epsFileName);
					pw2.println("convert "+epsFileName+" "+pngFileName);
					pw2.println();
					pw2.println("mv "+pngFileName+" "+pngDir);
					pw2.println("rm "+String.valueOf(id)+".*");
					pw2.close();
					
					
					Process p2 = Runtime.getRuntime().exec("sh "+shFile2.getAbsolutePath());
					BufferedReader br2 = new BufferedReader(new InputStreamReader(p2.getInputStream()));
					String line2;
					while((line2=br2.readLine()) != null) {
					    System.out.println("\t"+line2);	
					}
					p2.waitFor();
					p2.destroy();				
					br2.close();
					
					shFile2.delete();
					
				}
				else {
					System.err.println("\tNo DVI file for: " +id+"\t"+dviFile.getAbsolutePath());					
				}	
				
				
				shFile1.delete();
				
			}
			
			
			
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (conn != null) {
				try {
					conn.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}
	
	
	/*
	 * Creates topology diagrams for the one sequence that is given by its id.
	 */
	public static void createDiagramForSequence(String texDir, String pngDir, int sequenceid) {
		
		Connection conn = null;
		
		try {
			
			conn = DBAdaptor.getConnection("camps3");
			Statement stm = conn.createStatement();
			
			PreparedStatement pstm1 = conn.prepareStatement("SELECT n_term FROM topology WHERE sequences_sequenceid=?");
			PreparedStatement pstm2 = conn.prepareStatement("SELECT begin,end FROM tms WHERE sequences_sequenceid=? ORDER BY begin");
			PreparedStatement pstm3 = conn.prepareStatement("SELECT begin,end FROM elements WHERE sequences_sequenceid=? AND type=\"signal_peptide\"");
			
			ArrayList<File> tmpFiles = new ArrayList<File>();
			
			ResultSet rs = stm.executeQuery("SELECT sequence,length FROM sequences WHERE sequenceid="+sequenceid);
			while(rs.next()) {
							
				String sequence = rs.getString("sequence").toUpperCase();
											
				
				String topology = "";
				pstm1.setInt(1, sequenceid);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {					
					
					String nterm = rs1.getString("n_term");
					if(nterm.equals("in")) {
						topology = "intra";
					}
					else if(nterm.equals("out")) {
						topology = "extra";
					}
				}
				rs1.close();
				
				
				String tms = "";
				ArrayList<TMS> tmsArr = new ArrayList<TMS>();
				pstm2.setInt(1, sequenceid);
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()) {
					
					int begin = rs2.getInt("begin");
					int end = rs2.getInt("end");
					
					TMS tmsObject = new TMS(begin,end);
					tmsArr.add(tmsObject);
					
					tms += ","+begin+".."+end;
				}
				rs2.close();
				
				tms = tms.substring(1);
				
				
				String sp = "";
				pstm3.setInt(1, sequenceid);
				ResultSet rs3 = pstm3.executeQuery();
				while(rs3.next()) {
					
					int begin = rs3.getInt("begin");
					int end = rs3.getInt("end");
					
					sp = begin+".."+end;
				}
				rs3.close();
				
				
				//
				//create TeX file
				//
				String texFile = sequenceid + ".tex";
				createTexFile(texDir+texFile, sequence, tms, topology, sp);
				//createTexFile2(texDir+texFile, sequence, tmsArr, topology, sp);
				
				//
				//create temporary sh files
				//
				File tmpFile = File.createTempFile(String.valueOf(sequenceid)+"_", ".sh");
				PrintWriter pw = new PrintWriter(new FileWriter(tmpFile));
				
				String dviFile = texFile.replace(".tex", ".dvi");
				String epsFile = texFile.replace(".tex", ".eps");
				String pngFile = texFile.replace(".tex", ".png");
				
				pw.println("cd "+texDir);
				pw.println("pdflatex -interaction=batchmode "+texFile);
				pw.println("latex -interaction=batchmode "+texFile);
				pw.println("dvips -q -E -D 1000 "+dviFile+" -o "+epsFile);
				//pw.println("convert "+epsFile+" "+pngFile);
				pw.println("convert -density 150 "+epsFile+" "+pngFile);
				pw.println();
				pw.println("mv "+pngFile+" "+pngDir);
				//pw.println("rm "+String.valueOf(sequenceid)+".*");
								
				pw.close();
				
				
				tmpFiles.add(tmpFile);
				
				
				
			}
			rs.close();
			
			pstm1.close();
			pstm2.close();
			pstm3.close();
			
			if (conn != null) {
				try {
					conn.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			
			System.out.println("\tFinished reading from db!");
			
			
			int statusCounter = 0;			
			for(File tmpFile: tmpFiles) {
				
				statusCounter ++;
				if (statusCounter % 10 == 0) {
					System.out.write('.');
					System.out.flush();
				}
				if (statusCounter % 100 == 0) {
					System.out.write('\n');
					System.out.flush();
				}
				
				String id = tmpFile.getName().split("_")[0];
				
				System.out.println("\tIn progress: " +id+" ["+statusCounter+"]");
				
				//
				//run latex
				//
				String cmd = "sh "+tmpFile.getAbsolutePath();
			    Process p = Runtime.getRuntime().exec(cmd);
			    BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			    String line;
			    while((line=br.readLine()) != null) {
			    	System.err.println(line);			    				    	
			    }		    
				p.waitFor();
				p.destroy();				
				
				br.close();
				
				
				tmpFile.delete();
				
			}
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (conn != null) {
				try {
					conn.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}
	
	
	/*
	 * Deletes topology diagrams from db that are listed in the specified dir.
	 */
	public static void deleteFromDB(String pngDir) {
		
		Connection conn = null;
		
		try {
			
			conn = DBAdaptor.getConnection("camps3");
			
			PreparedStatement pstm = conn.prepareStatement("DELETE FROM topology_images WHERE sequences_sequenceid=?");
			
			File[] pngFiles = new File(pngDir).listFiles();
			for(File pngFile: pngFiles) {
				
				String fname = pngFile.getName();
				int id = Integer.parseInt(fname.replace(".png", ""));
				
				pstm.setInt(1, id);
				pstm.executeUpdate();
			}
			
			pstm.close();
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (conn != null) {
				try {
					conn.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}		
	}
	
	
	/*
	 * Same as save2DB, but with the difference that the diagrams are 
	 * written to the db by browsing the sequenceids in the sequence
	 * table and check whether a file is available.
	 */
	public static void save2DB2(String pngDir) {
		
		Connection conn1 = null;
		Connection conn2 = null;
		
		try {
			
			conn1 = DBAdaptor.getConnection("camps3");
			conn2 = DBAdaptor.getConnection("camps3");
			
			Statement stm = conn1.createStatement();
			
			PreparedStatement pstm = conn2.prepareStatement(
					"INSERT INTO topology_images " +
					"(sequences_sequenceid, image) " +
					"VALUES " +
					"(?,?)");
			
			int count = 0;
			
			ResultSet rs = stm.executeQuery("SELECT sequenceid FROM sequences ORDER BY sequenceid");
			while(rs.next()) {
							
				int sequenceid = rs.getInt("sequenceid");
				
				File pngFile = new File(pngDir+sequenceid+".png");
				
				if(pngFile.exists()) {
					
					pstm.setInt(1,sequenceid);
					
					FileInputStream fis = new FileInputStream(pngFile);
					pstm.setBinaryStream(2, (InputStream)fis, (int)(pngFile.length()));

					int s = pstm.executeUpdate();
					if(s == 0) {
						System.out.println("WARNING: Could not save "+pngFile.getName()+" to database!");
					}	
					else {
						count++;
					}
					
					fis.close();	//VERY IMPORTANT! Do not forget.
				}
			}
			rs.close();
			
			stm.close();
			pstm.close();
			
			System.out.println("\n\nWrote "+count+" images to db!");
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		finally {			
			if (conn1 != null) {
				try {
					conn1.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			if (conn2 != null) {
				try {
					conn2.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}
	
	
	/*
	 * Create topology diagrams that are still missing. Available diagrams
	 * are scanned in the database.
	 */
	public static void createMissingDiagrams3(String pngDir, String texDir, String shDir) {
		
		Connection conn = null;
		
		try {
			
			conn = DBAdaptor.getConnection("camps3");
			Statement stm = conn.createStatement();
			
			BitSet finishedIds = new BitSet();
			
			ResultSet rsx = stm.executeQuery("SELECT sequences_sequenceid FROM topology_images");
			while(rsx.next()) {
				
				int id = rsx.getInt("sequences_sequenceid");
				
				finishedIds.set(id);
			}
			rsx.close();
			
			System.out.println("Number of available diagrams: " +finishedIds.cardinality());
			
			
			BitSet missingIds = new BitSet(); 
			
			ResultSet rsy = stm.executeQuery("SELECT sequenceid FROM sequences");
			while(rsy.next()) {
				
				int id = rsy.getInt("sequenceid");
				
				if(!finishedIds.get(id)) {
					
					missingIds.set(id);
				}
			}
			rsy.close();
			stm.close();
			
			System.out.println("Number of missing diagrams: " +missingIds.cardinality());
			
			
			PreparedStatement pstm1 = conn.prepareStatement("SELECT sequence FROM sequences WHERE sequenceid=?");
			PreparedStatement pstm2 = conn.prepareStatement("SELECT n_term FROM topology WHERE sequences_sequenceid=?");
			PreparedStatement pstm3 = conn.prepareStatement("SELECT begin,end FROM tms WHERE sequences_sequenceid=? ORDER BY begin");
			PreparedStatement pstm4 = conn.prepareStatement("SELECT begin,end FROM elements WHERE sequences_sequenceid=? AND type=\"signal_peptide\"");
			
			int minLength = Integer.MAX_VALUE;
			int maxLength = Integer.MIN_VALUE;
			
			for(int id = missingIds.nextSetBit(0); id>=0; id = missingIds.nextSetBit(id+1)) {
				
				String sequence = "";
				pstm1.setInt(1, id);
				ResultSet rs1 = pstm1.executeQuery();
				while(rs1.next()) {
					sequence = rs1.getString("sequence").toUpperCase();
				}
				rs1.close();
				
				int seqLength = sequence.length();
				
//				if(sequence.length() > 500) {
//					continue;
//				}
				
				if(seqLength<minLength) {
					minLength =seqLength;
				}
				if(seqLength>maxLength) {
					maxLength =seqLength;
				}
				
				String topology = "";
				pstm2.setInt(1, id);
				ResultSet rs2 = pstm2.executeQuery();
				while(rs2.next()) {					
					
					String nterm = rs2.getString("n_term");
					if(nterm.equals("in")) {
						topology = "intra";
					}
					else if(nterm.equals("out")) {
						topology = "extra";
					}
				}
				rs2.close();
								
				
				String tms = "";
				ArrayList<TMS> tmsArr = new ArrayList<TMS>();
				pstm3.setInt(1, id);
				ResultSet rs3 = pstm3.executeQuery();
				while(rs3.next()) {
					
					int begin = rs3.getInt("begin");
					int end = rs3.getInt("end");
					
					TMS tmsObject = new TMS(begin,end);
					tmsArr.add(tmsObject);
					
					tms += ","+begin+".."+end;
				}
				rs3.close();
				
				tms = tms.substring(1);
				
				
				String sp = "";
				pstm4.setInt(1, id);
				ResultSet rs4 = pstm4.executeQuery();
				while(rs4.next()) {
					
					int begin = rs4.getInt("begin");
					int end = rs4.getInt("end");
					
					sp = begin+".."+end;
				}
				rs4.close();
				
				
				//
				//create TeX file
				//
				String texFile = id + ".tex";
				createTexFile(texDir+texFile, sequence, tms, topology, sp);
												
			}
			
			System.out.println("Minimal length of missing sequences: " +minLength);
			System.out.println("Maximal length of missing sequences: " +maxLength);
			
			int count = 0;
			File[] texFiles = new File(texDir).listFiles();
			for(File texFile: texFiles) {
				
				count++;
				
				String texFileName = texFile.getName();	
				int id = Integer.parseInt(texFileName.replace(".tex",""));
				
				System.out.println("In progress: "+id+" ("+count+")");
				
				String dviFileName = texFileName.replace(".tex", ".dvi");
				String epsFileName = texFileName.replace(".tex", ".eps");
				String pngFileName = texFileName.replace(".tex", ".png");
				
				
				File shFile1 = new File(shDir + "topo_seq"+id+ "_part1.sh");
				PrintWriter pw1 = new PrintWriter(new FileWriter(shFile1));
				
				pw1.println("cd "+texDir);
				pw1.println("pdflatex -interaction=batchmode "+texFileName);
				pw1.println("latex -interaction=batchmode "+texFileName);
				pw1.close();
				
				Process p1 = Runtime.getRuntime().exec("sh "+shFile1.getAbsolutePath());
				BufferedReader br1 = new BufferedReader(new InputStreamReader(p1.getInputStream()));
				String line1;
				while((line1=br1.readLine()) != null) {
				    //System.out.println("\t"+line1);	
				}
				p1.waitFor();
				p1.destroy();				
				br1.close();
				
				
				File dviFile = new File(texDir + dviFileName);
				if(dviFile.exists()) {
					
					File shFile2 = new File(shDir + "topo_seq"+id+ "_part2.sh");
					PrintWriter pw2 = new PrintWriter(new FileWriter(shFile2));
					
					pw2.println("cd "+texDir);
					pw2.println("dvips -q -E -D 1000 "+dviFileName+" -o "+epsFileName);
					pw2.println("convert "+epsFileName+" "+pngFileName);
					pw2.println();
					pw2.println("mv "+pngFileName+" "+pngDir);
					pw2.println("rm "+String.valueOf(id)+".*");
					pw2.close();
					
					
					Process p2 = Runtime.getRuntime().exec("sh "+shFile2.getAbsolutePath());
					BufferedReader br2 = new BufferedReader(new InputStreamReader(p2.getInputStream()));
					String line2;
					while((line2=br2.readLine()) != null) {
					    System.out.println("\t"+line2);	
					}
					p2.waitFor();
					p2.destroy();				
					br2.close();
					
					shFile2.delete();
					
				}
				else {
					
					File shFile2 = new File(shDir + "topo_seq"+id+ "_part2.sh");
					PrintWriter pw2 = new PrintWriter(new FileWriter(shFile2));
					
					pw2.println("cd "+texDir);
					pw2.println("rm "+String.valueOf(id)+".*");
					pw2.close();
					
					
					Process p2 = Runtime.getRuntime().exec("sh "+shFile2.getAbsolutePath());
					BufferedReader br2 = new BufferedReader(new InputStreamReader(p2.getInputStream()));
					String line2;
					while((line2=br2.readLine()) != null) {
					    System.out.println("\t"+line2);	
					}
					p2.waitFor();
					p2.destroy();				
					br2.close();
										
					shFile2.delete();
					
					System.err.println("\tNo DVI file for: " +id+"\t"+dviFile.getAbsolutePath());					
				}	
				
				
				shFile1.delete();
				
			}
			
			
			
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (conn != null) {
				try {
					conn.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}
	}
	
	
	public static void main(String[] args) {
		
		try {
			
//			String texDir = "/scratch/sneumann/textopo/tex/";
//			String pngDir = "/scratch/sneumann/textopo/png/";
//			
//			SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
//			
//			Date startDate = new Date();
//			System.out.println("\n\n\t...["+df.format(startDate)+"]  Start");
//			
//			int start = Integer.parseInt(args[0]);
//			int length = Integer.parseInt(args[1]); 
//			
//			CreateTopologyFigures tf = new CreateTopologyFigures(start,length);
//			tf.run(texDir, pngDir);
//			
//			Date endDate = new Date();
//			System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
			
			
			
			//##################################################################
			
			
//			checkLogFiles("/scratch/sneumann/textopo/log/");

			
//			checkPngFile(pngDir);
			
			
//			String shDir = "/scratch/sneumann/textopo/sh/";
//			createMissingDiagrams2(pngDir, texDir, shDir);

				
		
			//##################################################################
			
			//save2DB("/scratch/sneumann/textopo/png4DB/");
			
			//deleteFromDB("/scratch/sneumann/textopo/png4DB/");
			
			
			//##################################################################
			
			//save2DB2("/scratch/sneumann/textopo/png/");
			
			
//			String texDir = "/scratch/sneumann/textopo/test/tex/";
//			String pngDir = "/scratch/sneumann/textopo/test/png/";
//			String shDir = "/scratch/sneumann/textopo/test/sh/";
//			createMissingDiagrams3(pngDir, texDir, shDir);
			
//			save2DB("/scratch/sneumann/textopo/test/png4DB/");
			
			
//			String texDir = "/scratch/sneumann/textopo/test/tex/";
//			String pngDir = "/scratch/sneumann/textopo/test/png/";
//			//createDiagramForSequence(texDir, pngDir, 13603981);
//			createDiagramForSequence(texDir, pngDir, 471569);
			
			
			//##################################################################
			
			//Create topology diagrams for sequences that can be found
			//in the same SCOP/CATH fold, but in different SC-clusters
			//-> see evaluation: /home/users/sneumann/PHD/MetaModels/4CAMPS3/Evaluation/PDBTM/comparison_usingPDBTMseqs/update_040311/
			
//			String texDir = "/home/users/sneumann/PHD/MetaModels/4CAMPS3/Evaluation/PDBTM/comparison_usingPDBTMseqs/update_040311/topologyDiagrams/";
//			String pngDir = "/home/users/sneumann/PHD/MetaModels/4CAMPS3/Evaluation/PDBTM/comparison_usingPDBTMseqs/update_040311/topologyDiagrams/";
//			createDiagramForSequence(texDir, pngDir, 350918);
//			createDiagramForSequence(texDir, pngDir, 354238);
//			
//			createDiagramForSequence(texDir, pngDir, 441891);
//			createDiagramForSequence(texDir, pngDir, 1974691);
		} catch(Exception e) {
			e.printStackTrace();
		}	
				
	}

}
