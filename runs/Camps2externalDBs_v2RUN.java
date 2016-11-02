package runs;

import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import workflow.Camps2externalDBs_v2;

public class Camps2externalDBs_v2RUN {
	private static final SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );

	/**
	 * @param args
	 */
	public static void run_uniprot(String t, int n){ 
	//public static void main(String[] args) {
		
		Date startDate = new Date();
		System.out.println("\n\n\t...["+df.format(startDate)+"]  Extract UniProt links");
		
		//CreateDatabaseTables.create_table_camps2uniprot();
		//CreateDatabaseTables.create_table_camps2uniprot_taxonomies();
		//CreateDatabaseTables.create_table_camps2pdb();
		//CreateDatabaseTables.create_table_camps2genbank();
		/*
		String sprotFile = "/scratch/usman/mappingCamps/uniprot_sprot.fasta";
		String tremblFile = "/scratch/usman/mappingCamps/uniprot_trembl.fasta";
		String sprotDatFile = "/scratch/usman/mappingCamps/uniprot_sprot.dat.gz";
		String tremblDatFile = "/scratch/usman/mappingCamps/uniprot_trembl.dat.gz";
		*/
		//String sprotFile = "/tmp/mappingCamps/uniprot_sprot.fasta";
		//String tremblFile = "/tmp/mappingCamps/uniprot_trembl.fasta";
		//String tremblFile = "F:/tempTrembl.fasta";
		
		//String tremblFile = "/tmp/mappingCamps_tempFiles/tailTrembl.fasta";	//cuz of error re running by tail file
		String tremblFile = t;
		//String sprotDatFile = "/tmp/mappingCamps/uniprot_sprot.dat.gz";
		//String tremblDatFile = "/tmp/mappingCamps/uniprot_trembl.dat.gz";
		
		//Camps2externalDBs_v2.camps2uniprot(sprotFile,tremblFile,sprotDatFile,tremblDatFile);
		
		Camps2externalDBs_v2.camps2uniprot2(tremblFile, n);
		
		Date endDate = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
		
		
	}
}
