package runs;


import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import workflow.OtherDatabase;


public class OtherDatabaseRUN {
	
	private static final SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
		

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Date startDate = new Date();
		System.out.println("\n\n\t...["+df.format(startDate)+"]  Write BLAST results to database");
		//OtherDatabase.main(args);
		//CreateDatabaseTables.create_table_other_database();
				
		System.out.println("In progress: drugbank");
		
		System.out.println("\tInserting hits from"+args[0]+"\n");
		OtherDatabase.extractHitsFromBlastResults(args[0], "drugbank");
		/*
		System.out.println("\tInserting hits from part2");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part2_drugbank.blast", "drugbank");
		System.out.println("\tInserting hits from part3");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part3_drugbank.blast", "drugbank");
		System.out.println("\tInserting hits from part4");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part4_drugbank.blast", "drugbank");
		System.out.println("\tInserting hits from part5");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part5_drugbank.blast", "drugbank");
		System.out.println("\tInserting hits from part6");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part6_drugbank.blast", "drugbank");
		
		System.out.println("\n...DONE");
		/*
		System.out.println("\nIn progress: gpcrdb");
		
		System.out.println("\tInserting hits from part1");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part1_gpcrdb.blast", "gpcrdb");
		System.out.println("\tInserting hits from part2");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part2_gpcrdb.blast", "gpcrdb");
		System.out.println("\tInserting hits from part3");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part3_gpcrdb.blast", "gpcrdb");
		System.out.println("\tInserting hits from part4");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part4_gpcrdb.blast", "gpcrdb");
		System.out.println("\tInserting hits from part5");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part5_gpcrdb.blast", "gpcrdb");
		System.out.println("\tInserting hits from part6");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part6_gpcrdb.blast", "gpcrdb");
		*/
		//System.out.println("\n...DONE");
		/*
		System.out.println("\nIn progress: mptopo");
		System.out.println("\tInserting hits from part1");
		OtherDatabase.extractHitsFromBlastResults("/scratch/sneumann/Camps3/blastOutput/camps3_part1_mptopo.blast", "mptopo");
		System.out.println("\tInserting hits from part2");
		OtherDatabase.extractHitsFromBlastResults("/scratch/sneumann/Camps3/blastOutput/camps3_part2_mptopo.blast", "mptopo");
		System.out.println("\tInserting hits from part3");
		OtherDatabase.extractHitsFromBlastResults("/scratch/sneumann/Camps3/blastOutput/camps3_part3_mptopo.blast", "mptopo");
		System.out.println("\tInserting hits from part4");
		OtherDatabase.extractHitsFromBlastResults("/scratch/sneumann/Camps3/blastOutput/camps3_part4_mptopo.blast", "mptopo");
		System.out.println("\tInserting hits from part5");
		OtherDatabase.extractHitsFromBlastResults("/scratch/sneumann/Camps3/blastOutput/camps3_part5_mptopo.blast", "mptopo");
		System.out.println("\n...DONE");
		
		
		System.out.println("\nIn progress: omim");
		
		System.out.println("\tInserting hits from part1");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part1_omim.blast", "omim");
		System.out.println("\tInserting hits from part2");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part2_omim.blast", "omim");
		System.out.println("\tInserting hits from part3");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part3_omim.blast", "omim");
		System.out.println("\tInserting hits from part4");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part4_omim.blast", "omim");
		System.out.println("\tInserting hits from part5");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part5_omim.blast", "omim");
		System.out.println("\tInserting hits from part6");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part6_omim.blast", "omim");
		
		System.out.println("\n...DONE");
		System.out.println("\nIn progress: opm");
		
		System.out.println("\tInserting hits from part1");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part1_opm.blast", "opm");
		System.out.println("\tInserting hits from part2");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part2_opm.blast", "opm");
		System.out.println("\tInserting hits from part3");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part3_opm.blast", "opm");
		System.out.println("\tInserting hits from part4");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part4_opm.blast", "opm");
		System.out.println("\tInserting hits from part5");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part5_opm.blast", "opm");
		System.out.println("\tInserting hits from part6");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part6_opm.blast", "opm");
		
		System.out.println("\n...DONE");
		System.out.println("\nIn progress: pdbtm");
		
		System.out.println("\tInserting hits from part1");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part1_pdbtm.blast", "pdbtm");
		System.out.println("\tInserting hits from part2");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part2_pdbtm.blast", "pdbtm");
		System.out.println("\tInserting hits from part3");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part3_pdbtm.blast", "pdbtm");
		System.out.println("\tInserting hits from part4");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part4_pdbtm.blast", "pdbtm");
		System.out.println("\tInserting hits from part5");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part5_pdbtm.blast", "pdbtm");
		System.out.println("\tInserting hits from part6");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part6_pdbtm.blast", "pdbtm");
		
		System.out.println("\n...DONE");
		System.out.println("\nIn progress: targetdb");
		
		System.out.println("\tInserting hits from part1");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part1_targetdb.blast", "targetdb");
		System.out.println("\tInserting hits from part2");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part2_targetdb.blast", "targetdb");
		System.out.println("\tInserting hits from part3");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part3_targetdb.blast", "targetdb");
		System.out.println("\tInserting hits from part4");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part4_targetdb.blast", "targetdb");
		System.out.println("\tInserting hits from part5");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part5_targetdb.blast", "targetdb");
		System.out.println("\tInserting hits from part6");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part6_targetdb.blast", "targetdb");
		
		System.out.println("\n...DONE");
		System.out.println("\nIn progress: tcdb");
		
		System.out.println("\tInserting hits from part1");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part1_tcdb.blast", "tcdb");
		System.out.println("\tInserting hits from part2");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part2_tcdb.blast", "tcdb");
		System.out.println("\tInserting hits from part3");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part3_tcdb.blast", "tcdb");
		System.out.println("\tInserting hits from part4");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part4_tcdb.blast", "tcdb");
		System.out.println("\tInserting hits from part5");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part5_tcdb.blast", "tcdb");
		System.out.println("\tInserting hits from part6");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part6_tcdb.blast", "tcdb");
		
		System.out.println("\n...DONE");
		System.out.println("\nIn progress: topdb");
		
		System.out.println("\tInserting hits from part1");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part1_topdb.blast", "topdb");
		System.out.println("\tInserting hits from part2");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part2_topdb.blast", "topdb");
		System.out.println("\tInserting hits from part3");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part3_topdb.blast", "topdb");
		System.out.println("\tInserting hits from part4");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part4_topdb.blast", "topdb");
		System.out.println("\tInserting hits from part5");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part5_topdb.blast", "topdb");
		System.out.println("\tInserting hits from part6");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part6_topdb.blast", "topdb");
		
		System.out.println("\n...DONE");
		System.out.println("\nIn progress: vfdb");
		
		System.out.println("\tInserting hits from part1");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part1_vfdb.blast", "vfdb");
		System.out.println("\tInserting hits from part2");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part2_vfdb.blast", "vfdb");
		System.out.println("\tInserting hits from part3");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part3_vfdb.blast", "vfdb");
		System.out.println("\tInserting hits from part4");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part4_vfdb.blast", "vfdb");
		System.out.println("\tInserting hits from part5");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part5_vfdb.blast", "vfdb");
		System.out.println("\tInserting hits from part6");
		OtherDatabase.extractHitsFromBlastResults("/scratch/usman/Camps4/blastOutput/camps4_part6_vfdb.blast", "vfdb");
		
		System.out.println("\n...DONE");
		
		*/
		OtherDatabase.disconnect();
				
		Date endDate = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate)+"]");

	}

}
