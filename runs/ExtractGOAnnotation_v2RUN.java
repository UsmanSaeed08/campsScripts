package runs;

import general.CreateDatabaseTables;

import java.text.SimpleDateFormat;
import java.util.Date;

import extract_proteins.ExtractGOAnnotation_v2;

public class ExtractGOAnnotation_v2RUN {
private static final SimpleDateFormat df = new SimpleDateFormat( "HH:mm:ss" );
	
	
	/**
	 * 
	 * @param args
	 */

	public void run_ExtractGOAnnotation_v2(){
	//}
	//public static void main(String[] args) 
	//{
		
		Date startDate = new Date();
		System.out.println("\n\n\t...["+df.format(startDate)+"]  Extract GO annotations...");
		CreateDatabaseTables.create_table_go_annotations();
		
		String annotationFile = "/scratch/uniprot/goa_uniprot/gene_association.goa_uniprot";
		String ontologyFile = "/scratch/uniprot/goa_uniprot/gene_ontology_ext.obo";
		ExtractGOAnnotation_v2.run(annotationFile,ontologyFile);
					
		Date endDate = new Date();
		System.out.println("\n\n...DONE ["+df.format(endDate)+"]");
						
	}

}
