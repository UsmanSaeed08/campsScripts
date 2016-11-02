package general;

import java.sql.Connection;
import java.sql.Statement;

import utils.DBAdaptor;
/*
 * CreateDatabaseTables
 * 
 * Version 1.0
 * 
 * 2009-08-20
 * 
 */

public class CreateDatabaseTables {
	
	private static final String DB = "camps4";
	
	
	public static void create_table_proteins() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE proteins2 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +			
			"sequenceid INT(10) UNSIGNED NOT NULL, " +
			"name VARCHAR(100) NOT NULL, " +
			"description VARCHAR(255), " +
			"databaseid MEDIUMINT(8) UNSIGNED NOT NULL, " +
			"taxonomyid INT(10) UNSIGNED, " +
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_sequences() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE sequences2 " +
			"(" +
			"sequenceid INT(10) UNSIGNED NOT NULL, " +
			"PRIMARY KEY (sequenceid), " +				
			"md5 VARCHAR(32) NOT NULL, " +
			"length MEDIUMINT(8) UNSIGNED NOT NULL, " +
			"selfscore MEDIUMINT(8) UNSIGNED NOT NULL, " +
			"sequence TEXT NOT NULL, " +	
			"redundant VARCHAR(5), " +			
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_databases() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE databases_2 " +
			"(" +
			"databaseid MEDIUMINT(8) UNSIGNED NOT NULL, " +
			"PRIMARY KEY (databaseid), " +		
			"taxonomyid INT(10) UNSIGNED, " +
			"name VARCHAR(100), " +
			"description VARCHAR(255), " +
			"source VARCHAR(50), " +					
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_taxonomies() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE taxonomies2 " +
			"(" +
			"taxonomyid INT(10) UNSIGNED NOT NULL, " +
			"PRIMARY KEY (taxonomyid), " +		
			"taxonomy VARCHAR(255), " +		//added 07-Febr-2011
			"species VARCHAR(255), " +
			"genus VARCHAR(255), " +
			"family VARCHAR(255), " +
			"order_ VARCHAR(255), " +
			"class VARCHAR(255), " +
			"phylum VARCHAR(255), " +
			"kingdom VARCHAR(255), " +
			"superkingdom VARCHAR(255), " +				
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_tms() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE tms2 " +
			"(" +
			"sequenceid INT(10) UNSIGNED NOT NULL, " +			
			"tms_id INT(5) UNSIGNED NOT NULL, " +		
			"begin INT(5) UNSIGNED NOT NULL, " +	
			"end INT(5) UNSIGNED NOT NULL, " +	
			"length INT(5) UNSIGNED NOT NULL, " +
			"md5 VARCHAR(32) NOT NULL, " +
			"timestamp TIMESTAMP NOT NULL," +
			"PRIMARY KEY (sequenceid,tms_id)" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void create_table_mapped_tms() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE mapped_tms2 " +
			"(" +
			"idx INT(10) UNSIGNED NOT NULL, " +		
			"simapId INT(10) UNSIGNED NOT NULL, " +		
			"campsId INT(10) UNSIGNED NOT NULL, " +	
			"tmsNo INT(5) UNSIGNED NOT NULL, " +	
			"begin INT(5) UNSIGNED NOT NULL, " +
			"end INT(5) UNSIGNED NOT NULL, " +			
			"length INT(5) UNSIGNED NOT NULL, " +		
			"tmsCount INT(5) UNSIGNED NOT NULL, " +	
			"timestamp TIMESTAMP NOT NULL," +
			"PRIMARY KEY (idx)" +			
			")";
			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_elements() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE elements2 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +	
			"sequences_sequenceid INT(10) UNSIGNED NOT NULL, " +				
			"type VARCHAR(50) NOT NULL, " +
			"method VARCHAR(30) NOT NULL, " +
			"begin INT(5) UNSIGNED NOT NULL, " +	
			"end INT(5) UNSIGNED NOT NULL, " +	
			"length INT(5) UNSIGNED NOT NULL, " +	
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_domains_pfam() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE domains_pfam " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"sequenceid INT(11) UNSIGNED NOT NULL, " +	
			"accession VARCHAR(255), " +
			"description VARCHAR(255), " +
			"begin INT(5) UNSIGNED NOT NULL, " +	
			"end INT(5) UNSIGNED NOT NULL, " +	
			"length INT(5) UNSIGNED NOT NULL, " +				
			"evalue DOUBLE, " +
			"subtype VARCHAR(20), " +	
			"residues_in_tmh INT(5) UNSIGNED, " +
			"perc_residues_in_tmh DOUBLE UNSIGNED, " +
			"covered_tmh INT(5) UNSIGNED, " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_domains_superfamily() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE domains_superfamily " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"sequenceid INT(11) UNSIGNED NOT NULL, " +	
			"accession VARCHAR(255), " +
			"description VARCHAR(255), " +
			"begin INT(5) UNSIGNED NOT NULL, " +	
			"end INT(5) UNSIGNED NOT NULL, " +	
			"length INT(5) UNSIGNED NOT NULL, " +				
			"evalue DOUBLE, " +
			"subtype VARCHAR(20), " +
			"residues_in_tmh INT(5) UNSIGNED, " +
			"perc_residues_in_tmh DOUBLE UNSIGNED, " +
			"covered_tmh INT(5) UNSIGNED, " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void create_table_alignments_nonTMS() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE alignments_nonTMS " +
			"(" +
			"id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"sequences_sequenceid_query INT(11) UNSIGNED NOT NULL, " +
			"sequences_sequenceid_hit INT(11) UNSIGNED NOT NULL, " +
			"query_begin INT(5) UNSIGNED NOT NULL, " +	
			"query_end INT(5) UNSIGNED NOT NULL, " +	
			"hit_begin INT(5) UNSIGNED NOT NULL, " +	
			"hit_end INT(5) UNSIGNED NOT NULL, " +	
			"bit_score DOUBLE UNSIGNED, " +
			"sw_score INT(6) UNSIGNED, " +			
			"evalue DOUBLE UNSIGNED, " +
			"identity FLOAT UNSIGNED, " +
			"positives FLOAT UNSIGNED, " +
			"covered_tms_query INT(2) UNSIGNED, " +
			"perc_covered_tms_query DOUBLE UNSIGNED, " +
			"covered_tms_hit INT(2) UNSIGNED, " +
			"perc_covered_tms_hit DOUBLE UNSIGNED, " +												
			"timestamp TIMESTAMP NOT NULL" +			
			")";
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static void create_table_alignments() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE alignments " +
			"(" +
			"id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"sequences_sequenceid_query INT(11) UNSIGNED NOT NULL, " +
			"sequences_sequenceid_hit INT(11) UNSIGNED NOT NULL, " +
			"query_begin INT(5) UNSIGNED NOT NULL, " +	
			"query_end INT(5) UNSIGNED NOT NULL, " +	
			"hit_begin INT(5) UNSIGNED NOT NULL, " +	
			"hit_end INT(5) UNSIGNED NOT NULL, " +	
			"bit_score DOUBLE UNSIGNED, " +
			"sw_score INT(6) UNSIGNED, " +			
			"evalue DOUBLE UNSIGNED, " +
			"identity FLOAT UNSIGNED, " +
			"positives FLOAT UNSIGNED, " +
			"covered_tms_query INT(2) UNSIGNED, " +
			"perc_covered_tms_query DOUBLE UNSIGNED, " +
			"covered_tms_hit INT(2) UNSIGNED, " +
			"perc_covered_tms_hit DOUBLE UNSIGNED, " +												
			"timestamp TIMESTAMP NOT NULL" +			
			")";
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static void create_table_alignments_temp() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE alignments_temp " +
			"(" +
			"id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"seqid_query INT(11) UNSIGNED NOT NULL, " +
			"seqid_hit INT(11) UNSIGNED NOT NULL, " +
			"query_begin INT(5) UNSIGNED NOT NULL, " +	
			"query_end INT(5) UNSIGNED NOT NULL, " +	
			"hit_begin INT(5) UNSIGNED NOT NULL, " +	
			"hit_end INT(5) UNSIGNED NOT NULL, " +	
			"bit_score DOUBLE UNSIGNED, " +
			"sw_score INT(6) UNSIGNED, " +			
			"evalue DOUBLE UNSIGNED, " +
			"identity FLOAT UNSIGNED, " +
			"positives FLOAT UNSIGNED, " +
			"covered_tms_query INT(2) UNSIGNED, " +
			"perc_covered_tms_query DOUBLE UNSIGNED, " +
			"covered_tms_hit INT(2) UNSIGNED, " +
			"perc_covered_tms_hit DOUBLE UNSIGNED, " +
		
			"similarity FLOAT UNSIGNED, " +
			"overlap INT(5) UNSIGNED NOT NULL, " +
			"alignment_coverage_query FLOAT UNSIGNED, " +
			"alignment_coverage_hit FLOAT UNSIGNED, " +
			"selfscoreRatio_query FLOAT UNSIGNED, " +
			"selfscoreRatio_hit FLOAT UNSIGNED, " +
			
			"timestamp TIMESTAMP NOT NULL" +			
			")";
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static void create_table_alignments2() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE alignments2 " +
			"(" +
			"id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"seqid_query INT(11) UNSIGNED NOT NULL, " +
			"seqid_hit INT(11) UNSIGNED NOT NULL, " +
			"query_begin INT(5) UNSIGNED NOT NULL, " +	
			"query_end INT(5) UNSIGNED NOT NULL, " +	
			"hit_begin INT(5) UNSIGNED NOT NULL, " +	
			"hit_end INT(5) UNSIGNED NOT NULL, " +	
			"bit_score DOUBLE UNSIGNED, " +
			"sw_score INT(6) UNSIGNED, " +			
			"evalue DOUBLE UNSIGNED, " +
			"identity FLOAT UNSIGNED, " +
			"positives FLOAT UNSIGNED, " +
			"covered_tms_query INT(2) UNSIGNED, " +
			"perc_covered_tms_query DOUBLE UNSIGNED, " +
			"covered_tms_hit INT(2) UNSIGNED, " +
			"perc_covered_tms_hit DOUBLE UNSIGNED, " +
		
			"similarity FLOAT UNSIGNED, " +
			"overlap INT(5) UNSIGNED NOT NULL, " +
			"alignment_coverage_query FLOAT UNSIGNED, " +
			"alignment_coverage_hit FLOAT UNSIGNED, " +
			"selfscoreRatio_query FLOAT UNSIGNED, " +
			"selfscoreRatio_hit FLOAT UNSIGNED, " +
			
			"timestamp TIMESTAMP NOT NULL" +			
			")";
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static void create_table_alignments_initial() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE alignments_initial " +
			"(" +
			"id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"seqid_query INT(11) UNSIGNED NOT NULL, " +
			"seqid_hit INT(11) UNSIGNED NOT NULL, " +
			"query_begin INT(5) UNSIGNED NOT NULL, " +	
			"query_end INT(5) UNSIGNED NOT NULL, " +	
			"hit_begin INT(5) UNSIGNED NOT NULL, " +	
			"hit_end INT(5) UNSIGNED NOT NULL, " +	
			"bit_score DOUBLE UNSIGNED, " +
			"sw_score INT(6) UNSIGNED, " +			
			"evalue DOUBLE UNSIGNED, " +
			"identity FLOAT UNSIGNED, " +
			"positives FLOAT UNSIGNED, " +
			"covered_tms_query INT(2) UNSIGNED, " +
			"perc_covered_tms_query DOUBLE UNSIGNED, " +
			"covered_tms_hit INT(2) UNSIGNED, " +
			"perc_covered_tms_hit DOUBLE UNSIGNED, " +
		
			"similarity FLOAT UNSIGNED, " +
			"overlap INT(5) UNSIGNED NOT NULL, " +
			"alignment_coverage_query FLOAT UNSIGNED, " +
			"alignment_coverage_hit FLOAT UNSIGNED, " +
			"selfscoreRatio_query FLOAT UNSIGNED, " +
			"selfscoreRatio_hit FLOAT UNSIGNED, " +
			
			"timestamp TIMESTAMP NOT NULL" +			
			")";
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static void create_table_bulk() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE bulk" +
			"(" +
			"id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"number INT(11) UNSIGNED NOT NULL, " +												
			"timestamp TIMESTAMP NOT NULL" +			
			")";
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void create_table_go_annotations() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE go_annotations " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"sequenceid INT(11) UNSIGNED NOT NULL, " +	
			"accession VARCHAR(255), " +
			"name VARCHAR(255), " +
			"term_type VARCHAR(55), " +
			"evidence_code VARCHAR(8), " +
			"uniprot_accession VARCHAR(10), " +		//added 2011-02-09
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_da_cluster_assignments() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE da_cluster_assignments " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"sequenceid INT(11) UNSIGNED NOT NULL, " +	
			"begin INT(5) UNSIGNED, " +
			"end INT(5) UNSIGNED, " +
			"length INT(5) UNSIGNED, " +
			"name VARCHAR(30), " +
			"description VARCHAR(255), " +
			"method VARCHAR(255), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_clusters_mcl() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_mcl " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"sequences_sequenceid INT(10) UNSIGNED, " +
			"redundant VARCHAR(5), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_topology() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE topology " +
			"(" +
			"sequenceid INT(10) UNSIGNED NOT NULL, " +
			"PRIMARY KEY (sequenceid), " +
			"method VARCHAR(20) NOT NULL, " +
			"n_term VARCHAR(3) NOT NULL, " +
			"c_term VARCHAR(3) NOT NULL, " +				
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_sequences_other_database() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE sequences_other_database " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +			
			"name VARCHAR(255), " +
			"additional_information VARCHAR(255), " +
			"sequenceid INT(10) UNSIGNED, " +
			"md5 VARCHAR(32) NOT NULL, " +					
			"length MEDIUMINT(8) UNSIGNED, " +
			"db VARCHAR(20), " +
			"linkouturl TEXT, " +
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_other_database() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE other_database " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"sequenceid INT(11) UNSIGNED NOT NULL, " +
			"sequences_other_database_sequenceid INT(11) UNSIGNED NOT NULL, " +
			"alignment_length INT(5) UNSIGNED, " +
			"query_start INT(5) UNSIGNED, " +
			"query_end INT(5) UNSIGNED, " +
			"query_coverage FLOAT, " +
			"hit_start INT(5) UNSIGNED, " +
			"hit_end INT(5) UNSIGNED, " +
			"hit_coverage FLOAT, " +
			"bitscore FLOAT, " +
			"evalue DOUBLE, " +
			"ident FLOAT, " +		
			"db VARCHAR(20), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_other_database_hierarchies() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE other_database_hierarchies " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"key_ VARCHAR(50), " +
			"description VARCHAR(255), " +
			"hierarchy VARCHAR(20), " +
			"comment VARCHAR(100), " +
			"db VARCHAR(20), " +				
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_pdbtm2scop_cath() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE pdbtm2scop_cath " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +			
			"pdb_id VARCHAR(6) NOT NULL, " +
			"domain VARCHAR(10) NOT NULL, " +
			"classification VARCHAR(20) NOT NULL, " +
			"db VARCHAR(20), " +
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
		
	
	public static void create_table_clusters_mcl_track() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_mcl_track " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"child_cluster_number INT(5) UNSIGNED, " +
			"child_cluster_id INT(5) UNSIGNED, " +
			"child_cluster_threshold FLOAT, " +		
			"intersection_size INT(6), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_clusters_slc() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_slc " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"sequences_sequenceid INT(10) UNSIGNED, " +
			"redundant VARCHAR(5), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_clusters_mcl_info() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_mcl_info " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED NOT NULL, " +
			"cluster_threshold FLOAT NOT NULL, " +
			"sequences INT(6), " +	
			"average_length INT(5) UNSIGNED, " +
			"median_length FLOAT UNSIGNED, " +	//added 31-05-2011
			"cores INT(5) UNSIGNED, " +
			"tms_range VARCHAR(10), " +
			"structural_homogeneity FLOAT, " +
			"sequences_with_GOA INT(6), " +	
			"functional_homogeneity FLOAT, " +
			"mean_psi FLOAT UNSIGNED, " +			//added 17-02-2011
			"median_psi FLOAT UNSIGNED, " +			//added 17-02-2011
			"proteins INT(6), " +	
			"proteins_archaea INT(6), " +	
			"proteins_bacteria INT(6), " +	
			"proteins_eukaryota INT(6), " +	
			//"proteins_other INT(6), " +	
			"proteins_viruses INT(6), " +		//addedd 16-02-2011
			"proteins_misc INT(6), " +			//added 16-02-2011
			"superkingdom_composition VARCHAR(40), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void create_table_clusters_mcl_nr_info2() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_mcl_nr_info2 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED NOT NULL, " +
			"cluster_threshold FLOAT NOT NULL, " +
			"sequences INT(6), " +	
			"average_length INT(5) UNSIGNED, " +
			"median_length FLOAT UNSIGNED, " +	//added 31-05-2011
			"cores INT(5) UNSIGNED, " +
			"tms_range VARCHAR(10), " +
			"structural_homogeneity FLOAT, " +
			"sequences_with_GOA INT(6), " +	
			"functional_homogeneity FLOAT, " +
			"mean_psi FLOAT UNSIGNED, " +			//added 17-02-2011
			"median_psi FLOAT UNSIGNED, " +			//added 17-02-2011
			"proteins INT(6), " +	
			"proteins_archaea INT(6), " +	
			"proteins_bacteria INT(6), " +	
			"proteins_eukaryota INT(6), " +	
			//"proteins_other INT(6), " +	
			"proteins_viruses INT(6), " +		//addedd 16-02-2011
			"proteins_misc INT(6), " +			//added 16-02-2011
			"superkingdom_composition VARCHAR(40), " +	
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static void create_table_clusters_mcl_nr_info() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_mcl_nr_info " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED NOT NULL, " +
			"cluster_threshold FLOAT NOT NULL, " +
			"sequences INT(6), " +	
			"average_length INT(5) UNSIGNED, " +
			"median_length FLOAT UNSIGNED, " +	//added 31-05-2011
			"cores INT(5) UNSIGNED, " +
			"tms_range VARCHAR(10), " +
			"structural_homogeneity FLOAT, " +
			"sequences_with_GOA INT(6), " +	
			"functional_homogeneity FLOAT, " +
			"mean_psi FLOAT UNSIGNED, " +			//added 17-02-2011
			"median_psi FLOAT UNSIGNED, " +			//added 17-02-2011
			"proteins INT(6), " +	
			"proteins_archaea INT(6), " +	
			"proteins_bacteria INT(6), " +	
			"proteins_eukaryota INT(6), " +	
			//"proteins_other INT(6), " +	
			"proteins_viruses INT(6), " +		//addedd 16-02-2011
			"proteins_misc INT(6), " +			//added 16-02-2011
			"superkingdom_composition VARCHAR(40), " +	
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static void create_tms_cores2() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE tms_cores2 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"tms_core_id INT(5) UNSIGNED NOT NULL, " +	
			"sequenceid INT(10) UNSIGNED, " +			
			"sequence_aligned VARCHAR(150)," +
			"begin_aligned INT(5) UNSIGNED NOT NULL, " +	
			"end_aligned INT(5) UNSIGNED NOT NULL, " +	
			"length_aligned INT(5) UNSIGNED NOT NULL, " +				
			"sequence_unaligned VARCHAR(150)," +
			"begin_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"end_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"length_unaligned INT(5) UNSIGNED NOT NULL, " +				
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_tms_blocks2() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE tms_blocks2 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"tms_block_id INT(5) UNSIGNED NOT NULL, " +	
			"sequenceid INT(10) UNSIGNED, " +
			"sequence_aligned VARCHAR(150)," +
			"begin_aligned INT(5) UNSIGNED NOT NULL, " +	
			"end_aligned INT(5) UNSIGNED NOT NULL, " +	
			"length_aligned INT(5) UNSIGNED NOT NULL, " +				
			"sequence_unaligned VARCHAR(150)," +
			"begin_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"end_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"length_unaligned INT(5) UNSIGNED NOT NULL, " +		
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void create_tms_cores() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE tms_cores " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"tms_core_id INT(5) UNSIGNED NOT NULL, " +	
			"sequenceid INT(10) UNSIGNED, " +			
			"sequence_aligned VARCHAR(150)," +
			"begin_aligned INT(5) UNSIGNED NOT NULL, " +	
			"end_aligned INT(5) UNSIGNED NOT NULL, " +	
			"length_aligned INT(5) UNSIGNED NOT NULL, " +				
			"sequence_unaligned VARCHAR(150)," +
			"begin_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"end_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"length_unaligned INT(5) UNSIGNED NOT NULL, " +				
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_tms_blocks() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE tms_blocks " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"tms_block_id INT(5) UNSIGNED NOT NULL, " +	
			"sequenceid INT(10) UNSIGNED, " +
			"sequence_aligned VARCHAR(150)," +
			"begin_aligned INT(5) UNSIGNED NOT NULL, " +	
			"end_aligned INT(5) UNSIGNED NOT NULL, " +	
			"length_aligned INT(5) UNSIGNED NOT NULL, " +				
			"sequence_unaligned VARCHAR(150)," +
			"begin_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"end_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"length_unaligned INT(5) UNSIGNED NOT NULL, " +		
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_clusters_mcl_2ndRun() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_mcl_2ndRun " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"sequences_sequenceid INT(10) UNSIGNED, " +
			"redundant VARCHAR(5), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_clusters_hctest() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_hctest " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"sequences_sequenceid INT(10) UNSIGNED, " +
			"redundant90 VARCHAR(5), " +
			"redundant80 VARCHAR(5), " +
			"redundant70 VARCHAR(5), " +
			"redundant60 VARCHAR(5), " +
			"redundant50 VARCHAR(5), " +
			"redundant40 VARCHAR(5), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_clusters_hctest_nr90_info() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_hctest_nr90_info " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED NOT NULL, " +
			"cluster_threshold FLOAT NOT NULL, " +
			"sequences INT(6), " +	
			"average_length INT(5) UNSIGNED, " +
			"cores INT(5) UNSIGNED, " +
			"tms_range VARCHAR(10), " +
			"structural_homogeneity FLOAT, " +
			"sequences_with_GOA INT(6), " +	
			"functional_homogeneity FLOAT, " +
			"proteins INT(6), " +	
			"proteins_archaea INT(6), " +	
			"proteins_bacteria INT(6), " +	
			"proteins_eukaryota INT(6), " +	
			"proteins_viruses INT(6), " +	
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_clusters_hctest_nr80_info() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_hctest_nr80_info " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED NOT NULL, " +
			"cluster_threshold FLOAT NOT NULL, " +
			"sequences INT(6), " +	
			"average_length INT(5) UNSIGNED, " +
			"cores INT(5) UNSIGNED, " +
			"tms_range VARCHAR(10), " +
			"structural_homogeneity FLOAT, " +
			"sequences_with_GOA INT(6), " +	
			"functional_homogeneity FLOAT, " +
			"proteins INT(6), " +	
			"proteins_archaea INT(6), " +	
			"proteins_bacteria INT(6), " +	
			"proteins_eukaryota INT(6), " +	
			"proteins_viruses INT(6), " +	
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_clusters_hctest_nr70_info() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_hctest_nr70_info " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED NOT NULL, " +
			"cluster_threshold FLOAT NOT NULL, " +
			"sequences INT(6), " +	
			"average_length INT(5) UNSIGNED, " +
			"cores INT(5) UNSIGNED, " +
			"tms_range VARCHAR(10), " +
			"structural_homogeneity FLOAT, " +
			"sequences_with_GOA INT(6), " +	
			"functional_homogeneity FLOAT, " +
			"proteins INT(6), " +	
			"proteins_archaea INT(6), " +	
			"proteins_bacteria INT(6), " +	
			"proteins_eukaryota INT(6), " +	
			"proteins_viruses INT(6), " +	
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_clusters_hctest_nr60_info() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_hctest_nr60_info " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED NOT NULL, " +
			"cluster_threshold FLOAT NOT NULL, " +
			"sequences INT(6), " +	
			"average_length INT(5) UNSIGNED, " +
			"cores INT(5) UNSIGNED, " +
			"tms_range VARCHAR(10), " +
			"structural_homogeneity FLOAT, " +
			"sequences_with_GOA INT(6), " +	
			"functional_homogeneity FLOAT, " +
			"proteins INT(6), " +	
			"proteins_archaea INT(6), " +	
			"proteins_bacteria INT(6), " +	
			"proteins_eukaryota INT(6), " +	
			"proteins_viruses INT(6), " +	
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_clusters_hctest_nr50_info() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_hctest_nr50_info " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED NOT NULL, " +
			"cluster_threshold FLOAT NOT NULL, " +
			"sequences INT(6), " +	
			"average_length INT(5) UNSIGNED, " +
			"cores INT(5) UNSIGNED, " +
			"tms_range VARCHAR(10), " +
			"structural_homogeneity FLOAT, " +
			"sequences_with_GOA INT(6), " +	
			"functional_homogeneity FLOAT, " +
			"proteins INT(6), " +	
			"proteins_archaea INT(6), " +	
			"proteins_bacteria INT(6), " +	
			"proteins_eukaryota INT(6), " +	
			"proteins_viruses INT(6), " +	
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_clusters_hctest_nr40_info() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_hctest_nr40_info " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED NOT NULL, " +
			"cluster_threshold FLOAT NOT NULL, " +
			"sequences INT(6), " +	
			"average_length INT(5) UNSIGNED, " +
			"cores INT(5) UNSIGNED, " +
			"tms_range VARCHAR(10), " +
			"structural_homogeneity FLOAT, " +
			"sequences_with_GOA INT(6), " +	
			"functional_homogeneity FLOAT, " +
			"proteins INT(6), " +	
			"proteins_archaea INT(6), " +	
			"proteins_bacteria INT(6), " +	
			"proteins_eukaryota INT(6), " +	
			"proteins_viruses INT(6), " +	
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_tms_cores_hctest_nr90() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE tms_cores_hctest_nr90 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"tms_core_id INT(5) UNSIGNED NOT NULL, " +	
			"sequences_sequenceid INT(10) UNSIGNED, " +			
			"sequence_aligned VARCHAR(100)," +
			"begin_aligned INT(5) UNSIGNED NOT NULL, " +	
			"end_aligned INT(5) UNSIGNED NOT NULL, " +	
			"length_aligned INT(5) UNSIGNED NOT NULL, " +				
			"sequence_unaligned VARCHAR(100)," +
			"begin_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"end_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"length_unaligned INT(5) UNSIGNED NOT NULL, " +				
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_tms_cores_hctest_nr80() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE tms_cores_hctest_nr80 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"tms_core_id INT(5) UNSIGNED NOT NULL, " +	
			"sequences_sequenceid INT(10) UNSIGNED, " +			
			"sequence_aligned VARCHAR(100)," +
			"begin_aligned INT(5) UNSIGNED NOT NULL, " +	
			"end_aligned INT(5) UNSIGNED NOT NULL, " +	
			"length_aligned INT(5) UNSIGNED NOT NULL, " +				
			"sequence_unaligned VARCHAR(100)," +
			"begin_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"end_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"length_unaligned INT(5) UNSIGNED NOT NULL, " +				
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_tms_cores_hctest_nr70() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE tms_cores_hctest_nr70 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"tms_core_id INT(5) UNSIGNED NOT NULL, " +	
			"sequences_sequenceid INT(10) UNSIGNED, " +			
			"sequence_aligned VARCHAR(100)," +
			"begin_aligned INT(5) UNSIGNED NOT NULL, " +	
			"end_aligned INT(5) UNSIGNED NOT NULL, " +	
			"length_aligned INT(5) UNSIGNED NOT NULL, " +				
			"sequence_unaligned VARCHAR(100)," +
			"begin_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"end_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"length_unaligned INT(5) UNSIGNED NOT NULL, " +				
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_tms_cores_hctest_nr60() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE tms_cores_hctest_nr60 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"tms_core_id INT(5) UNSIGNED NOT NULL, " +	
			"sequences_sequenceid INT(10) UNSIGNED, " +			
			"sequence_aligned VARCHAR(100)," +
			"begin_aligned INT(5) UNSIGNED NOT NULL, " +	
			"end_aligned INT(5) UNSIGNED NOT NULL, " +	
			"length_aligned INT(5) UNSIGNED NOT NULL, " +				
			"sequence_unaligned VARCHAR(100)," +
			"begin_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"end_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"length_unaligned INT(5) UNSIGNED NOT NULL, " +				
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_tms_cores_hctest_nr50() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE tms_cores_hctest_nr50 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"tms_core_id INT(5) UNSIGNED NOT NULL, " +	
			"sequences_sequenceid INT(10) UNSIGNED, " +			
			"sequence_aligned VARCHAR(100)," +
			"begin_aligned INT(5) UNSIGNED NOT NULL, " +	
			"end_aligned INT(5) UNSIGNED NOT NULL, " +	
			"length_aligned INT(5) UNSIGNED NOT NULL, " +				
			"sequence_unaligned VARCHAR(100)," +
			"begin_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"end_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"length_unaligned INT(5) UNSIGNED NOT NULL, " +				
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_tms_cores_hctest_nr40() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE tms_cores_hctest_nr40 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"tms_core_id INT(5) UNSIGNED NOT NULL, " +	
			"sequences_sequenceid INT(10) UNSIGNED, " +			
			"sequence_aligned VARCHAR(100)," +
			"begin_aligned INT(5) UNSIGNED NOT NULL, " +	
			"end_aligned INT(5) UNSIGNED NOT NULL, " +	
			"length_aligned INT(5) UNSIGNED NOT NULL, " +				
			"sequence_unaligned VARCHAR(100)," +
			"begin_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"end_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"length_unaligned INT(5) UNSIGNED NOT NULL, " +				
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_clusters_hctest_track() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_hctest_track " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"child_cluster_number INT(5) UNSIGNED, " +
			"child_cluster_id INT(5) UNSIGNED, " +
			"child_cluster_threshold FLOAT, " +		
			"intersection_size INT(6), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void create_table_pdbtm2opm() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE pdbtm2opm " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +			
			"pdb_id VARCHAR(6) NOT NULL, " +			
			"classification VARCHAR(20) NOT NULL, " +			
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void create_table_camps2uniprot() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE camps2uniprot " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +			
			"sequenceid INT(11) UNSIGNED, " +	
			//"uniprot_name VARCHAR(255), " +
			"entry_name VARCHAR(15), " +
			"accession VARCHAR(10), " +
			"description TEXT, " +
			"taxonomyid INT(10) UNSIGNED, " +
			"subset VARCHAR(10), " +
			"linkouturl VARCHAR(50), " +		
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void create_table_camps2pdb() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE camps2pdb " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +			
			"sequenceid INT(11) UNSIGNED, " +	
			"pdb_name VARCHAR(255), " +
			"linkouturl TEXT, " +		
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static void create_table_camps2pdb30Iden() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE camps2pdb30Iden " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +			
			"sequenceid INT(11) UNSIGNED, " +	
			"pdb_name VARCHAR(255), " +
			"linkouturl TEXT, " +		
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void create_table_camps2genbank() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE camps2genbank " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +			
			"sequenceid INT(11) UNSIGNED, " +	
			"genbank_name VARCHAR(255), " +
			"linkouturl TEXT, " +		
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_ec_classification() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE ec_classification " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"sequenceid INT(11) UNSIGNED NOT NULL, " +	
			"classification VARCHAR(20), " +
			"uniprot_name VARCHAR(255), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void create_table_clusters_cross_db2() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_cross_db2 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"db VARCHAR(50), " +
			"link_description TEXT, " +
			"instances_abs INT(6), " +
			"instances_rel DOUBLE, " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static void create_table_clusters_cross_db() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_cross_db " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"db VARCHAR(50), " +
			"link_description TEXT, " +
			"instances_abs INT(6), " +
			"instances_rel DOUBLE, " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static void create_table_cp_clusters2() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE cp_clusters2 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"code VARCHAR(30)," +
			"description TEXT," +
			"super_cluster_id INT(5) UNSIGNED, " +
			"super_cluster_threshold FLOAT, " +
			"type VARCHAR(20)," +
			"comment VARCHAR(255), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static void create_table_cp_clusters() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE cp_clusters " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"code VARCHAR(30)," +
			"description TEXT," +
			"super_cluster_id INT(5) UNSIGNED, " +
			"super_cluster_threshold FLOAT, " +
			"type VARCHAR(20)," +
			"comment VARCHAR(255), " +
			"timestamp TIMESTAMP NOT NULL" +
			"structures VARCHAR(5), " +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_pfam() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE pfam " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"accession VARCHAR(30)," +
			"name VARCHAR(255)," +
			"description VARCHAR(255)," +
			//"clan VARCHAR(30)," +
			"clan_id VARCHAR(30), " +
			"clan_description VARCHAR(255)," +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_taxonomy2cpCluster() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE taxonomy2cpCluster " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"taxonomyid INT(10) UNSIGNED NOT NULL, " +
			"taxonomy VARCHAR(255), " +		
			"superkingdom VARCHAR(255), " +
			"cluster_code VARCHAR(30)," +
			"cluster_size INT(6), " +	
			"taxonomic_instances INT(6), " +
			"type VARCHAR(20)," +			
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_clusters_mcl_structures() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE clusters_mcl_structures2 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"pdbid VARCHAR(6), " +
			"method VARCHAR(30), " +
			"resolution FLOAT, " +
			"query_coverage FLOAT, " +
			"hit_coverage FLOAT, " +
			"bitscore FLOAT, " +
			"evalue DOUBLE, " +
			"ident FLOAT, " +				
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_statistics() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE statistics " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"statistic VARCHAR(255)," +
			"value INT(6), " +	
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_camps2eggnog() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE camps2eggnog " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +			
			"sequenceid INT(11) UNSIGNED, " +	
			"eggnog_protein VARCHAR(50), " +
			"start INT(5) UNSIGNED, " +
			"end INT(5) UNSIGNED, " +
			"group_ VARCHAR(30), " +
			"description TEXT, " +
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void create_table_fh_clusters2() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE fh_clusters2 " + // 2 for reRun
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"code VARCHAR(30)," +
			"sequenceid INT(10) UNSIGNED, " +
			"redundant VARCHAR(5), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void create_table_fh_clusters() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE fh_clusters " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"code VARCHAR(30)," +
			"sequenceid INT(10) UNSIGNED, " +
			"redundant VARCHAR(5), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_topology_images() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE topology_images " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"sequences_sequenceid INT(10) UNSIGNED, " +
			"image mediumblob, " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_fh_clusters_info() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE fh_clusters_info " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"code VARCHAR(30) NOT NULL," +
			"sequences INT(6), " +	
			"average_length INT(5) UNSIGNED, " +
			"cores INT(5) UNSIGNED, " +
			"tms_range VARCHAR(10), " +
			"structural_homogeneity FLOAT, " +
			"sequences_with_GOA INT(6), " +	
			"functional_homogeneity FLOAT, " +
			"mean_psi FLOAT UNSIGNED, " +			//added 22-02-2011
			"median_psi FLOAT UNSIGNED, " +			//added 22-02-2011
			"proteins INT(6), " +	
			"proteins_archaea INT(6), " +	
			"proteins_bacteria INT(6), " +	
			"proteins_eukaryota INT(6), " +	
			//"proteins_other INT(6), " +	
			"proteins_viruses INT(6), " +		//addedd 22-02-2011
			"proteins_misc INT(6), " +			//added 22-02-2011
			"superkingdom_composition VARCHAR(40), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_fh_clusters_nr_info() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE fh_clusters_nr_info " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"code VARCHAR(30) NOT NULL," +
			"sequences INT(6), " +	
			"average_length INT(5) UNSIGNED, " +
			"cores INT(5) UNSIGNED, " +
			"tms_range VARCHAR(10), " +
			"structural_homogeneity FLOAT, " +
			"sequences_with_GOA INT(6), " +	
			"functional_homogeneity FLOAT, " +
			"mean_psi FLOAT UNSIGNED, " +			//added 22-02-2011
			"median_psi FLOAT UNSIGNED, " +			//added 22-02-2011
			"proteins INT(6), " +	
			"proteins_archaea INT(6), " +	
			"proteins_bacteria INT(6), " +	
			"proteins_eukaryota INT(6), " +	
			//"proteins_other INT(6), " +	
			"proteins_viruses INT(6), " +		//addedd 22-02-2011
			"proteins_misc INT(6), " +			//added 22-02-2011
			"superkingdom_composition VARCHAR(40), " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_fh_clusters_nr_da() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE fh_clusters_nr_da " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"code VARCHAR(30) NOT NULL," +
			"domain_architecture TEXT, " +
			"simap_id VARCHAR(20), " +
			"frequency_all_members DOUBLE, " +
			"frequency_members_with_pfam_domain DOUBLE, " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_camps2uniprot_taxonomies() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE camps2uniprot_taxonomies " +
			"(" +
			"taxonomyid INT(10) UNSIGNED NOT NULL, " +
			"PRIMARY KEY (taxonomyid), " +		
			"taxonomy VARCHAR(255), " +		//added 07-Febr-2011
			"species VARCHAR(255), " +
			"genus VARCHAR(255), " +
			"family VARCHAR(255), " +
			"order_ VARCHAR(255), " +
			"class VARCHAR(255), " +
			"phylum VARCHAR(255), " +
			"kingdom VARCHAR(255), " +
			"superkingdom VARCHAR(255), " +				
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_proteins_merged() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE proteins_merged " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +			
			"sequenceid INT(10) UNSIGNED NOT NULL, " +
			"name VARCHAR(100) NOT NULL, " +
			"description VARCHAR(255), " +
			"databaseid MEDIUMINT(8) UNSIGNED NOT NULL, " +
			"taxonomyid INT(10) UNSIGNED, " +
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_taxonomies_merged() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE taxonomies_merged " +
			"(" +
			"taxonomyid INT(10) UNSIGNED NOT NULL, " +
			"PRIMARY KEY (taxonomyid), " +		
			"taxonomy VARCHAR(255), " +		
			"species VARCHAR(255), " +
			"genus VARCHAR(255), " +
			"family VARCHAR(255), " +
			"order_ VARCHAR(255), " +
			"class VARCHAR(255), " +
			"phylum VARCHAR(255), " +
			"kingdom VARCHAR(255), " +
			"superkingdom VARCHAR(255), " +				
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_fh_clusters_tms_cores() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE fh_clusters_tms_cores " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"code VARCHAR(30)," +
			"tms_core_id INT(5) UNSIGNED NOT NULL, " +	
			"sequenceid INT(10) UNSIGNED, " +			
			"sequence_aligned VARCHAR(150)," +
			"begin_aligned INT(5) UNSIGNED NOT NULL, " +	
			"end_aligned INT(5) UNSIGNED NOT NULL, " +	
			"length_aligned INT(5) UNSIGNED NOT NULL, " +				
			"sequence_unaligned VARCHAR(150)," +
			"begin_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"end_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"length_unaligned INT(5) UNSIGNED NOT NULL, " +				
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_fh_clusters_tms_blocks() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE fh_clusters_tms_blocks " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"code VARCHAR(30)," +
			"tms_block_id INT(5) UNSIGNED NOT NULL, " +	
			"sequenceid INT(10) UNSIGNED, " +
			"sequence_aligned VARCHAR(150)," +
			"begin_aligned INT(5) UNSIGNED NOT NULL, " +	
			"end_aligned INT(5) UNSIGNED NOT NULL, " +	
			"length_aligned INT(5) UNSIGNED NOT NULL, " +				
			"sequence_unaligned VARCHAR(150)," +
			"begin_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"end_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"length_unaligned INT(5) UNSIGNED NOT NULL, " +		
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_fh_clusters_cross_db() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE fh_clusters_cross_db " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"code VARCHAR(30)," +
			"db VARCHAR(50), " +
			"link_description TEXT, " +
			"instances_abs INT(6), " +
			"instances_rel DOUBLE, " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_fh_clusters_structures() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE fh_clusters_structures " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"code VARCHAR(30)," +
			"pdbid VARCHAR(6), " +
			"title VARCHAR(255), " +
			"method VARCHAR(30), " +
			"resolution FLOAT, " +
			"query_coverage FLOAT, " +
			"hit_coverage FLOAT, " +
			"bitscore FLOAT, " +
			"evalue DOUBLE, " +
			"ident FLOAT, " +				
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static void create_table_fh_clusters_structures0() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE fh_clusters_structures0 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"code VARCHAR(30)," +
			"pdbid VARCHAR(6), " +
			"title VARCHAR(255), " +
			"method VARCHAR(30), " +
			"resolution FLOAT, " +
			"query_coverage FLOAT, " +
			"hit_coverage FLOAT, " +
			"bitscore FLOAT, " +
			"evalue DOUBLE, " +
			"ident FLOAT, " +				
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void create_table_sequences2names() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE sequences2names " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +			
			"sequenceid INT(10) UNSIGNED NOT NULL, " +
			"md5 VARCHAR(32) NOT NULL, " +
			"name VARCHAR(100) NOT NULL, " +
			"description TEXT, " +			
			"taxonomyid INT(10) UNSIGNED, " +
			"taxonomy VARCHAR(255), " +		
			"in_SC VARCHAR(5), " +
			"sc_code VARCHAR(30)," +
			"timestamp TIMESTAMP NOT NULL" +
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_md_clusters_tms_cores() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE md_clusters_tms_cores " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"tms_core_id INT(5) UNSIGNED NOT NULL, " +	
			"sequenceid INT(10) UNSIGNED, " +			
			"sequence_aligned VARCHAR(150)," +
			"begin_aligned INT(5) UNSIGNED NOT NULL, " +	
			"end_aligned INT(5) UNSIGNED NOT NULL, " +	
			"length_aligned INT(5) UNSIGNED NOT NULL, " +				
			"sequence_unaligned VARCHAR(150)," +
			"begin_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"end_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"length_unaligned INT(5) UNSIGNED NOT NULL, " +				
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_md_clusters_tms_blocks() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE md_clusters_tms_blocks " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"cluster_id INT(5) UNSIGNED, " +
			"cluster_threshold FLOAT, " +
			"tms_block_id INT(5) UNSIGNED NOT NULL, " +	
			"sequenceid INT(10) UNSIGNED, " +
			"sequence_aligned VARCHAR(150)," +
			"begin_aligned INT(5) UNSIGNED NOT NULL, " +	
			"end_aligned INT(5) UNSIGNED NOT NULL, " +	
			"length_aligned INT(5) UNSIGNED NOT NULL, " +				
			"sequence_unaligned VARCHAR(150)," +
			"begin_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"end_unaligned INT(5) UNSIGNED NOT NULL, " +	
			"length_unaligned INT(5) UNSIGNED NOT NULL, " +		
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_taxonomy2cpClusterCount() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE taxonomy2cpClusterCount " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"taxonomyid INT(10) UNSIGNED NOT NULL, " +
			"taxonomy VARCHAR(255), " +		
			"superkingdom VARCHAR(255), " +
			"num_SC INT(5), " +	
			"num_FH INT(5), " +	
			"num_MD INT(5), " +	
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_fh_clusters_architecture() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE fh_clusters_architecture " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"code VARCHAR(30) NOT NULL," +
			"architecture TEXT, " +
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_taxonomy2cpCluster2() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE taxonomy2cpCluster2 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"taxonomyid INT(10) UNSIGNED NOT NULL, " +
			"taxonomy VARCHAR(255), " +		
			"superkingdom VARCHAR(255), " +
			"cluster_code VARCHAR(30)," +
			"cluster_size INT(6), " +	
			"taxonomic_instances INT(6), " +
			"type VARCHAR(20)," +			
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void create_table_taxonomy2cpClusterCount2() {
		try {
			Connection conn = DBAdaptor.getConnection(DB);
			Statement stm = conn.createStatement();
			
			String sqlString = "CREATE TABLE taxonomy2cpClusterCount2 " +
			"(" +
			"id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), " +
			"taxonomyid INT(10) UNSIGNED NOT NULL, " +
			"taxonomy VARCHAR(255), " +		
			"superkingdom VARCHAR(255), " +
			"num_SC INT(5), " +	
			"num_FH INT(5), " +	
			"num_MD INT(5), " +	
			"timestamp TIMESTAMP NOT NULL" +			
			")";			
			stm.executeUpdate(sqlString);
			
			stm.close();
			stm = null;
			conn.close();
			conn = null;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) {		
//		create_table_taxonomies();
//	    create_table_clusters_mcl();
//		create_table_sequences_other_database();	
//		create_table_other_database_hierarchies();
//	    create_table_clusters_slc();
//	    create_table_clusters_mcl_nr_info();
//		create_table_clusters_mcl_info();
//		create_tms_cores();
//		create_tms_blocks();
//	    create_table_clusters_mcl_2ndRun();
//		create_table_clusters_hctest();
//		create_table_clusters_hctest_track();
//		create_table_statistics();
//		create_table_camps2eggnog();
//		create_table_topology_images();
//		create_table_fh_clusters_nr_da();
//		create_table_camps2uniprot();
//		create_table_fh_clusters_architecture();
	}

}
