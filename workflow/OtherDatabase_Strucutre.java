package workflow;

public class OtherDatabase_Strucutre {
/*
 *  AND db=\"pdbtm\"");
 */
	public int sequences_other_database_sequenceid;
	public float query_coverage;
	public float hit_coverage;
	public float bitscore;
	public double evalue;
	public float ident;
	public void set(int s, float q, float h, float b, double e, float i){
		this.sequences_other_database_sequenceid = s;
		this.query_coverage = q;
		this.hit_coverage = h;
		this.bitscore = b;
		this.evalue = e;
		this.ident = i;
	}
}
