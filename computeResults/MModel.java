package computeResults;

public class MModel {
	
	private int threshold;
	private int clusterid;
	private int number_of_proteins;
	private int tm_no;
	private String code;
	private String pfamCode;
	
	public static int threshAll[] = new int[101];
	/*static int thresh10;
	static int thresh15;
	static int thresh20;
	static int thresh25;
	static int thresh30;
	static int thresh35;
	static int thresh40;
	static int thresh45;*/
	
	
	
	public MModel(){
		this.setThreshold(0);
		this.setClusterid(0);
		this.setNumber_of_proteins(0);
		this.setTm_no(0);
		this.setCode("");
	}
	public String getpfamCode() {
		return pfamCode;
	}

	public void setpfamCode(String s) {
		this.pfamCode = s;
	}
	public String getCode() {
		return code;
	}

	public void setCode(String s) {
		this.code = s;
	}

	public int getThreshold() {
		return threshold;
	}

	public void setThreshold(int threshold) {
		this.threshold = threshold;
	}

	public int getClusterid() {
		return clusterid;
	}

	public void setClusterid(int clusterid) {
		this.clusterid = clusterid;
	}

	public int getTm_no() {
		return tm_no;
	}

	public void setTm_no(int tm_no) {
		this.tm_no = tm_no;
	}

	public int getNumber_of_proteins() {
		return number_of_proteins;
	}

	public void setNumber_of_proteins(int number_of_proteins) {
		this.number_of_proteins = number_of_proteins;
	}

}
