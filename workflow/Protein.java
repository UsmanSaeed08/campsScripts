package workflow;

public class Protein {
	
	//String md5 ="";
	int seq_id;
	//String uniprot_id="";
	//String description="";
	public String link_description;
	public double instances_rel;
	int taxid=-1;
	
	public int child_clusterid;
	public float child_clus_thresh;
	public int intersectionsSz;
	
	public Protein(){
		//this.md5 = "";
		//this.seq_id = 0;
		//this.uniprot_id = "";
		//this.description = "";
		
		this.taxid = -1;
				
	}
	public void set(String l,double rel){
		this.link_description = l;
		this.instances_rel = rel;
	}
	public void setforTrack(int cid,float ct,int intersect){
		this.child_clusterid = cid;
		this.child_clus_thresh = ct;
		this.intersectionsSz = intersect;
	}
	public Protein(int id){
		//this.md5 = "";
		//this.seq_id = id;
		this.taxid=id;
	}
	
	/*
	public void set_unip_data(String i, String d){
		this.uniprot_id = i;
		this.description = d;
	}
*/	

}
