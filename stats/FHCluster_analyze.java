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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import computeResults.MModel;

import utils.DBAdaptor;

public class FHCluster_analyze {

	/**
	 * @param args
	 */

	public static ArrayList<MModel> Model_list = new ArrayList <MModel>();
	public static HashMap <Integer, Integer> ProteinsInHmm = new HashMap<Integer, Integer>();// list of proteins lost as no cluster made in last split round 100 thresh
	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// getting FH clusters
		//String path = "F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/HMMs/CAMPS4_1/";
		//final File folder = new File(path);
		//listFilesForFolder(folder);
		System.out.print("\ngetting fh Ids\n");
		getFHids();
		System.out.print("\ngetting fh Size distributions\n");
		getFHclustersSizeDistribution();
		System.out.print("\ngetting fh Size ratios\n");
		getFHClustersSizeRatio();
		System.out.print("\ngetting fh descriptions\n");
		getFHDescriptions();
		closeConnection();
		System.out.print("\nDone\n");
		

	}
	private static void closeConnection() {
		// TODO Auto-generated method stub
		try{
			CAMPS_CONNECTION.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void getFHDescriptions() {
		// TODO Auto-generated method stub
		try{
			
			PreparedStatement p = CAMPS_CONNECTION.prepareStatement("select description from pfam where accession=?");
			PreparedStatement p2 = CAMPS_CONNECTION.prepareStatement("select sequences from fh_clusters_info where code=?");
			
			ResultSet r;
			ResultSet r2;
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/FH_Cluster_Descriptions.txt")));
			bw.write("FH_ClusterCode\tSize_of_FHCluster\tDescription");
			bw.newLine();
			for (int i =0;i<=Model_list.size()-1;i++){
				String fh_clusterCode = Model_list.get(i).getCode();
				String line = Model_list.get(i).getpfamCode();
				//PF00487 [Score: 1.0, Coverage: 95.0]
				String pfam_code = (line.split(" "))[0];
				pfam_code = pfam_code.trim();
				
				p.setString(1, pfam_code); // gets the description from pfam table given the pfam code
				r = p.executeQuery();
				String Description = "";
				while(r.next()){
					Description = r.getString(1);
				}
				r.close();
				
				// gets the size of fhcluster give the fh cluster code
				p2.setString(1, fh_clusterCode); 
				r2 = p2.executeQuery();
				int size = 0;
				while(r2.next()){
					size = r2.getInt(1);
				}
				r2.close();
				bw.write(fh_clusterCode+"\t"+size+"\t"+Description);
				bw.newLine();
			}
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
	private static void getFHClustersSizeRatio() {
		// TODO Auto-generated method stub

		PreparedStatement p;
		ResultSet r = null;

		try{
			HashMap <String, Integer> ParentSCtoFHno = new HashMap<String, Integer>(); // Key sc clusters Code and value number of FH produced
			HashMap <String, Integer> ParentSCtosizeSC = new HashMap<String, Integer>(); // Key sc clusters Code and value is size of the original sc
			for (int i =0;i<=Model_list.size()-1;i++){
				int id = Model_list.get(i).getClusterid();
				float thresh = Model_list.get(i).getThreshold();

				p = CAMPS_CONNECTION.prepareStatement("select code from cp_clusters where cluster_id = ? and cluster_threshold=? and type=\"sc_cluster\"");
				p.setInt(1, id);
				p.setFloat(2, thresh);
				r = p.executeQuery();
				String scCode = "";
				while(r.next()){
					scCode = r.getString(1);
				}
				// so from the information of parents of fh clusters we get the code of sc cluster
				// also if the same parent is repeated, the code increments every hit for this parent
				// therefore, in the end we have sc cluster and the number of fh clusters produced by this sc cluster
				if (ParentSCtoFHno.containsKey(scCode)){
					int x = ParentSCtoFHno.get(scCode);
					x = x+1;
					ParentSCtoFHno.put(scCode, x);
				}
				else{
					//int x = ParentSCtoFHno.get(scCode);
					int x = 1;
					ParentSCtoFHno.put(scCode, x);
				}
			}
			// inorder to find the original size of sc cluster which is also a parent. go through the file with sc cluster details
			// if the read sc cluster is also a parent cluster, get the size
			//	code+"\t"+description+"\t"+size+"\t"+TMRange+"\t"+taxa+"\t"+RepStruct+"\n"
			BufferedReader br = new BufferedReader(new FileReader(new File("F:/SC_Cluster_table.txt")));
			String line = "";
			while((line = br.readLine())!=null){
				String parts[] = line.split("\t");
				if(ParentSCtoFHno.containsKey(parts[0])){
					// is a parent SC cluster
					int size = Integer.parseInt(parts[2]);
					ParentSCtosizeSC.put(parts[0], size);
					// no worries of over write as the file has unique sc cluster at every line
				}
			}
			br.close();

			// Print results;
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:/FH_Cluster_table.txt")));
			bw.write("ParentSC_Clsuter\tSize_of_Parent_SC\tNumber_of_FH_Produced");
			bw.newLine();
			Iterator it = ParentSCtoFHno.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry)it.next();
				String key =(String) pair.getKey();
				int producedFh =(Integer) pair.getValue();
				int sizeofParentSC = ParentSCtosizeSC.get(key);
				String l = key +"\t"+ sizeofParentSC + "\t" + producedFh;
				bw.write(l);
				bw.newLine();
				it.remove(); // avoids a ConcurrentModificationException
			}
			bw.close();


		}
		catch(Exception e){
			e.printStackTrace();
		}

	}
	
	private static void getFHids() {
		// TODO Auto-generated method stub
		try{
			// since none of the sc cluster directly goes into becoming an fh clusters,
			// therefore, only super clusterids and thresholds are to be used
			PreparedStatement prepst;
			ResultSet r = null;
			prepst = CAMPS_CONNECTION.prepareStatement("select code,super_cluster_id,super_cluster_threshold,description from cp_clusters where type=\"fh_cluster\"");
			r = prepst.executeQuery();
			while(r.next()){
				String code = r.getString(1);
				int id = r.getInt(2);
				float thresh = r.getFloat(3);
				String pfam_code = r.getString(4);

				MModel model = new MModel();
				model.setThreshold((int) thresh);
				model.setClusterid(id);
				model.setCode(code);
				model.setpfamCode(pfam_code);

				int th = model.getThreshold();
				MModel.threshAll[th] = MModel.threshAll[th]+1;
				Model_list.add(model);
				//System.out.print("Thresh "+th+"    Clusid "+id+"\n");

			}
			r.close();
			prepst.close();
			for (int j =0; j<=100;j++){
				if (MModel.threshAll[j]!=0){
					//System.out.print("Threhold " + j + "    Number of Clusters "+MModel.threshAll[j] + "\n");
					System.out.print(j+"\n");
				}
			}
			System.out.print("\n********\n");
			for (int j =0; j<=100;j++){
				if (MModel.threshAll[j]!=0){
					//System.out.print("Threhold " + j + "    Number of Clusters "+MModel.threshAll[j] + "\n");
					System.out.print(MModel.threshAll[j] + "\n");
				}
			}

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void getFHclustersSizeDistribution() {
		// TODO Auto-generated method stub

		PreparedStatement p;
		ResultSet r = null;
		try{
			p = CAMPS_CONNECTION.prepareStatement("select sequenceid from fh_clusters where code = ?");

			for (int i =0;i<=Model_list.size()-1;i++){
				String coda = Model_list.get(i).getCode();
				int tempsize = 0;
				p.setString(1, coda);
				r = p.executeQuery();
				while(r.next()){
					int prot = r.getInt(1);
					tempsize ++ ;
					//int prot = r2.getInt(1);
					if(!ProteinsInHmm.containsKey(prot)){
						ProteinsInHmm.put(prot, null);
					}
				}
				r.close();
				Model_list.get(i).setNumber_of_proteins(tempsize);
			}
			p.close();
			//CAMPS_CONNECTION.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}

		List<Integer> sizeList = new ArrayList<>();
		//get max
		Integer max =0;
		int NumberProteinsInFH = 0;
		for (int i =0;i<=Model_list.size()-1;i++){
			NumberProteinsInFH = NumberProteinsInFH + Model_list.get(i).getNumber_of_proteins();
			if (Model_list.get(i).getNumber_of_proteins()>max)
				max = Model_list.get(i).getNumber_of_proteins();
		}
		//initialize till max
		for(int i =0;i<=max+1;i++){
			sizeList.add(i, 0);
		}
		//calc the frequency
		for (int i =0;i<=Model_list.size()-1;i++){
			int size = Model_list.get(i).getNumber_of_proteins();
			Integer t = (Integer) sizeList.get(size);
			t = t+1;
			sizeList.set(size, t);
		}
		System.out.print("The results for size calculation.. first printing sizes then no of clusters with that size\n\n");
		for(int i =0;i<=sizeList.size()-1;i++){
			if (sizeList.get(i)!=0){
				System.out.print(i+"\n");
			}
		}
		System.out.print("\n\n*************************************\n\n");
		for(int i =0;i<=sizeList.size()-1;i++){
			if (sizeList.get(i)!=0){
				System.out.print(sizeList.get(i)+"\n");
			}
		}
		System.out.print("\n\n*************************************\n\n");
		System.out.print("The exact Number of Proteins in HMMs: "+NumberProteinsInFH +"\n");
	}

}
