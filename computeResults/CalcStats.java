package computeResults;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import utils.DBAdaptor;

public class CalcStats {

	/**
	 * @param args
	 * The class computes the general stats but not specific comparisons with Cath and SCOP of Pfam. 
	 */
	private static ArrayList<String> files_results;
	public static ArrayList<MModel> Model_list = new ArrayList <MModel>();
	public static HashMap <Integer, Integer> ProteinsInHmm = new HashMap<Integer, Integer>();// list of proteins lost as no cluster made in last split round 100 thresh
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("Hey");
		//String path = "F:/SC_Clust_postHmm/Results_hmm_new16Jan/RunMetaModel_gef/HMMs/CAMPS4_1/";
		String path = "F:/SC_Clust_postHmm/MetaModelsJune2016/HMMs/CAMPS4_1";
		final File folder = new File(path);
		// below function lists all the hmm files.. i.e. the SC cluster
		listFilesForFolder(folder);
		// no the below functions makes a list of all the clusters in the data structure of MModel... with id thresh members etc
		getstats();
		// this below function adds the members for each cluster and computes the total number of members
		getclusterSize();

	}


	private static void getclusterSize() {
		// TODO Auto-generated method stub
		Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");
		PreparedStatement p;
		PreparedStatement p2;
		ResultSet r = null;
		ResultSet r2 = null;
		try{
			p = CAMPS_CONNECTION.prepareStatement("select count(*) from clusters_mcl2 where cluster_id= ? and cluster_threshold = ?");
			p2 = CAMPS_CONNECTION.prepareStatement("select sequenceid from clusters_mcl2 where cluster_id= ? and cluster_threshold = ?");

			for (int i =0;i<=Model_list.size()-1;i++){
				int tempClus = Model_list.get(i).getClusterid();
				int tempthresh = Model_list.get(i).getThreshold();
				int tempsize = 0;
				
				p.setInt(1, tempClus);
				p.setInt(2, tempthresh);
				
				p2.setInt(1, tempClus);
				p2.setInt(2, tempthresh);

				r = p.executeQuery();
				while(r.next()){
					tempsize = r.getInt(1);
				}
				r.close();
				Model_list.get(i).setNumber_of_proteins(tempsize);
				
				r2 = p2.executeQuery();
				while(r2.next()){
					int prot = r2.getInt(1);
					if(!ProteinsInHmm.containsKey(prot)){
						ProteinsInHmm.put(prot, null);
					}
				}
				r2.close();
			}
			p.close();
			p2.close();
			CAMPS_CONNECTION.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		// now to get all the numbers of clusters of each size
		List<Integer> sizeList = new ArrayList<>();
		//get max
		Integer max =0;
		int NumberProteinsInSC = 0;
		for (int i =0;i<=Model_list.size()-1;i++){
			NumberProteinsInSC = NumberProteinsInSC + Model_list.get(i).getNumber_of_proteins();
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
		System.out.print("The exact Number of Proteins in HMMs: "+NumberProteinsInSC +"\n");

	}


	private static void getstats() {
		// TODO Auto-generated method stub
		for (int i =0;i<=files_results.size()-1;i++){
			System.out.print(files_results.get(i)+"\n");
			// cluster_10.0_9413.hmm
			String temp[] = files_results.get(i).split("_");
			// temp[0] = cluster
			// temp[1] = 10.0
			// temp[2] = 9413.hmm
			int n = temp[1].indexOf("."); 
			temp[1] = temp[1].substring(0, n);

			n = temp[2].indexOf(".hmm"); 
			temp[2] = temp[2].substring(0, n);

			MModel model = new MModel();
			model.setThreshold(Integer.parseInt(temp[1]));
			model.setClusterid(Integer.parseInt(temp[2]));

			int thresh = model.getThreshold();
			MModel.threshAll[thresh] = MModel.threshAll[thresh]+1;
			Model_list.add(model);
			System.out.print("Thresh "+temp[1]+"    Clusid "+temp[2]+"\n");
		}

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

	private static void listFilesForFolder(final File folder) {

		int count = 0; 
		files_results = new ArrayList<String>();

		for (final File fileEntry : folder.listFiles()) {
			String fileName = fileEntry.getName();
			if (fileEntry.isDirectory()) {
				listFilesForFolder(fileEntry);
			} else if (fileName.endsWith(".hmm")) {
				count++;
				files_results.add(fileEntry.getName().toString());
			}
		}
		System.out.println("\n total file " + count + " \n");
		//print test



	}

}
