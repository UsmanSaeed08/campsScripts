package stats;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import utils.DBAdaptor;

public class FH_cluster_abc_bac_Case {

	/**
	 * @param args
	 */
	// clusters with multiple pfam asignments
	// how many have conserved domain architecture -> % of positive and negative cases & examples for both
	// 

	private static Connection CAMPS_CONNECTION = DBAdaptor.getConnection("CAMPS4");
	private static ArrayList<String> fhClusIds = new ArrayList<String>(); // list of fhClusterids
	public static HashMap <String, ArrayList<Integer>> FHClusAndSeq = new HashMap<String, ArrayList<Integer>>();// key is fh clusid and value is arraylist of seqids
	public static HashMap <String, String> FHClusAndArchitecture = new HashMap<String, String>();// key is fh clusid and value is arraylist of seqids

	private static int Count_TotalFH_multipleDomains ;
	private static int Count_RearrangedDomains ;

	private static int Count_LostDomains ;	// counts the clusters which have at least 1 lost domain in any of the members
	private static int Count_ShuffeledDomains ;	// 


	private static BufferedWriter bwPositive; 


	FH_cluster_abc_bac_Case(){
		Count_TotalFH_multipleDomains = 0;
		Count_RearrangedDomains = 0;
		try{

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void GetFHClustersWithMultiplePfam(){
		try{
			PreparedStatement prepst;
			ResultSet r = null;
			prepst = CAMPS_CONNECTION.prepareStatement("select code,architecture from fh_clusters_architecture");
			r = prepst.executeQuery();
			while(r.next()){
				String code = r.getString(1);
				String architecture = r.getString(2);
				if(architecture.contains(" - ")){
					//PF03717 - PF00905 [Score: 0.4, Coverage: 99.67]
					int ix = architecture.indexOf("[");
					architecture = architecture.substring(0, ix-1);
					architecture = architecture.trim();
					Count_TotalFH_multipleDomains ++;
					fhClusIds.add(code);
					FHClusAndArchitecture.put(code, architecture);
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	// select * from fh_clusters_architecture

	private static void GetFHClustersANDIds(){
		try{
			// get all fh clusters and the members of each cluster
			PreparedStatement prepst;
			ResultSet r = null;
			prepst = CAMPS_CONNECTION.prepareStatement("select code,sequenceid from fh_clusters order by code");
			r = prepst.executeQuery();
			while(r.next()){
				String code = r.getString(1);
				int seqId = r.getInt(2);
				if(FHClusAndArchitecture.containsKey(code)){
					if (FHClusAndSeq.containsKey(code)){
						ArrayList<Integer> temp = FHClusAndSeq.get(code);
						temp.add(seqId);
						FHClusAndSeq.put(code, temp);
					}
					else{
						ArrayList<Integer> temp = new ArrayList<Integer>();
						temp.add(seqId);
						FHClusAndSeq.put(code, temp);
					}
				}
			}
			// Now we have the fh clusters and their member sequence ids
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void calculate(){
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter (new File("F:/FhClusters_ReportOnDomainRearragement")));
			bwPositive = new BufferedWriter(new FileWriter (new File("F:/FhClusters_ReportOnDomainRearragementPositiveCases")));
			//String l = code+"\t" + key +"\t"+ count + "\t" + members.size();
			bw.write("FH_clusterId\tDomainsInOrder\tNumberofTotalDomains\tNumberofRearrangements\tTotalNumberofProteinsInCluster");
			bw.newLine();
			bwPositive.write("FH_clusterId\tDomainsInOrder\tTotalNumberofProteinsInCluster");
			bwPositive.newLine();

			for(int i =0;i<=fhClusIds.size()-1;i++){
				String code = fhClusIds.get(i);
				ArrayList<Integer> members = FHClusAndSeq.get(code);

				HashMap <Integer, String> SeqToOrderedDomains = new HashMap<Integer, String>();// key is seqId and value is pfam domain in order
				for(int j =0;j<=members.size()-1;j++){
					// for each member here
					//select sequenceid,begin,end,name from da_cluster_assignments where method="Pfam"
					PreparedStatement prepst;
					ResultSet r = null;
					int sqid = members.get(j);

					//prepst = CAMPS_CONNECTION.prepareStatement("select name from da_cluster_assignments where method=\"Pfam\" and sequenceid= ? order by begin");
					prepst = CAMPS_CONNECTION.prepareStatement("select accession from domains_pfam where sequenceid= ? order by begin");
					prepst.setInt(1, sqid);
					r = prepst.executeQuery();
					// so we simply get the domains for each member
					// keep adding them a temporary hash
					// and then compare
					while(r.next()){					
						//int begin = r.getInt(2);
						//int end = r.getInt(3);
						String name = r.getString(1);
						if(name!=null ){
							name=name.trim();
							// ordered by begin you would automatically get the domain at start first.
							if (SeqToOrderedDomains.containsKey(sqid)){
								String domain = SeqToOrderedDomains.get(sqid);
								domain = domain + "-"+ name;
								SeqToOrderedDomains.put(sqid, domain);
							}
							else{
								String domain = name;
								//domain = domain + "|"+ name;
								SeqToOrderedDomains.put(sqid, domain);
							}
						}
					}
				}
				// so now have pf in exact order for all the member
				HashMap<String, Integer> DifferentDomainstoCount = compareDomainArchitecture(code,members,SeqToOrderedDomains);
				Iterator it = DifferentDomainstoCount.entrySet().iterator();
				bw.newLine();
				bw.write(code+":");
				bw.newLine();
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry)it.next();
					String key =(String) pair.getKey();
					int NumberOfDomains = countInstancess(key,"-");
					//int producedFh =(Integer) pair.getValue();
					int count = DifferentDomainstoCount.get(key);
					String l = code+"\t" + key +"\t"+NumberOfDomains+"\t"+ count + "\t" + members.size();
					bw.write(l);
					bw.newLine();
					it.remove(); // avoids a ConcurrentModificationException
				}			
			}
			bw.close();
			bwPositive.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param str Main String
	 * @param findStr SubString to find
	 * @return Number of times the subString occurs in the Main String
	 */
	private static Integer countInstancess(String str,String findStr){
		//String str = "helloslkhellodjladfjhello";
		//		String findStr = "|";

		if (str.contains(findStr)){
			String[] parts = str.split(findStr);
			System.out.print(parts[0]);
			int s = parts.length;
			return s;
		}
		return 1;
		/*
		int lastIndex = 0;
		int count = 0;
		if (str.contains(findStr)){
			while(lastIndex != -1){
				lastIndex = str.indexOf(findStr,lastIndex);
				if(lastIndex != -1){
					count ++;
					lastIndex += findStr.length();
				}
			}
			return count;
		}
		else{
			return 1;
		}*/


	}

	private static HashMap<String, Integer> compareDomainArchitecture(String fh_clusterId,ArrayList<Integer> members,
			HashMap<Integer, String> seqToOrderedDomains) {
		// TODO Auto-generated method stub
		// called for each cluster
		// 1. so get all the domain architectures in one array 
		// 2. compare
		// 3. get the sequence with largest number of domains in this cluster
		// 4. get the sequeence with smallest number of domains in this cluster

		ArrayList <String> domains = new ArrayList<String>();
		HashMap <String, Integer> domainArchitecturesCount = new HashMap<String, Integer>();

		int maxDomainSeq = 0;
		int maxdomain = 0;
		
		int minDomainSeq = 0;
		int mindomain = 110;
		
		int minDomainSeq2nd = 0;
		int mindomain2nd = 110;
		
		//int maxDomainNumber =0;
		boolean Set = false;

		for(int i =0;i<=members.size()-1;i++){
			int sqid= members.get(i);
			String domain = seqToOrderedDomains.get(sqid);
			// set max and min if i =0
			if(domain!=null){

				if (domainArchitecturesCount.containsKey(domain)){
					int temp = domainArchitecturesCount.get(domain);
					temp++;
					domainArchitecturesCount.put(domain, temp);
				}
				else{
					int temp = 1;
					domainArchitecturesCount.put(domain, temp);
					domains.add(domain);
				}
			}
		}

		for (int i =0;i<=members.size()-1;i++){
			// to get min and max
			int sqid= members.get(i);
			String domain = seqToOrderedDomains.get(sqid);

			if(domain != null){
				int CurrentdomainNumber = countInstancess(domain,"-");
				if (Set == false){
					maxDomainSeq = sqid;
					maxdomain = CurrentdomainNumber;

					minDomainSeq = sqid;
					mindomain = CurrentdomainNumber;
					Set = true;
				}
				
				if (i>0 && Set){
					if (CurrentdomainNumber > maxdomain){
						maxdomain = CurrentdomainNumber;
						maxDomainSeq = sqid;
					}
					if (CurrentdomainNumber < mindomain){
						mindomain = CurrentdomainNumber;
						minDomainSeq = sqid;
					}
				}
				if (i>0 && Set){
					if(CurrentdomainNumber > mindomain && CurrentdomainNumber < mindomain2nd){
						mindomain2nd = CurrentdomainNumber;
						minDomainSeq2nd = sqid;
					}
				}
			}

		}

		// compare
		//maxDomainNumber = countInstancess(seqToOrderedDomains.get(maxDomainSeq),"-");
		if (domainArchitecturesCount.size() > 1){
			// we have multiple domain arrangements
			Count_RearrangedDomains ++;
			// diffDomains/totalMembers*100
			float result = (float)domainArchitecturesCount.size()/(float)members.size();
			result = result*100;

			if (maxdomain > 2){
				CheckDomainLost(fh_clusterId,members,seqToOrderedDomains,minDomainSeq,minDomainSeq2nd,maxDomainSeq);
				CheckDomainShuffle(fh_clusterId,members,seqToOrderedDomains,minDomainSeq,minDomainSeq2nd,maxDomainSeq);
			}
			System.out.print(fh_clusterId+": "+"("+domainArchitecturesCount.size()+"/"+members.size()+")*100"+" = "+ result + "\n");
		}
		else{
			// to write the cases where domain architecture is same
			Iterator it = domainArchitecturesCount.entrySet().iterator();
			String key= "";
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry)it.next();
				key =(String) pair.getKey();
				it.remove(); // avoids a ConcurrentModificationException
			}		
			reportPositiveCase(fh_clusterId,members.size(),key);
		}
		return domainArchitecturesCount;


	}

	private static void CheckDomainShuffle(String fh_clusterId,
			ArrayList<Integer> members,
			HashMap<Integer, String> seqToOrderedDomains, int minDomainSeq,
			int minDomainSeq2nd, int maxDomainSeq) {
		// TODO Auto-generated method stub
		String minDomain = seqToOrderedDomains.get(minDomainSeq);
		String minDomain2nd = seqToOrderedDomains.get(minDomainSeq2nd);
		String maxDomain = seqToOrderedDomains.get(maxDomainSeq);

		if (minDomain.length()!=maxDomain.length()){
			// doing this below because in order to calculate the shufling, at least 2 domains should be minimum number of Domains
			int n = countInstancess(minDomain,"-");
			//int n2 = countInstancess(minDomain2nd,"-");
			String minDom = "";
			int mins = 0;
			if (n>1){
				minDom = minDomain;
				mins = minDomainSeq;
			}
			else{// means n is one and only one domain so get the second highest 
				minDom = minDomain2nd;
				mins = minDomainSeq2nd;
			}
			// for all the members... if in any one of them reshuffling occurs, count and exit loop return
			// how to count reshuffling
			// for all the members -> if the min Domain seq can not be found in any of the proteins, EXACTLY like that.. the cluster has shuffeled domain
			for (int i =0;i<=members.size()-1;i++){
				if(members.get(i)!= mins && members.get(i)!=minDomainSeq){
					String currentDom = seqToOrderedDomains.get(members.get(i));
					try{
						if(currentDom!=null){
							if (!currentDom.contains(minDom)){
								Count_ShuffeledDomains ++;
								break;
							}
						}
					}
					catch(Exception e){
						System.out.print("found");
						e.printStackTrace();
					}
				}
			}
			return;

		}

	}

	private static void CheckDomainLost(String fh_clusterId,
			ArrayList<Integer> members,
			HashMap<Integer, String> seqToOrderedDomains, int minDomainSeq,
			int minDomainSeq2nd, int maxDomainSeq) {
		// TODO Auto-generated method stub
		String minDomain = seqToOrderedDomains.get(minDomainSeq);
		String minDomain2nd = seqToOrderedDomains.get(minDomainSeq2nd);

		String maxDomain = seqToOrderedDomains.get(maxDomainSeq);

		if (minDomain.length()!=maxDomain.length()){
			int n = countInstancess(minDomain,"-");
			int n2 = countInstancess(maxDomain,"-");
			int lostDomains = n2-n;
			if(lostDomains>0)
				Count_LostDomains++; 

		}

	}

	private static void reportPositiveCase(String fh_clusterId, int size, String domains) {
		// TODO Auto-generated method stub
		try{
			bwPositive.write(fh_clusterId + "\t" +domains.trim()+"\t"+ size );
			bwPositive.newLine();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.print("Here we go\n");
		//GetFHClustersWithMultiplePfam();
		//GetFHClustersANDIds();
		//calculate();

		System.out.print("\nTotal Number of Clusters with multiple domains: "+ Count_TotalFH_multipleDomains +"\n");
		System.out.print("\nTotal Number of Clusters with multiple and re-arranged domains: "+ Count_RearrangedDomains +"\n");

		System.out.print("\nTotal Number of Clusters with lost domains: "+ Count_LostDomains +"\n");
		System.out.print("\nTotal Number of Clusters with shuffeled domains: "+ Count_ShuffeledDomains +"\n");

	}

}
