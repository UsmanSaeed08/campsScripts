package stats;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

import utils.DBAdaptor;

public class Kingdoms {

	/**
	 * @param args
	 * gets the super kingdom ids and the taxonomyid of proteins in the initial set. 
	 * a simple traversion over all the taxonomy ids to reveals the occurance of the 4 super kingdoms.  
	 */
	//private static HashMap <String, ArrayList<Integer>> SC = new HashMap<String,ArrayList<Integer>>();

	// key sequence id and value is taxonomy
	private static HashMap <Integer, Integer> taxids = new HashMap<Integer,Integer>();
	// key - taxid ...... val - superKingdom of this taxId
	private static HashMap <Integer, String> taxidKingdom = new HashMap<Integer,String>();

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	private static ArrayList<Integer> totalTaxas = new ArrayList<Integer>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("XXX");
		// get all taxids
		getTaxas();
		// get all super kingdoms
		getkingdoms();
		// count & print
		calc();

	}
	private static void calc(){
		int a = 0;	// archae 
		int b = 0;	// bacteria
		int v = 0;	// virus
		int e = 0;	// eukaryotes	
		int notInList = 0;
		for (int i =0;i<=totalTaxas.size()-1;i++){
			int txid = totalTaxas.get(i);
			if (taxidKingdom.containsKey(txid)){
				String kingdom = taxidKingdom.get(txid);
				if (kingdom.contains("Archaea")){
					a++;
				}
				else if (kingdom.contains("Bacteria")){
					b++;
				}
				else if (kingdom.contains("Viruses")){
					v++;
				}
				else if (kingdom.contains("Eukaryota")){
					e++;
				}
				else 
				{
					System.err.print("\n"+txid + " NOT Super kingdom");
					System.exit(0);
				}
			}
			else{
				// count the txids not in list
				notInList++;
			}
		}
		// Print the stats 
		System.out.print("\nNumber of archea " + a);
		System.out.print("\nNumber of bacteria " + b);
		System.out.print("\nNumber of virus " + v);
		System.out.print("\nNumber of eukaryotes " + e);
		System.out.print("\nNot IN list " + notInList);
	}
	private static void getTaxas() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("select distinct taxonomyid from proteins2");
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				int tx = rs.getInt("taxonomyid");
				if (!totalTaxas.contains(tx)){
					totalTaxas.add(tx);
				}
			}
			System.out.print("Distinct Taxids retreived " + totalTaxas.size()+"\n");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void getkingdoms() {
		// TODO Auto-generated method stub
		try{
			PreparedStatement pstm = CAMPS_CONNECTION.prepareStatement("" +
					"select taxonomyid,superkingdom from taxonomies_merged");
			ResultSet rs = pstm.executeQuery();
			while(rs.next()){
				Integer tx = rs.getInt("taxonomyid");
				String kg = rs.getString("superkingdom");
				if(tx != null && kg != null){
				taxidKingdom.put(tx,kg);
				}
			}
			System.out.print("TaxKingdoms retreived " +"\n");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
