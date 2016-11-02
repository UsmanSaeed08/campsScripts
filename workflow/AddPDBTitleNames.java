package workflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

import utils.DBAdaptor;

public class AddPDBTitleNames {

	/**
	 * @param args
	 * The class adds titles to structures tables in camps... 
	 * the functions are used after using the clustersandStructures_ForMCL0 and creating those structure tables
	 */
private static final String PDB_URL = "http://www.pdb.org/pdb/rest/describePDB?structureId=";
	
	public static void addTitleToMcLStructTable0(){
		try{
			HashMap<String,String> pdb2title = new HashMap<String,String>();
			
			Connection conn = DBAdaptor.getConnection("camps4");
			Statement stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			//
			//table 'clusters_mcl_structures0'
			//
			
			System.out.println("Updating table clusters_mcl_structures...");
			ResultSet rs3 = stm.executeQuery(
					"SELECT id,pdbid,title " +
					"FROM clusters_mcl_structures0 FOR UPDATE");
			int i = 0;
			while(rs3.next()) {
				
				String pdbid = rs3.getString("pdbid");
				String pdbidShort = pdbid.split("_")[0].trim();	//without chain
				String title = "";
				if(pdb2title.containsKey(pdbidShort)){
					title = pdb2title.get(pdbidShort);
				}
				else{
					title = getTitle(pdbidShort);
					pdb2title.put(pdbidShort, title);
				}
				//String title = getTitle(pdbidShort);
				
				rs3.updateString("title", title);
				rs3.updateRow();
				i++;
				System.out.println(title + " -cluster_mcl_struct0- " +i);
			}
			rs3.close();
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void run() {
		
		try {
			
			Connection conn = DBAdaptor.getConnection("camps4");
			
			Statement stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			
			//
			//table 'camps2pdb'
			//
			HashMap<String,String> pdb2title = new HashMap<String,String>();
			System.out.println("Updating table camps2pdb...");
			ResultSet rs1 = stm.executeQuery(
					"SELECT id,pdb_name,title " +
					"FROM camps2pdb FOR UPDATE");
			int i = 0 ;
			while(rs1.next()) {
				
				String pdbid = rs1.getString("pdb_name");
				String pdbidShort = pdbid.split("_")[0].trim();	//without chain
				String title = "";
				i++;
				if(pdb2title.containsKey(pdbidShort)){
					title = pdb2title.get(pdbidShort);
				}
				else{
					title = getTitle(pdbidShort);
					pdb2title.put(pdbidShort, title);
				}
				//String title = getTitle(pdbidShort);
				
				rs1.updateString("title", title);
				rs1.updateRow();
				System.out.println(title + " -camps2pdb- " +i);
			}
			rs1.close();
			
			
			//
			//table 'sequences_other_database'
			//
			/*
			HashMap<String,String> pdb2title = new HashMap<String,String>();
			System.out.println("Updating table sequences_other_database...");
			ResultSet rs2 = stm.executeQuery(
					"SELECT id,name,additional_information " +
					"FROM sequences_other_database WHERE db=\"pdbtm\" FOR UPDATE");
			while(rs2.next()) {
				
				String pdbid = rs2.getString("name");
				String pdbidShort = pdbid.split("_")[0].trim();	//without chain
				String title = "";
				if(pdb2title.containsKey(pdbidShort)){
					title = pdb2title.get(pdbidShort);
				}
				else{
					title = getTitle(pdbidShort);
					pdb2title.put(pdbidShort, title);
				}
				//String title = getTitle(pdbidShort);
				
				rs2.updateString("additional_information", title);
				rs2.updateRow();
				System.out.println(title);
			}
			rs2.close();
			*/
			
			//
			//table 'clusters_mcl_structures'
			//
			
			System.out.println("Updating table clusters_mcl_structures...");
			ResultSet rs3 = stm.executeQuery(
					"SELECT id,pdbid,title " +
					"FROM clusters_mcl_structures FOR UPDATE");
			i = 0;
			while(rs3.next()) {
				
				String pdbid = rs3.getString("pdbid");
				String pdbidShort = pdbid.split("_")[0].trim();	//without chain
				String title = "";
				if(pdb2title.containsKey(pdbidShort)){
					title = pdb2title.get(pdbidShort);
				}
				else{
					title = getTitle(pdbidShort);
					pdb2title.put(pdbidShort, title);
				}
				//String title = getTitle(pdbidShort);
				
				rs3.updateString("title", title);
				rs3.updateRow();
				i++;
				System.out.println(title + " -cluster_mcl_struct- " +i);
			}
			rs3.close();
			
			
			//
			//table 'fh_clusters_structures'
			//
			
			System.out.println("Updating table fh_clusters_structures...");
			ResultSet rs4 = stm.executeQuery(
					"SELECT id,pdbid,title " +
					"FROM fh_clusters_structures FOR UPDATE");
			i = 0;
			while(rs4.next()) {
				
				String pdbid = rs4.getString("pdbid");
				String pdbidShort = pdbid.split("_")[0].trim();	//without chain
				String title = "";
				if(pdb2title.containsKey(pdbidShort)){
					title = pdb2title.get(pdbidShort);
				}
				else{
					title = getTitle(pdbidShort);
					pdb2title.put(pdbidShort, title);
				}
				//String title = getTitle(pdbidShort);
				
				rs4.updateString("title", title);
				rs4.updateRow();
				i++;
				System.out.println(title + " -fh_clusters_strcutres- " +i);
			}
			rs4.close();
			
			stm.close();
			conn.close();
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	private static void addTitleToFHClustStruc0(){
		try{
			HashMap<String,String> pdb2title = new HashMap<String,String>();
			Connection conn = DBAdaptor.getConnection("camps4");
			Statement stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			System.out.println("Updating table fh_clusters_structures0 ...");
			ResultSet rs4 = stm.executeQuery(
					"SELECT id,pdbid,title " +
					"FROM fh_clusters_structures0 FOR UPDATE");
			int i = 0;
			while(rs4.next()) {
				
				String pdbid = rs4.getString("pdbid");
				String pdbidShort = pdbid.split("_")[0].trim();	//without chain
				String title = "";
				if(pdb2title.containsKey(pdbidShort)){
					title = pdb2title.get(pdbidShort);
				}
				else{
					title = getTitle(pdbidShort);
					pdb2title.put(pdbidShort, title);
				}
				//String title = getTitle(pdbidShort);
				
				rs4.updateString("title", title);
				rs4.updateRow();
				i++;
				System.out.println(pdbid+"\t"+ title + " -fh_clusters_strcutres- " +i);
			}
			rs4.close();
			
			stm.close();
			conn.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private static String getTitle(String pdbid) {
		
		String title = null;
		
		try {
			
			//
			//extract information from PDB website
			//
			File tmpFile = File.createTempFile(pdbid, "_.xml");
			PrintWriter tmpPw = new PrintWriter(new FileWriter(tmpFile));
			
			URL url = new URL(PDB_URL+pdbid);
	        InputStreamReader is  = new InputStreamReader(url.openStream());
	        BufferedReader in  = new BufferedReader(is);
	        String line;
	        
	        while((line = in.readLine()) != null) {
	        	
	        	tmpPw.println(line);
	        }		        
	        in.close();		
	        tmpPw.close();
	        	       
	        
	        //parse
	        SAXBuilder builder = new SAXBuilder();
			Document doc = builder.build(tmpFile);
			Element root = doc.getRootElement();	//PDBdescription	
			List<Element> entries = root.getChildren();	//PDB
			for(Element entry: entries) {	//
			    	
				title = entry.getAttributeValue("title");			  				
			}
	        		        
	        tmpFile.delete();
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return title;
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//run(); -- run adds titles to all fh and sc clusters  , but the old ones i.e.90% identity
		//addTitleToMcLStructTable0();	-- function was used to add titles to >30% identity structures mcl
		addTitleToFHClustStruc0(); // -- function used to add titles to >30 identity structures but in fh only
		//System.out.print(getTitle("3fwl"));	
	}

}
