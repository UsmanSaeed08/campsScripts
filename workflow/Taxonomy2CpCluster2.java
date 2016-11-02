/*
 * Taxonomy2CpCluster
 * 
 * Version 2.0
 * 
 * 2011-02-08
 * 
 * 
 * Differences to version 1.0:
 * - now covers also MD- and FH-cluster, not only SC-cluster
 * - use tables 'proteins_merged' and 'taxonomies_merged' as references
 *   instead of 'proteins' and 'taxonomies'
 * 
 */

package workflow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Set;

import utils.DBAdaptor;

public class Taxonomy2CpCluster2 {

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");

	public ArrayList<Integer> taxonomyIDs;
	public Hashtable<Integer,String> taxonomyid2taxonomy;
	public Hashtable<Integer,String> taxonomyid2superkingdom;


	public void run() {

		try {
			//updateTabletaxonomy();
			//System.exit(0);

			getTaxonomies();

			PreparedStatement pstm1 = CAMPS_CONNECTION.prepareStatement(
					"SELECT cluster_id,cluster_threshold " +
							"FROM clusters_mcl " +
					"WHERE sequenceid=?");

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					"SELECT code " +
							"FROM cp_clusters " +
					"WHERE cluster_id=? AND cluster_threshold=? AND type=\"sc_cluster\"");

			PreparedStatement pstm3 = CAMPS_CONNECTION.prepareStatement(
					"SELECT code " +
							"FROM cp_clusters " +
					"WHERE cluster_id=? AND cluster_threshold=? AND type=\"md_cluster\"");

			PreparedStatement pstm4 = CAMPS_CONNECTION.prepareStatement(
					"SELECT code FROM fh_clusters WHERE sequenceid=?");

			PreparedStatement pstm5 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequences FROM cp_clusters t1, clusters_mcl_nr_info t2 " +
					"WHERE t1.cluster_id=t2.cluster_id AND t1.cluster_threshold=t2.cluster_threshold AND t1.code=?");

			PreparedStatement pstm6 = CAMPS_CONNECTION.prepareStatement(
					"SELECT sequences FROM fh_clusters_info WHERE code=?");


			PreparedStatement pstm_insert = CAMPS_CONNECTION.prepareStatement(
					"INSERT INTO taxonomy2cpCluster2 " +
							"(taxonomyid, taxonomy, superkingdom, cluster_code, cluster_size, taxonomic_instances, type) " +
							"VALUES " +
					"(?,?,?,?,?,?,?)");

			System.out.print("Starting processing "+ this.taxonomyIDs.size()+"\n");
			int c = 0;
			for(Integer itax: this.taxonomyIDs) {
				c++;
				Hashtable<String,Integer> scCluster2instances = new Hashtable<String,Integer>();
				Hashtable<String,Integer> mdCluster2instances = new Hashtable<String,Integer>();
				Hashtable<String,Integer> fhCluster2instances = new Hashtable<String,Integer>();


				int taxonomyID = itax.intValue();
				String taxonomy = this.taxonomyid2taxonomy.get(itax); 
				String superkingdom = this.taxonomyid2superkingdom.get(itax);

				BitSet members = getTaxonomyMembers(taxonomyID);
				System.out.print(c+" taxas done\n");
				for(int sequenceid = members.nextSetBit(0); sequenceid>=0; sequenceid = members.nextSetBit(sequenceid+1)) {

					pstm1.setInt(1, sequenceid);
					ResultSet rs1 = pstm1.executeQuery();
					while(rs1.next()) {

						int clusterID = rs1.getInt("cluster_id");
						float clusterThreshold = rs1.getFloat("cluster_threshold");

						//
						//check if this cluster is a SC-cluster
						//
						String scCode = null;
						pstm2.setInt(1, clusterID);
						pstm2.setFloat(2, clusterThreshold);
						ResultSet rs2 = pstm2.executeQuery();
						while(rs2.next()) {
							scCode = rs2.getString("code");
						}
						rs2.close();


						if(scCode != null) {

							int count = 0;
							if(scCluster2instances.containsKey(scCode)) {
								count = scCluster2instances.get(scCode).intValue();
							}
							count = count+1;
							scCluster2instances.put(scCode, Integer.valueOf(count));
						}


						//
						//check if this cluster is a MD-cluster
						//
						String mdCode = null;
						pstm3.setInt(1, clusterID);
						pstm3.setFloat(2, clusterThreshold);
						ResultSet rs3 = pstm3.executeQuery();
						while(rs3.next()) {
							mdCode = rs3.getString("code");
						}
						rs3.close();


						if(mdCode != null) {

							int count = 0;
							if(mdCluster2instances.containsKey(mdCode)) {
								count = mdCluster2instances.get(mdCode).intValue();
							}
							count = count+1;
							mdCluster2instances.put(mdCode, Integer.valueOf(count));
						}						
					}
					rs1.close();


					//
					//check if sequence is in FH-cluster
					//
					String fhCode = null;
					pstm4.setInt(1, sequenceid);
					ResultSet rs4 = pstm4.executeQuery();
					while(rs4.next()) {
						fhCode = rs4.getString("code");
					}
					rs4.close();

					if(fhCode != null) {

						int count = 0;
						if(fhCluster2instances.containsKey(fhCode)) {
							count = fhCluster2instances.get(fhCode).intValue();
						}
						count = count+1;
						fhCluster2instances.put(fhCode, Integer.valueOf(count));
					}
				}

				Enumeration<String> scClusters = scCluster2instances.keys();
				while(scClusters.hasMoreElements()) {

					String scCluster = scClusters.nextElement();
					int instances = scCluster2instances.get(scCluster);

					int size = 0;

					pstm5.setString(1, scCluster);
					ResultSet rs5 = pstm5.executeQuery();
					while(rs5.next()) {
						size = rs5.getInt("sequences");
					}
					rs5.close();

					pstm_insert.setInt(1, taxonomyID);
					pstm_insert.setString(2, taxonomy);
					pstm_insert.setString(3, superkingdom);
					pstm_insert.setString(4, scCluster);
					pstm_insert.setInt(5, size);
					pstm_insert.setInt(6, instances);
					pstm_insert.setString(7, "sc_cluster");

					pstm_insert.executeUpdate();
				}

				Enumeration<String> mdClusters = mdCluster2instances.keys();
				while(mdClusters.hasMoreElements()) {

					String mdCluster = mdClusters.nextElement();
					int instances = mdCluster2instances.get(mdCluster);

					int size = 0;

					pstm5.setString(1, mdCluster);
					ResultSet rs5 = pstm5.executeQuery();
					while(rs5.next()) {
						size = rs5.getInt("sequences");
					}
					rs5.close();

					pstm_insert.setInt(1, taxonomyID);
					pstm_insert.setString(2, taxonomy);
					pstm_insert.setString(3, superkingdom);
					pstm_insert.setString(4, mdCluster);
					pstm_insert.setInt(5, size);
					pstm_insert.setInt(6, instances);
					pstm_insert.setString(7, "md_cluster");

					pstm_insert.executeUpdate();
				}

				Enumeration<String> fhClusters = fhCluster2instances.keys();
				while(fhClusters.hasMoreElements()) {

					String fhCluster = fhClusters.nextElement();
					int instances = fhCluster2instances.get(fhCluster);

					int size = 0;

					pstm6.setString(1, fhCluster);
					ResultSet rs6 = pstm6.executeQuery();
					while(rs6.next()) {
						size = rs6.getInt("sequences");
					}
					rs6.close();

					pstm_insert.setInt(1, taxonomyID);
					pstm_insert.setString(2, taxonomy);
					pstm_insert.setString(3, superkingdom);
					pstm_insert.setString(4, fhCluster);
					pstm_insert.setInt(5, size);
					pstm_insert.setInt(6, instances);
					pstm_insert.setString(7, "fh_cluster");

					pstm_insert.executeUpdate();
				}
			}

			pstm1.close();
			pstm2.close();
			pstm3.close();
			pstm4.close();
			pstm5.close();
			pstm6.close();

			pstm_insert.close();

		}catch(Exception e) {
			e.printStackTrace();
		}finally {			
			if (CAMPS_CONNECTION != null) {
				try {
					CAMPS_CONNECTION.close();					
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
		}		
	}
	private static void updateTabletaxonomy(){
		try{
			Hashtable<Integer,String> idtoTax = new Hashtable<Integer,String>();
			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT taxonomyid, species from taxonomies2");
			while(rs.next()) {
				Integer taxonomyID = rs.getInt("taxonomyid");
				String taxonomy = rs.getString("species");	//either genome name or species name
				if(taxonomyID!= null){
					if(!idtoTax.containsKey(taxonomyID)){
						if( taxonomy!= null){
							idtoTax.put(taxonomyID, taxonomy);
						}
						else
							idtoTax.put(taxonomyID, " ");

					}
				}
			}
			rs.close();
			stm.close();
			System.out.print("Size: "+ idtoTax.size() + "\n");
			PreparedStatement ps = CAMPS_CONNECTION.prepareStatement(
					"UPDATE taxonomies2 SET taxonomy = ? WHERE taxonomyid = ?");
			Set<Integer> keys = idtoTax.keySet();
			for(Integer key: keys){
				String value = idtoTax.get(key);
				// set the preparedstatement parameters
				ps.setString(1,value);
				ps.setInt(2,key);
				// call executeUpdate to execute our sql update statement
				ps.executeUpdate();

			}
			ps.close();
			System.out.print("Update Complete: "+ idtoTax.size() + "\n");

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private void getTaxonomies() {

		try {

			this.taxonomyIDs = new ArrayList<Integer>();
			this.taxonomyid2taxonomy = new Hashtable<Integer,String>();
			this.taxonomyid2superkingdom = new Hashtable<Integer,String>();

			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT taxonomyid, taxonomy, superkingdom FROM taxonomies2");
			while(rs.next()) {

				Integer taxonomyID = rs.getInt("taxonomyid");
				String taxonomy = rs.getString("taxonomy");	//either genome name or species name
				String superkingdom = rs.getString("superkingdom");

				if (taxonomyID !=null){
					this.taxonomyIDs.add(Integer.valueOf(taxonomyID));
					if(taxonomy==null){
						this.taxonomyid2taxonomy.put(Integer.valueOf(taxonomyID), " ");
					}
					else{
						this.taxonomyid2taxonomy.put(Integer.valueOf(taxonomyID), taxonomy);
					}
					if(superkingdom==null){
						this.taxonomyid2superkingdom.put(Integer.valueOf(taxonomyID), " ");
					}
					else{
						this.taxonomyid2superkingdom.put(Integer.valueOf(taxonomyID), superkingdom);
					}

				}
			}
			rs.close();

			stm.close();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	private BitSet getTaxonomyMembers(int id) {

		BitSet members = null;

		try {

			members = new BitSet();
			Statement stm = CAMPS_CONNECTION.createStatement();
			ResultSet rs = stm.executeQuery("SELECT sequenceid FROM proteins2 WHERE taxonomyid="+id);
			while(rs.next()) {

				int sequenceID = rs.getInt("sequenceid");

				members.set(sequenceID);
			}
			rs.close();

			stm.close();

		}catch(Exception e) {
			e.printStackTrace();
		}

		return members;
	}

}
