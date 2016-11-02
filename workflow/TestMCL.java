package workflow;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import utils.DBAdaptor;

public class TestMCL {

	private static final Connection CAMPS_CONNECTION = DBAdaptor.getConnection("camps4");
	private static final DecimalFormat SIMILARITY_FORMAT = new DecimalFormat("0.000");

	public void writefile(){
		try {


			//to find the number of actual sequences and hits which have a -5 eval 
			//i.e. trying to find the exact number of nodes there would be...and also for the dimensions of the matrix.
			//here is something which might be slow but canbe effective

			//ArrayList<Integer[]> list =new ArrayList<Integer[]>();
			List<Integer> list = new ArrayList<Integer>();

			PreparedStatement pstm2 = CAMPS_CONNECTION.prepareStatement(
					//"select seqid_query,seqid_hit,evalue from alignments_initial limit 1000");
					"select seqid_query,seqid_hit,evalue from alignments_initial where id=?"); //since all the hits have >-5 evalue

			//File file = new File("F:/mcl_05.tab"); 	//mcl dataset with all proteins less than -05 evalue
			///scratch/usman/mcl

			int counter = 0; //counter for the number of actual rows excluding self hits


			File file = new File("/scratch/usman/mcl/mcl_05.tab");
			BufferedWriter output = new BufferedWriter(new FileWriter(file));
			output.write("(mclheader");
			output.newLine();
			output.write("mcltype matrix");
			output.newLine();
			output.write("dimensions 1000x1000");	//should be dynamic
			output.newLine();
			output.write(")");
			output.newLine();
			output.write("(mclmatrix");
			output.newLine();
			output.write("begin");
			output.newLine();

			for (int j=1; j<= 163409044; j++ ){
				pstm2.setInt(1, j);	//set the id you want to pick now

				ResultSet rs1 = pstm2.executeQuery();
				while (rs1.next()){
					int query = rs1.getInt("seqid_query");
					int hit= rs1.getInt("seqid_hit");
					double eval = rs1.getDouble("evalue");
					//System.out.print(query + ":" + hit+"\n");
					if (!(query==hit)){	//self hit
						counter++;

						if(!list.contains(query)){
							//queryCounter++;
							//add it to the list
							list.add(query);
						}
						output.write(query+" "+hit+" "+SIMILARITY_FORMAT.format(-Math.log10(eval)));

						output.newLine();
						if (j%1000 == 0){
							System.out.print(".");
							if (j%100000 == 0){
								System.out.print("\n");
							}
						}
					}

				}


			}

			output.write(")");
			output.newLine();
			output.flush();
			output.close();

			System.out.print("\n the total number of seq-hit less than -5 evalue and in file are: "+ counter);
			System.out.print("\n the total number of nodes less than -5 evalue and in file are: "+ list.size());

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
