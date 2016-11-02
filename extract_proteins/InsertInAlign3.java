package extract_proteins;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

//import mips.gsf.de.simapclient.client.SimapAccessWebService;
//import mips.gsf.de.simapclient.datatypes.HitSet;
//import mips.gsf.de.simapclient.datatypes.ResultParser;

import utils.DBAdaptor;

public class InsertInAlign3 {

	public void run_threads(){
		try{
			///scratch/usman/download/camps_seq_file.matrix
			String ff = "/scratch/usman/download/parts/camps_seq_file";
			InsertInAlign3_Thread[] threadclass = new InsertInAlign3_Thread[31];		
			// 	adjust threads to number of files
			for(int i =0; i<=30;i++){
				int x = i+1;
				String h = ff+x;
				threadclass[i] = new InsertInAlign3_Thread(h+".matrix",i);
				threadclass[i].start();
				System.out.print("\nThread Started"+i+" \n");
			}
			for(int i =0; i<=30; i++){			
				if (threadclass[i].isAlive()){
					threadclass[i].join();
					System.out.print("Thread joining: "+i+"\n");
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();


		}

	}

}