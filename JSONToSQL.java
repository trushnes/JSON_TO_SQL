package com.sm.mongo;

import java.util.List;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;

import java.sql.* ;
import java.util.*;
import java.lang.reflect.Type;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

public class JavaMongoConnection {       
    
    public static void main(String[] args) {
	
	        String currentDir = new File("").getAbsolutePath();
	        String JSON_FILE  = currentDir + "/TVCBank.json";
            String DATABASE   = currentDir + "/TVCBank.db";
            
            //Class.forName("org.sqlite.JDBC");
	    
            try {
                // READ JSON
                Type type = new TypeToken<List<Map<String, String>>>(){}.getType();
                List<Map<String, String>> tvcbank = new Gson().fromJson(new FileReader(JSON_FILE), type);

                // CONNECT TO DB
                Connection conn = DriverManager.getConnection("jdbc:sqlite:"+DATABASE);
                conn.setAutoCommit(false);

                Statement stmt = conn.createStatement();
                stmt.executeUpdate("DELETE FROM tvcbank");

                String insertSQL = "INSERT INTO tvcbank (`AdhrID`, `Name`, `Gender`, `DOB`, `AccType`, `AccNo`, `Balance`, `InterestRate`)"
		                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement pstmt = conn.prepareStatement(insertSQL);

                // APPEND DATA
                for(int i=0; i < tvcbank.size(); i++) {
                   int n = 1;
                   Map<String, String> mcdata = tvcbank.get(i);

                   for (Map.Entry<String,String> entry : mcdata.entrySet()) {
                       pstmt.setString(n, entry.getValue());
                       n = n + 1;
                   }
                   // APPEND DATA    
                   pstmt.executeUpdate();
                }

                conn.commit();
                pstmt.close();
                conn.close();
				
                System.out.println("Successfully migrated JSON data to SQL database!");
	            
	    } catch (FileNotFoundException ffe) {
                System.out.println(ffe.getMessage());
	    } catch (SQLException sqe) {
                System.out.println(sqe.getMessage());
            }
    }
}
 
