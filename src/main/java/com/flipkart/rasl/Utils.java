package com.flipkart.rasl;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.min;

public class Utils {

    public static Integer event_count =0;

    public static ArrayList<String> final_res = new ArrayList<String>();
    public static Integer processed_count=0;
    public static synchronized  void increaseEventCount(){
        event_count +=1;
    }

    public static synchronized void increaseProcessedCount(){
        processed_count+=1;
    }

    public static synchronized  void addResult(String res){
        final_res.add(res);
    }
    public static void displayCounts(){

        System.out.println("Displaying count");
        System.out.println(processed_count);
        System.out.println(event_count);
        //System.out.println(final_res.size());
    }
    public static JSONArray processQuery(DataSource dataSource, String query){
        ResultSet rsObj = null;
        Connection connObj = null;
        PreparedStatement pstmtObj = null;
        JSONArray jsonArray=null;
        try {
            // Performing Database Operation!
            System.out.println("\n=====Making A New Connection Object For Db Transaction=====\n");
            connObj = dataSource.getConnection();

            pstmtObj = connObj.prepareStatement(query);
            rsObj = pstmtObj.executeQuery();
            /*while (rsObj.next()) {
                System.out.println(rsObj);

                System.out.println("");
                System.out.println("Count: " + rsObj.getInt("COUNT"));

            }*/
            jsonArray = convertToJSON(rsObj);
            System.out.println("\n=====Releasing Connection Object To Pool=====\n");
        } catch(Exception sqlException) {
            sqlException.printStackTrace();
        } finally {
            try {
                // Closing ResultSet Object
                if(rsObj != null) {
                    rsObj.close();
                }
                // Closing PreparedStatement Object
                if(pstmtObj != null) {
                    pstmtObj.close();
                }
                // Closing Connection Object
                if(connObj != null) {
                    connObj.close();
                }
            } catch(Exception sqlException) {
                sqlException.printStackTrace();
            }
        }
        return jsonArray;
    }

    public static JSONArray convertToJSON(ResultSet resultSet)
            throws Exception {
        JSONArray jsonArray = new JSONArray();
        while (resultSet.next()) {
            int total_rows = resultSet.getMetaData().getColumnCount();
            //System.out.println("rows : " + Integer.toString(total_rows));
            JSONObject obj = new JSONObject();

            for (int i = 0; i < total_rows; i++) {
                obj.put(resultSet.getMetaData().getColumnLabel(i + 1)
                        .toLowerCase(), resultSet.getObject(i + 1));
            }
            jsonArray.put(obj);
            //System.out.println(obj.toString());

        }
        return jsonArray;
    }

    public static JSONObject constructMap(List<String> words, List<String> opening_headers) throws JSONException {


        JSONObject record = new JSONObject();
        System.out.println(opening_headers.size());
        System.out.println(words.size());
        int len = min(opening_headers.size(),words.size());
        for(int cnt=0;cnt<len;cnt++){
            record.put(opening_headers.get(cnt),words.get(cnt));
        }
        return record;

    }
}
