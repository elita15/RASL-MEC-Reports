package com.flipkart.rasl;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.json.JSONArray;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Movements {
    private String seller_id;
    private String ip;
    private String username; // load from configuration file
    private String password;
    private String url;
    private String filename;
    private String month;
    private String year;
    private Integer range_start;
    private Integer range_end;
    private String temp_file_prefix;
    private Integer pool_size;

    public Movements() {


    }
    public Movements(String seller_id, String ip, String username, String password, String url, String month, String year, Integer range_start, Integer range_end, String filename){
        this.seller_id = seller_id;
        this.ip = ip;
        this.username = username;
        this.password = password;
        this.url = url;
        this.month = month;
        this.year = year;
        this.filename = filename;
        this.range_start = range_start;
        this.range_end = range_end;
        this.temp_file_prefix = "files/" + seller_id + "/stock_ledger_" + seller_id + "_temp_";


    }

    public Boolean getMovements(Transformer transformer) {

        this.pool_size=1;
        Integer thread_cnt=0;

        System.out.println(Runtime.getRuntime().availableProcessors());
        long startTime = System.currentTimeMillis();
        //Integer range_start= 220000000;
        //Integer range_end = 220020000;
        Integer per_transaction = 1500;
        DbConnection dbConnection = new DbConnection(url,username,password,ip);
        DataSource dataSource = null;
        try {
            dataSource = dbConnection.setUpPool();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        //JSONArray results = Utils.processQuery(dataSource,"select COUNT(*) as COUNT FROM rasl_mm_events where id>= " + Integer.toString(range_start) + " and id<= " + Integer.toString(range_end));
        Integer total_records = this.range_end - this.range_start +1 ;//results.getJSONObject(0).getInt("count");
        System.out.println(total_records);
        Integer per_thread = (total_records/pool_size);
        if (total_records%pool_size!=0){
            per_thread+=1;
        }
        System.out.println("per thread " + Integer.toString(total_records));
        //Task task = new Task(  dataSource, 0, range_start,  per_thread, range_end, per_transaction);
        ExecutorService taskExecutor = Executors.newFixedThreadPool(pool_size);
        for(Integer i=0;i<pool_size;i++) {
            System.out.println(" start " + Integer.toString(this.range_start));
            System.out.println(" end " + Integer.toString(range_end));
            Task task = new Task( dataSource, i, this.range_start,  per_thread, this.range_end, per_transaction, this.seller_id, filename);
            taskExecutor.execute(task);
            System.out.println(" final start " + Integer.toString(this.range_start));
            System.out.println(" final end " + Integer.toString(this.range_end));
        }

        taskExecutor.shutdown();
        try {
            taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }

        try {
            while (!taskExecutor.awaitTermination(24L, TimeUnit.HOURS)) {
                System.out.println("Not yet. Still waiting for termination");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(Long.toString( endTime-startTime));

        BufferedWriter writer=null;

        try {
            writer = new BufferedWriter(new FileWriter(this.filename,true));

            for (int cnt = 0; cnt < this.pool_size; cnt++){
                System.out.println(temp_file_prefix + Integer.toString(cnt) + ".csv");
                File  cur = new File(this.temp_file_prefix + Integer.toString(cnt) + ".csv");
                if (cur.isFile()){
                    FileInputStream fis;
                    try {
                        fis = new FileInputStream(cur);
                        BufferedReader in = new BufferedReader(new InputStreamReader(fis));

                        String aLine;
                        while ((aLine = in.readLine()) != null) {
                            writer.write(aLine);
                            writer.newLine();
                        }

                        in.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            writer.close();
        } catch (IOException e) {


            e.printStackTrace();
        }
        finally {
            try {
                if(writer!= null) {
                    writer.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }



        //Utils.displayCounts();
        //System.out.println(Utils.final_res.toString());
        dbConnection.disconnectSession();
        System.out.println("Total time taken " + Long.toString(endTime-startTime));


        return true;
    }


}




