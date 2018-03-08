package com.flipkart.rasl;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;


import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.json.JSONArray;

/**
 * Hello world!
 *
 */
public class App 
{


    public static void main( String[] args ) throws Exception {

        List<Integer> range_start = Arrays.asList(220000000,235419296,220000000,220000000);
        List<Integer> range_end =   Arrays.asList(220020000,242901560,220001334,220001334);
        List<String> seller_ids = Arrays.asList("FKI","WSR","TECHRETAIL","CONSULTINGRETAIL");


        String seller_id;
        for(int cnt=0;cnt<seller_ids.size();cnt++){
            seller_id = seller_ids.get(cnt);
            if(seller_id=="FKI" /*|| seller_id=="WSR"*/){


                String opening_file = "data/feb_fki_opening.csv";
                String closing_file = "data/feb_fki_closing.csv";
                String opening_date = "2018-01-01";
                String closing_date = "2018-01-31";
                //CombinedReport combinedReport = new CombinedReport(seller_id,"10.85.162.234","ra_mm_fki_ro","YHwRyw80","jdbc:mysql://127.0.0.1:3366/ra_materialmanager_fki","feb","2018", range_start.get(cnt),range_end.get(cnt),opening_file, closing_file);
                CombinedReport combinedReport = new CombinedReport(seller_id,"10.85.154.19","ra_mm_fki_ro","YHwRyw80","jdbc:mysql://127.0.0.1:3366/ra_materialmanager_fki","feb","2018", range_start.get(cnt),range_end.get(cnt),opening_file, closing_file,opening_date,closing_date);

                //CombinedReport combinedReport = new CombinedReport(seller_id,db_ips.get(cnt),usernames.get(cnt),passwords.get(cnt),"jdbc:mysql://" + db_ips.get(cnt)+ ":3306/" + db_name.get(cnt),"feb","2018", range_start.get(cnt),range_end.get(cnt),opening_file, closing_file, opening_date,closing_date);

                combinedReport.generateReport();
            }
            else {
                System.out.println("process movements for other sellers");
            }
        }
    }
}
