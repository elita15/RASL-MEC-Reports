package com.flipkart.rasl;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;
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


        Config config = Config.instance();
        List<String> seller_ids = Arrays.asList((config.SELLERS).split(","));






                Calendar prev_month_start = Calendar.getInstance();
                Calendar prev_month_end = Calendar.getInstance();
                Calendar current = Calendar.getInstance();
                current.setTime(new Date());
                prev_month_start.setTime(new Date());
                prev_month_start.add(Calendar.MONTH,-1);
                prev_month_end.add(Calendar.MONTH,-1);
                prev_month_end.setTime(new Date());
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                prev_month_start.set(Calendar.DAY_OF_MONTH,1);
                prev_month_end.set(Calendar.DAY_OF_MONTH, prev_month_end.getActualMaximum(Calendar.DAY_OF_MONTH));
                String opening_date = format.format(prev_month_start.getTime());
                String closing_date = format.format(prev_month_end.getTime());
                String current_month_year = Integer.toString(current.get(Calendar.YEAR)+1);
                String current_month = new SimpleDateFormat("MMM").format(current.getTime());
                String prev_month_year = Integer.toString(prev_month_start.get(Calendar.YEAR));
                String prev_month = new SimpleDateFormat("MMM").format(prev_month_start.getTime());
                String opening_file = prev_month_year+ "_" + prev_month + "_closing.csv";
                String closing_file =  current_month_year + "_" + current_month + "_closing.csv";
                for(String seller_id: seller_ids) {
                    HashMap<String,Integer> range = Utils.getRange(seller_id);
                    if(range==null) {
                        continue;
                    }
                    //CombinedReport combinedReport = new CombinedReport(seller_id,"10.85.162.234","ra_mm_fki_ro","YHwRyw80","jdbc:mysql://127.0.0.1:3366/ra_materialmanager_fki","feb","2018", range_start.get(cnt),range_end.get(cnt),opening_file, closing_file);
                    CombinedReport combinedReport = new CombinedReport(seller_id, Config.getProperty(seller_id,"ip"),Config.getProperty(seller_id,"username"), Config.getProperty(seller_id,"password"), Config.tunnel_url+ Config.getProperty(seller_id,"database"), prev_month, prev_month_year, range.get("start"), range.get("end"),opening_file,closing_file, opening_date, closing_date);

                    //CombinedReport combinedReport = new CombinedReport(seller_id,db_ips.get(cnt),usernames.get(cnt),passwords.get(cnt),"jdbc:mysql://" + db_ips.get(cnt)+ ":3306/" + db_name.get(cnt),"feb","2018", range_start.get(cnt),range_end.get(cnt),opening_file, closing_file, opening_date,closing_date);

                    combinedReport.generateReport();
                }
        }

}
