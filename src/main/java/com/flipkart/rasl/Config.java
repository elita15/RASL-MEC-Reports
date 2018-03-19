package com.flipkart.rasl;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public class Config {




        static private Config _instance = null;

        public static String tunnel_url;

        public static String range_path;
        private static HashMap<String,String> db_properties;
        static public String SELLERS = null;
        protected Config() {
            try {
                InputStream file = new FileInputStream(new File("stock_ledger.properties"));
                Properties props = new Properties();
                props.load(file);

                db_properties.put("FKI_IP",props.getProperty("FKI_IP"));
                db_properties.put("FKI_USERNAME", props.getProperty("FKI_USERNAME"));
                db_properties.put("FKI_PASSWORD",props.getProperty("FKI_PASSWORD"));
                db_properties.put("WSR_IP",props.getProperty("WSR_IP"));
                db_properties.put("WSR_USERNAME",props.getProperty("WSR_USERNAME"));
                db_properties.put("WSR_PASSWORD",props.getProperty("WSR_PASSWORD"));
                db_properties.put("TECHRETAIL_IP",props.getProperty("TECHRETAIL_IP"));
                db_properties.put("TECHRETAIL_USERNAME",props.getProperty("TECHRETAIL_USERNAME"));
                db_properties.put("TECHRETAIL_PASSWORD",props.getProperty("TECHRETAIL_PASSWORD"));
                db_properties.put("APL_IP",props.getProperty("APL_IP"));
                db_properties.put("APL_USERNAME",props.getProperty("APL_USERNAME"));
                db_properties.put("APL_PASSWORD",props.getProperty("APL_PASSWORD"));
                db_properties.put("PROC_IP",props.getProperty("TECHRETAIL_IP"));
                db_properties.put("PROC_USERNAME",props.getProperty("TECHRETAIL_USERNAME"));
                db_properties.put("PROC_PASSWORD",props.getProperty("PROC_PASSWORD"));
                db_properties.put("FKI_DATABASE",props.getProperty("FKI_DATABASE"));
                db_properties.put("WSR_DATABASE",props.getProperty("WSR_DATABASE"));
                db_properties.put("TECHRETAIL_DATABASE",props.getProperty("TECHRETAIL_DATABASE"));


                db_properties.put("APL_DATABASE",props.getProperty("APL_DATABASE"));
                db_properties.put("PROC_DATABASE",props.getProperty("PROC_DATABASE"));

                tunnel_url = props.getProperty("tunnel_url");
                range_path = props.getProperty("range_path");
                SELLERS = props.getProperty("SELLERS");

            } catch (Exception e) {
                System.out.println("error" + e);
            }
        }

        static public Config instance(){
            if (_instance == null) {
                _instance = new Config();
            }
            return _instance;
        }

        static public String getProperty(String seller_id, String property) {
            String key = seller_id.toUpperCase()+"_"+ property.toUpperCase();
            if(db_properties.containsKey(key)){
                return db_properties.get(key);
            }
            return null;
        }

}
