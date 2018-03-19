package com.flipkart.rasl;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import sun.tools.tree.DoubleExpression;

import java.io.*;
import java.util.*;

import static com.sun.tools.doclint.Entity.and;

public class Transformer {
    public final String closing_date;
    public final List<String> summary_headers = Arrays.asList("fsn", "sku", "package_id", "warehouse", "transaction_type", "transaction_reference", "purchase_price", "tax_amount", "stock_equation_quantity", "price_type", "tax_type", "final_cost", "movement_date", "seller_id", "analytics_category", "component_id", "vs_ref", "location", "price_component_transaction_date", "hsn", "mrp", "is_mps");

    public final HashMap<String, String> events_to_type_mapping;

    public final HashMap<String, String> events_to_reference_mapping_prefix;

    public final HashMap<String, String> events_to_reference_mapping ;

    public final HashMap<String, JSONObject> apl_data;

    public final HashMap<String, JSONObject> proc_data;

    public HashMap<String,Long> opening_map;

    public HashMap<String, Long> closing_map;

    String opening_date;
    public List<String> events_to_ignore = Arrays.asList(
            "goods_sales_in_trans_to_cr_in_trans",
            "goods_cr_in_trans_to_sales_in_trans",
            "goods_pen_qc_irn_to_inv",
            "goods_pen_qc_irn_to_ret_area",
            "goods_pen_qc_irn_to_irn_sr",
            "goods_inv_to_pen_qc_irn",
            "goods_ret_area_to_disposal",
            "goods_ret_area_to_fraud_area",
            "goods_ret_area_to_inv",
            "goods_ret_area_to_prod_exch",
            "goods_ret_area_to_refb",
            "goods_ret_inv_trans",
            "goods_pen_sa_to_sr_compl");


    List<String> transit_areas = Arrays.asList("sales_in_transit", "cr_in_transit", "iwit_in_transit", "liquidation_receivable", "lost", "found", "pending_supplier_acceptance", "pending_inwarding", "fbf_seller_acceptance", "out_for_refurbishment", "invalidated", "warehouse_closure");
    List<String> qoh = Arrays.asList("inventory", "returns_area", "fraud_area", "pending_qc_irn", "disposal_area", "refurbishment_area", "irn_supplier_return", "product_exchange", "Seed/Opening Stock", "pending_supplier_return");


    public Transformer(String opening_date, String closing_date) throws JSONException {
        this.opening_map = new HashMap<String,Long>();

        this.closing_map = new HashMap<String, Long>();
        this.events_to_type_mapping = new HashMap<String, String>();
        this.events_to_reference_mapping = new HashMap<String, String>();
        this.events_to_reference_mapping_prefix = new HashMap<String, String>();
        this.opening_date = opening_date;
        this.closing_date = closing_date;
        events_to_type_mapping.put("goods_cr_in_trans_to_ret_area", "customer_return_inward");
        events_to_type_mapping.put("goods_inv_to_iwit_in_trans", "iwit_dispatch");
        events_to_type_mapping.put("goods_grn_invld", "grn_invalid");
        events_to_type_mapping.put("goods_inv_to_lq_rcvbl", "liquidation");
        events_to_type_mapping.put("goods_inv_to_pen_sa", "supplier_acceptance_dispatch");
        events_to_type_mapping.put("goods_inv_to_sales_in_trans", "sales");
        events_to_type_mapping.put("goods_irn_sr_to_pen_sa", "supplier_return_dispatch");
        events_to_type_mapping.put("goods_iwit_in_trans_to_inv", "iwit_inward");
        events_to_type_mapping.put("goods_iwit_in_trans_to_rasl_area", "iwit_inward");
        events_to_type_mapping.put("goods_iwit_in_trans_to_ret_area", "iwit_inward");
        events_to_type_mapping.put("goods_pen_inw_to_inv", "purchase");
        events_to_type_mapping.put("goods_pen_inw_to_pen_qc_irn", "purchase");
        events_to_type_mapping.put("goods_pen_qc_irn_to_inv", "");
        events_to_type_mapping.put("goods_pen_qc_irn_to_irn_sr", "");
        events_to_type_mapping.put("goods_pen_qc_irn_to_pen_sr", "");
        events_to_type_mapping.put("goods_pen_qc_irn_to_ret_area", "");
        events_to_type_mapping.put("goods_pen_sa_to_inv", "supplier_return");
        events_to_type_mapping.put("goods_pen_sa_to_ret_area", "supplier_return");
        events_to_type_mapping.put("goods_pen_sa_to_sr_compl", "");
        events_to_type_mapping.put("goods_pen_sr_to_pen_sa", "supplier_return_dispatch");
        events_to_type_mapping.put("goods_refb_area_to_lq_rcvbl", "liquidation");
        events_to_type_mapping.put("goods_ret_area_to_fraud_area", "");
        events_to_type_mapping.put("goods_ret_area_to_inv", "");
        events_to_type_mapping.put("goods_ret_area_to_iwit_in_trans", "iwit_dispatch");
        events_to_type_mapping.put("goods_ret_area_to_lq_rcvbl", "liquidation");
        events_to_type_mapping.put("goods_ret_area_to_pen_sa", "supplier_acceptance_dispatch");
        events_to_type_mapping.put("goods_ret_area_to_refb", "");
        events_to_type_mapping.put("goods_ret_inv_trans", "");
        events_to_type_mapping.put("goods_sales_in_trans_to_inv", "customer_return_inward");
        events_to_type_mapping.put("goods_sales_in_trans_to_ret_area", "customer_return_inward");
        events_to_type_mapping.put("goods_supplier_return_variance", "variance");
        events_to_type_mapping.put("goods_variance_found", "variance");
        events_to_type_mapping.put("goods_variance_lost", "variance");
        events_to_type_mapping.put("goods_inw_from_fki_to_inv", "purchase");
        events_to_type_mapping.put("goods_out_for_refb_to_ret_area", "refb_inward");
        events_to_type_mapping.put("goods_out_for_refb_to_inv", "refb_inward");
        events_to_type_mapping.put("goods_prod_exch_to_iwit_in_trans", "iwit_dispatch");
        events_to_type_mapping.put("goods_rasl_area_to_iwit_in_trans", "iwit_dispatch");
        events_to_type_mapping.put("goods_prod_exch_to_lq_rcvbl", "liquidation");
        events_to_type_mapping.put("goods_refb_to_out_for_refb", "refb_dispatch");
        events_to_type_mapping.put("goods_fraud_area_to_lq_rcvbl", "liquidation");



        events_to_reference_mapping.put("goods_cr_in_trans_to_ret_area", "grn_id");
        events_to_reference_mapping.put("goods_inv_to_iwit_in_trans", "consignment_id");
        events_to_reference_mapping.put("goods_grn_invld", "grn_id");
        events_to_reference_mapping.put("goods_inv_to_lq_rcvbl", "consignment_id");
        events_to_reference_mapping.put("goods_inv_to_pen_sa", "consignment_id");
        events_to_reference_mapping.put("goods_inv_to_sales_in_trans", "shipment_id");
        events_to_reference_mapping.put("goods_irn_sr_to_pen_sa", "consignment_id");
        events_to_reference_mapping.put("goods_iwit_in_trans_to_inv", "grn_id");
        events_to_reference_mapping.put("goods_iwit_in_trans_to_rasl_area", "grn_id");
        events_to_reference_mapping.put("goods_iwit_in_trans_to_ret_area", "grn_id");
        events_to_reference_mapping.put("goods_pen_inw_to_inv", "grn_id");
        events_to_reference_mapping.put("goods_pen_inw_to_pen_qc_irn", "grn_id");
        events_to_reference_mapping.put("goods_pen_qc_irn_to_inv", "");
        events_to_reference_mapping.put("goods_pen_qc_irn_to_irn_sr", "");
        events_to_reference_mapping.put("goods_pen_qc_irn_to_pen_sr", "");
        events_to_reference_mapping.put("goods_pen_qc_irn_to_ret_area", "");
        events_to_reference_mapping.put("goods_pen_sa_to_inv", "grn_id");
        events_to_reference_mapping.put("goods_pen_sa_to_ret_area", "grn_id");
        events_to_reference_mapping.put("goods_pen_sa_to_sr_compl", "");
        events_to_reference_mapping.put("goods_pen_sr_to_pen_sa", "consignment_id");
        events_to_reference_mapping.put("goods_refb_area_to_lq_rcvbl", "consignment_id");
        events_to_reference_mapping.put("goods_ret_area_to_fraud_area", "");
        events_to_reference_mapping.put("goods_ret_area_to_inv", "");
        events_to_reference_mapping.put("goods_ret_area_to_iwit_in_trans", "consignment_id");
        events_to_reference_mapping.put("goods_rasl_area_to_iwit_in_trans", "consignment_id");
        events_to_reference_mapping.put("goods_ret_area_to_lq_rcvbl", "consignment_id");
        events_to_reference_mapping.put("goods_ret_area_to_pen_sa", "consignment_id");
        events_to_reference_mapping.put("goods_ret_area_to_refb", "");
        events_to_reference_mapping.put("goods_ret_inv_trans", "");
        events_to_reference_mapping.put("goods_sales_in_trans_to_inv", "grn_id");
        events_to_reference_mapping.put("goods_sales_in_trans_to_ret_area", "grn_id");
        events_to_reference_mapping.put("goods_supplier_return_variance", "variance_id");
        events_to_reference_mapping.put("goods_variance_found", "variance_id");
        events_to_reference_mapping.put("goods_variance_lost", "variance_id");
        events_to_reference_mapping.put("goods_inw_from_fki_to_inv", "grn_id");
        events_to_reference_mapping.put("goods_out_for_refb_to_ret_area", "grn_id");
        events_to_reference_mapping.put("goods_out_for_refb_to_inv", "grn_id");
        events_to_reference_mapping.put("goods_prod_exch_to_iwit_in_trans", "consignment_id");
        events_to_reference_mapping.put("goods_prod_exch_to_lq_rcvbl", "consignment_id");
        events_to_reference_mapping.put("goods_refb_to_out_for_refb", "consignment_id");
        events_to_reference_mapping.put("goods_fraud_area_to_lq_rcvbl", "consignment_id");



        events_to_reference_mapping_prefix.put("goods_cr_in_trans_to_ret_area", "GRN");
        events_to_reference_mapping_prefix.put("goods_inv_to_iwit_in_trans", "CON");
        events_to_reference_mapping_prefix.put("goods_grn_invld", "GRN");
        events_to_reference_mapping_prefix.put("goods_inv_to_lq_rcvbl", "CON");
        events_to_reference_mapping_prefix.put("goods_inv_to_pen_sa", "CON");
        events_to_reference_mapping_prefix.put("goods_inv_to_sales_in_trans", "SH");
        events_to_reference_mapping_prefix.put("goods_irn_sr_to_pen_sa", "CON");
        events_to_reference_mapping_prefix.put("goods_iwit_in_trans_to_inv", "GRN");
        events_to_reference_mapping_prefix.put("goods_iwit_in_trans_to_rasl_area", "GRN");
        events_to_reference_mapping_prefix.put("goods_iwit_in_trans_to_ret_area", "GRN");
        events_to_reference_mapping_prefix.put("goods_pen_inw_to_inv", "GRN");
        events_to_reference_mapping_prefix.put("goods_pen_inw_to_pen_qc_irn", "GRN");
        events_to_reference_mapping_prefix.put("goods_pen_qc_irn_to_inv", "");
        events_to_reference_mapping_prefix.put("goods_pen_qc_irn_to_irn_sr", "");
        events_to_reference_mapping_prefix.put("goods_pen_qc_irn_to_pen_sr", "");
        events_to_reference_mapping_prefix.put("goods_pen_qc_irn_to_ret_area", "");
        events_to_reference_mapping_prefix.put("goods_pen_sa_to_inv", "GRN");
        events_to_reference_mapping_prefix.put("goods_pen_sa_to_ret_area", "GRN");
        events_to_reference_mapping_prefix.put("goods_pen_sa_to_sr_compl", "");
        events_to_reference_mapping_prefix.put("goods_pen_sr_to_pen_sa", "CON");
        events_to_reference_mapping_prefix.put("goods_refb_area_to_lq_rcvbl", "CON");
        events_to_reference_mapping_prefix.put("goods_ret_area_to_fraud_area", "");
        events_to_reference_mapping_prefix.put("goods_ret_area_to_inv", "");
        events_to_reference_mapping_prefix.put("goods_ret_area_to_iwit_in_trans", "CON");
        events_to_reference_mapping_prefix.put("goods_ret_area_to_lq_rcvbl", "CON");
        events_to_reference_mapping_prefix.put("goods_ret_area_to_pen_sa", "CON");
        events_to_reference_mapping_prefix.put("goods_ret_area_to_refb", "");
        events_to_reference_mapping_prefix.put("goods_ret_inv_trans", "");
        events_to_reference_mapping_prefix.put("goods_sales_in_trans_to_inv", "GRN");
        events_to_reference_mapping_prefix.put("goods_sales_in_trans_to_ret_area", "GRN");
        events_to_reference_mapping_prefix.put("goods_supplier_return_variance", "VAR");
        events_to_reference_mapping_prefix.put("goods_variance_found", "VAR");
        events_to_reference_mapping_prefix.put("goods_variance_lost", "VAR");
        events_to_reference_mapping_prefix.put("goods_inw_from_fki_to_inv", "GRN");
        events_to_reference_mapping_prefix.put("goods_out_for_refb_to_ret_area", "GRN");
        events_to_reference_mapping_prefix.put("goods_out_for_refb_to_inv", "GRN");
        events_to_reference_mapping_prefix.put("goods_prod_exch_to_iwit_in_trans", "CON");
        events_to_reference_mapping_prefix.put("goods_rasl_area_to_iwit_in_trans", "CON");
        events_to_reference_mapping_prefix.put("goods_prod_exch_to_lq_rcvbl", "CON");
        events_to_reference_mapping_prefix.put("goods_refb_to_out_for_refb", "CON");
        events_to_reference_mapping_prefix.put("goods_fraud_area_to_lq_rcvbl", "CON");


        List<String> negative_quantity_events = Arrays.asList(
                "goods_inv_to_iwit_in_trans",
                "goods_grn_invld",
                "goods_inv_to_lq_rcvbl",
                "goods_inv_to_pen_sa",
                "goods_inv_to_sales_in_trans",
                "goods_irn_sr_to_pen_sa",
                "goods_pen_sr_to_pen_sa",
                "goods_refb_area_to_lq_rcvbl",
                "goods_ret_area_to_iwit_in_trans",
                "goods_ret_area_to_lq_rcvbl",
                "goods_ret_area_to_pen_sa",
                "goods_supplier_return_variance",
                "goods_variance_lost"
        );

        String apl_filename="data/apl_file.csv";
        String proc_filename = "data/proc_file.csv";
        this.apl_data = getAPLData(apl_filename,"apl");
        this.proc_data = getProcData(proc_filename,"proc");


    }

    public HashMap<String, JSONObject> getAPLData(String req_file, String type) throws JSONException {
        HashMap<String,JSONObject> data_map = new HashMap<String, JSONObject>();

        DbConnection dbConnection = null;
        try {
            dbConnection = new DbConnection("jdbc:mysql://127.0.0.1:3366/" + Config.getProperty("apl","database"),Config.getProperty("apl","username"), Config.getProperty("apl","password"), Config.getProperty("apl","password"));

            DataSource dataSource = null;
            dataSource = dbConnection.setUpPool();
            JSONArray results = Utils.processQuery(dataSource, "'select po.internal_id as component_id,vs.external_id as vs_ref,po.origin_warehouse_id as location from purchase_orders po join vendor_sites vs on vs.id = po.vendor_site_id where status = \"completed\"");
            int total_cnt = results.length();
            //System.out.println("total_cnt for thread: " + Integer.toString(total_cnt));
            for (int idx = 0; idx < total_cnt; idx++) {
                JSONObject record = results.getJSONObject(idx);
                data_map.put(record.getString("component_id"),record);

            }
            dbConnection.disconnectSession();
            return data_map;

        }

        catch (IOException e){
            e.printStackTrace();
            System.out.println("could not get apl data");
            return null;
        }
        catch(Exception e) {
            e.getStackTrace();
        }
        return null;

    }


    public HashMap<String, JSONObject> getProcData(String req_file, String type) throws JSONException {

        HashMap<String,JSONObject> data_map = new HashMap<String, JSONObject>();

        Config config = Config.instance();

        DbConnection dbConnection = null;
        try {
            dbConnection = new DbConnection("jdbc:mysql://127.0.0.1:3366/" + Config.getProperty("proc","database"),Config.getProperty("proc","username"), Config.getProperty("proc","password"), Config.getProperty("proc","password"));
            DataSource dataSource = null;
            dataSource = dbConnection.setUpPool();
            JSONArray results = Utils.processQuery(dataSource, "'select po.internal_id as component_id,vs.external_id as vs_ref,po.origin_warehouse_id as location from purchase_orders po join vendor_sites vs on vs.id = po.vendor_site_id where status = \"completed\"");
            int total_cnt = results.length();
            //System.out.println("total_cnt for thread: " + Integer.toString(total_cnt));
            for (int idx = 0; idx < total_cnt; idx++) {
                JSONObject record = results.getJSONObject(idx);
                data_map.put(record.getString("component_id"),record);

            }
            dbConnection.disconnectSession();

        }

        catch (IOException e){
            e.printStackTrace();
            System.out.println("could not get apl data");
            return null;
        }
        catch(Exception e) {
            e.getStackTrace();
        }
        return null;



    }


    private Boolean to_be_ignored_or_not(JSONObject row_hash) throws JSONException {
        String from_stock_type = row_hash.getString("from_stock_type");
        String to_stock_type = row_hash.getString("to_stock_type");
        if (transit_areas.contains(from_stock_type) && transit_areas.contains(to_stock_type)) {
            return true;
        }
        if (qoh.contains(from_stock_type) && qoh.contains(to_stock_type)) {
            return true;
        }
        return false;
    }

    private Integer quantity_sign(JSONObject row_hash) throws JSONException {


        String from_stock_type =row_hash.getString("from_stock_type");
        String to_stock_type  =row_hash.getString("to_stock_type");
        if (qoh.contains(from_stock_type) && transit_areas.contains(to_stock_type)){
            return -1;
        }
        if(qoh.contains(to_stock_type)&& transit_areas.contains(from_stock_type)){
            return 1;
        }

        return 0;
    }

    private Double get_final_cost(JSONObject row_hash) throws JSONException {
        if(row_hash.getString("tax_type") == "CST - Purchase Category (Cost)" &&
                (row_hash.getString("price_type")=="invoice" || row_hash.getString("price_type")=="po")) {
            return row_hash.getInt("quantity_moved") * (row_hash.getDouble("unit_price_without_tax") + row_hash.getDouble("unit_tax_amount"));
        }
        return row_hash.getInt("quantity_moved") * (row_hash.getDouble("unit_price_without_tax"));
    }

    private String get_vs_ref(String component_id) throws JSONException {
        if(apl_data.containsKey(component_id)) {
            return apl_data.get(component_id).getString("vs_ref");
        }
        if(proc_data.containsKey(component_id)) {
            return proc_data.get(component_id).getString("vs_ref");
        }

        return null;
    }

    private String  get_location(String component_id) throws JSONException {
        if(apl_data.containsKey(component_id)) {
            return apl_data.get(component_id).getString("location");
        }
        if(proc_data.containsKey(component_id)) {
            return proc_data.get(component_id).getString("location");
        }
        return null;
    }


    public ArrayList<String> transformRecord(JSONObject row_hash) throws IOException, JSONException {

        Long quantity=0L;

        Boolean ignored = false;
        if (row_hash.get("type")=="movement") {
            ignored = to_be_ignored_or_not(row_hash);
        }

        ArrayList<String> row = new ArrayList<String>();
        if ((row_hash.getString("type") == "closing" || row_hash.getString("type") == "opening") && row_hash.getString("to_stock_type") != "invalidated") {
            row.add(row_hash.getString("fsn"));
            row.add(row_hash.getString("sku"));
            row.add(row_hash.getString("package_id"));
            row.add((row_hash.getString("from_stock_type")));
            row.add((row_hash.getString("to_stock_type")));
            row.add(row_hash.getString("warehouse"));
            row.add(row_hash.getString("type"));
            row.add(row_hash.getString("to_stock_type"));
            row.add(row_hash.getString("unit_price_without_tax"));
            row.add(row_hash.getString("unit_tax_amount"));
            row.add(row_hash.getString("quantity_moved"));
            row.add(row_hash.getString("price_type"));
            row.add(row_hash.getString("tax_type"));
            row.add(Double.toString(get_final_cost(row_hash)));
            if (row_hash.getString("type") == "opening") {
                row.add(this.opening_date);
            } else {
                row.add(this.closing_date);
            }
            row.add(row_hash.getString("seller_id"));
            row.add(row_hash.getString("analytics_category"));
            row.add(row_hash.getString("current_component_id"));
            row.add(get_vs_ref(row_hash.getString("current_component_id")));
            row.add(get_location(row_hash.getString("current_component_id")));
            row.add("NULL");
            row.add(row_hash.getString("hsn"));
            row.add(row_hash.getString("mrp"));
            row.add("false");
            System.out.println(row.size());

            quantity = 0L;
            String key = row_hash.getString("fsn") + "_"+ row_hash.getString("sku") + "_"+ row_hash.getString("warehouse");

            if(row_hash.getString("type")=="opening"){
               if(opening_map.containsKey(key)) {
                    quantity += opening_map.get(key);
                }
                quantity += row_hash.getLong("quantity_moved")*1L;
                opening_map.put(key,quantity);
            }
            else {
                if(closing_map.containsKey(key)) {
                    quantity += closing_map.get(key);
                }
                quantity += row_hash.getLong("quantity_moved")*1L;
                closing_map.put(key,quantity);
            }
            return row;
        }

        Double tax_amount=0.0;
        if  (!ignored && row_hash.getString("type") == "movement") {
            if (row_hash.getString("tax_type") == "GST") {
                tax_amount = row_hash.getDouble("igst_unit_tax_amount") + row_hash.getDouble("sgst_unit_tax_amount") + row_hash.getDouble("cgst_unit_tax_amount") + row_hash.getDouble("cess_unit_tax_amount");
            }
            else {
                tax_amount = row_hash.getDouble("unit_tax_amount");
            }
            Boolean is_mps = false;
            if (row_hash.getString("package_id") != "nil") {
                is_mps = true;
            }
            row.add(row_hash.getString("fsn"));
            row.add(row_hash.getString("sku"));
            row.add(row_hash.getString("package_id"));
            row.add((row_hash.getString("from_stock_type")));
            row.add((row_hash.getString("to_stock_type")));
            row.add(row_hash.getString("warehouse"));
            row.add(events_to_type_mapping.get(row_hash.getString("event")));
            if(row_hash.getString("source_event") == "variance" ) {
                row.add("Variance: " + row_hash.getString("variance_reason"));
                String variance_id = "";
                if (row_hash.has("variance_id")){
                    variance_id = row_hash.getString("variance_id");
                }
                row.add("VAR"+ variance_id);

            } else {
                row.add(events_to_type_mapping.get(row_hash.getString("event")));
                row.add(events_to_reference_mapping_prefix.get(row_hash.getString("event")) + row_hash.get(events_to_reference_mapping.get(row_hash.getString("event"))));

            }
            row.add(row_hash.getString("unit_price_without_tax"));
            row.add(Double.toString(tax_amount));
            row.add(Integer.toString(quantity_sign(row_hash) * row_hash.getInt("quantity_moved")));
            row.add(row_hash.getString("price_type"));
            row.add(row_hash.getString("tax_type"));
            row.add(Double.toString(get_final_cost(row_hash)));
            row.add(row_hash.getString("source_event_date"));
            row.add(row_hash.getString("seller_id"));
            row.add(row_hash.getString("analytics_category"));
            row.add(row_hash.getString("current_component_id"));
            row.add(get_vs_ref(row_hash.getString("current_component_id")));
            row.add(get_location(row_hash.getString("current_component_id")));
            row.add(row_hash.getString("price_component_transaction_date"));
            row.add(row_hash.getString("hsn"));
            row.add(row_hash.getString("mrp"));
            row.add(Boolean.toString(is_mps));
            quantity = 0L;
            String key = row_hash.getString("fsn") + "_"+ row_hash.getString("sku") + "_"+ row_hash.getString("warehouse");
            if(opening_map.containsKey(key)) {
                quantity += opening_map.get(key);
            }
            quantity += row_hash.getLong("quantity_moved")*quantity_sign(row_hash)*1L;
            opening_map.put(key,quantity);
            return row;

        }


        return null;


    }

    public Boolean validateSummary() {
        HashSet<String> error_keys = new HashSet<String>();
        try {
            BufferedWriter validation_errors = new BufferedWriter(new FileWriter("files/error_report.csv"));
            for(String key: this.opening_map.keySet())
            {
                Long opening_cnt = opening_map.get(key);
                Long closing_cnt = closing_map.get(key);
                if (opening_cnt!=closing_cnt && !error_keys.contains(key)){
                    error_keys.add(key);
                    validation_errors.write(key + ","+ Long.toString(opening_cnt) + ","+ Long.toString(closing_cnt)+"\n");
                }

            }

            for(String key: this.closing_map.keySet())
            {
                Long opening_cnt = opening_map.get(key);
                Long closing_cnt = closing_map.get(key);
                if (opening_cnt!=closing_cnt && !error_keys.contains(key)){
                    error_keys.add(key);
                    validation_errors.write(key + ","+ Long.toString(opening_cnt) + ","+ Long.toString(closing_cnt)+"\n");
                }

            }

            validation_errors.close();


        }
        catch(IOException e){

            System.out.println("Could not validate summary");
            return false;

        }
        return true;
    }



}
