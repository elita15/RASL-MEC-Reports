package com.flipkart.rasl;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.*;

public class CombinedReport implements Runnable{
    private Movements movements;
    private String filename;

    //private BufferedWriter report;
    private String opening_file;
    private String closing_file;
    private List<String>  stock_areas;
    private ArrayList<String> fsns = new ArrayList<String>();
    //private static final Map<String, String> headerMapping;
    Transformer transformer = null;

    /*
    static
    {
        headerMapping = new HashMap<String, String>();
        {
            headerMapping.put("event", "mm_evnt_event");
            headerMapping.put("rasl_mm_event_id", "dummy");
            headerMapping.put(    "source_event","dummy");
            headerMapping.put(    "source_event_date" , "source_event_date");
            headerMapping.put(    "fsn" , "fsn");
            headerMapping.put(    "sku" , "sku");
            headerMapping.put(     "package_id" , "dummy");
            headerMapping.put(   "warehouse" , "warehouse");
            headerMapping.put(   "event_quantity" , "dummy");
            headerMapping.put(    "from_stock_type" , "dummy");
            headerMapping.put(    "to_stock_type" , "stock_type");
            headerMapping.put("quantity_moved" , "unmoved_quantity");
            headerMapping.put("shipment_id", "shipment_ref");
            headerMapping.put(    "order_id" , "order_ref");
            headerMapping.put(    "grn_id" , "grn_ref");
            headerMapping.put(     "mm_valuations_transaction_date" , "mm_valuation_transaction_date");
            headerMapping.put(    "component_id" , "component_id");
            headerMapping.put(    "irn_id" , "irn_ref");
            headerMapping.put(    "irn_irn_date" , "dummy");
            headerMapping.put(    "unit_price_without_tax" , "unit_price_without_tax");
            headerMapping.put(    "actual_unit_price_without_tax" , "actual_unit_price_without_tax");
            headerMapping.put(    "unit_tax_amount" , "unit_tax_amount");
            headerMapping.put(   "actual_unit_tax_amount" , "actual_unit_tax_amount");
            headerMapping.put(    "igst_tax_rate" , "igst_tax_rate");
            headerMapping.put(    "igst_unit_tax_amount" , "igst_unit_tax_amount");
            headerMapping.put(    "igst_actual_unit_tax_amount" , "igst_actual_unit_tax_amount");
            headerMapping.put(   "sgst_tax_rate" , "sgst_tax_rate");
            headerMapping.put(   "sgst_unit_tax_amount" , "sgst_unit_tax_amount");
            headerMapping.put(    "sgst_actual_unit_tax_amount" , "sgst_actual_unit_tax_amount");
            headerMapping.put(    "cgst_tax_rate" , "cgst_tax_rate");
            headerMapping.put(    "cgst_unit_tax_amount" , "cgst_unit_tax_amount");
            headerMapping.put(    "cgst_actual_unit_tax_amount" , "cgst_actual_unit_tax_amount");
            headerMapping.put(    "cess_tax_rate" , "cess_tax_rate");
            headerMapping.put(    "cess_unit_tax_amount" , "cess_unit_tax_amount");
            headerMapping.put(    "cess_actual_unit_tax_amount" , "cess_actual_unit_tax_amount");
            headerMapping.put(    "hsn" , "hsn");
            headerMapping.put(    "mrp" , "mrp");
            headerMapping.put(    "price_type" , "price_type");
            headerMapping.put(    "vs_id" , "vs_ref");
            headerMapping.put(    "variance_id" , "variance_ref");
            headerMapping.put(    "variance_reason" , "dummy");
            headerMapping.put(    "grn_quantity" , "dummy");
            headerMapping.put(    "consignment_id" , "consignment_ref");
            headerMapping.put(    "ro_id" , "ro_ref");
            headerMapping.put(    "tax_type" , "tax_type");
            headerMapping.put(   "tax_rate" , "tax_rate");
            headerMapping.put(   "currency" , "currency");
            headerMapping.put(   "type" , "type");
            headerMapping.put(    "analytics_category" , "analytics_category");
            headerMapping.put("seller_id" , "seller_id");
        }
    }

    */

    List<String> headers = Arrays.asList("event", "rasl_mm_event_id", "source_event", "source_event_date", "fsn", "sku","package_id", "warehouse", "event_quantity", "from_stock_type", "to_stock_type", "quantity_moved", "shipment_id", "order_id", "grn_id", "mm_valuation_transaction_date", "current_component_id", "irn_id", "irn_irn_date", "unit_price_without_tax", "actual_unit_price_without_tax", "unit_tax_amount", "actual_unit_tax_amount", "igst_tax_rate","igst_unit_tax_amount","igst_actual_unit_tax_amount","sgst_tax_rate","sgst_unit_tax_amount","sgst_actual_unit_tax_amount","cgst_tax_rate","cgst_unit_tax_amount","cgst_actual_unit_tax_amount","cess_tax_rate","cess_unit_tax_amount","cess_actual_unit_tax_amount","hsn","mrp", "price_type", "tax_rate", "tax_type", "currency", "price_component_transaction_date","vs_id","variance_id", "variance_reason", "grn_quantity", "consignment_id", "ro_id", "analytics_category", "seller_id", "type");
    //List<String> final_headers = Arrays.asList("fsn","sku","package_id","from_area","to_area","warehouse","transaction_type","transaction_reference","purchase_price","tax_amount","stock_equation_quantity","price_type","tax_type","final_cost","movement_date","seller_id","analytics_category","component_id","vs_ref","location","price_component_transaction_date", "hsn", "mrp","is_mps");
    public CombinedReport() {

    }


    public CombinedReport(String seller_id, String ip, String username, String password, String url, String month, String year, Integer range_start, Integer range_end, String opening_file, String closing_file, String opening_date, String closing_date) throws IOException, JSONException {
        this.filename = "files"  + "/stock_ledger_" + seller_id + "_final_"+ month + "_" + year+".csv";


        this.movements = new Movements(seller_id,ip,username,password,url,month,year, range_start,range_end, filename);

        this.opening_file = opening_file;
        this.closing_file = closing_file;
        this.transformer = new Transformer(opening_date, closing_date);

        //this.report = new BufferedWriter(new FileWriter(this.filename));
        //this.opening_file = new BufferedReader(new FileReader(opening_file));
        //this.closing_file = new BufferedReader(new FileReader(closing_file));
        this.stock_areas = Arrays.asList("iwit_in_transit","cr_in_transit","fbf_seller_acceptance","out_for_refurbishment");





    }



    public Boolean addOpeningClosing(String type, String req_filename)  {

        BufferedReader req_file = null;
        BufferedWriter report = null;
        try {
            req_file = new BufferedReader(new FileReader(req_filename));


            String temp_headers = null;
            temp_headers = req_file.readLine();


            if(temp_headers==null){
                System.out.println("cannot add openings");
                return false;
            }
            temp_headers = temp_headers.trim();
            System.out.println("Headers");
            System.out.println(temp_headers);
            List<String> opening_headers = Arrays.asList(temp_headers.split(","));

            report = new BufferedWriter(new FileWriter(this.filename,true));

            //System.out.println(opening_headers);
            String line;
            while((line=req_file.readLine())!=null) {
                List<String> words = Arrays.asList(line.split("\\s*,\\s*"));

                System.out.println(words);
                System.out.println(opening_headers);
                JSONObject record = Utils.constructMap(words, opening_headers);

                System.out.println(record);
                int write = 0;
                if (!(stock_areas.contains(record.get("stock_type")))) {
                    write = 1;
                    record.put("dummy", "NULL");
                    record.put("type", type);
                }
                if (write == 1) {

                    System.out.println("writing");
                    ArrayList<String> row = new ArrayList<String>();

                    row.add(record.getString("event"));
                    row.add(record.getString("dummy"));
                    row.add(record.getString("source_event"));
                    row.add(record.getString("source_event_date"));
                    row.add(record.getString("fsn"));
                    row.add(record.getString("sku"));
                    row.add(record.getString("package_id"));
                    row.add(record.getString("warehouse"));
                    row.add(record.getString("event_quantity"));
                    row.add(record.getString("from_stock_type"));
                    row.add(record.getString("to_stock_type"));
                    row.add(record.getString("quantity_moved"));
                    row.add(record.getString("shipment_id"));
                    row.add(record.getString("order_id"));
                    row.add(record.getString("grn_id"));
                    row.add(record.getString("mm_valuations_transaction_date"));
                    row.add(record.getString("component_id"));
                    row.add(record.getString("irn_id"));
                    row.add(record.getString("irn_irn_date"));
                    row.add(record.getString("unit_price_without_tax"));
                    row.add(record.getString("actual_unit_price_without_tax"));
                    row.add(record.getString("unit_tax_amount"));
                    row.add(record.getString("actual_unit_tax_amount"));
                    System.out.println(row);
                    if (record.has("igst_tax_rate")) {
                        row.add(record.getString("igst_tax_rate"));
                    } else {
                        row.add("0.0");
                    }
                    if (record.has("igst_unit_tax_amount")){
                        row.add(record.getString("igst_unit_tax_amount"));
                    } else {
                        row.add("0.0");
                    }
                    if (record.has("igst_actual_unit_tax_amount")) {
                        row.add(record.getString("igst_actual_unit_tax_amount"));
                    } else {
                        row.add("0.0");
                    }
                    if (record.has("sgst_tax_rate")) {
                        row.add(record.getString("sgst_tax_rate"));
                    } else {
                        row.add("0.0");
                    }
                    if (record.has("sgst_unit_tax_amount")) {
                        row.add(record.getString("sgst_unit_tax_amount"));
                    } else {
                        row.add("0.0");
                    }
                    if (record.has("sgst_actual_unit_tax_amount")) {
                        row.add(record.getString("sgst_actual_unit_tax_amount"));
                    } else {
                        row.add("0.0");
                    }
                    if (record.has("cgst_tax_rate")) {
                        row.add(record.getString("cgst_tax_rate"));
                    } else {
                        row.add("0.0");
                    }
                    if (record.has("cgst_unit_tax_amount")) {
                        row.add(record.getString("cgst_unit_tax_amount"));
                    } else {
                        row.add("0.0");
                    }
                    if (record.has("cgst_actual_unit_tax_amount")) {
                        row.add(record.getString("cgst_actual_unit_tax_amount"));
                    } else {
                        row.add("0.0");
                    }
                    if (record.has("cess_tax_rate")) {
                        row.add(record.getString("cess_tax_rate"));
                    } else {
                        row.add("0.0");
                    }
                    if (record.has("cess_unit_tax_amount")) {
                        row.add(record.getString("cess_unit_tax_amount"));
                    } else {
                        row.add("0.0");
                    }

                    if (record.has("cess_actual_unit_tax_amount")) {
                        row.add(record.getString("cess_actual_unit_tax_amount"));
                    } else {
                        row.add("0.0");
                    }
                    row.add(record.getString("hsn"));
                    if (record.has("mrp")) {
                        row.add(record.getString("mrp"));
                    } else {
                        row.add("0.0");
                    }
                    row.add(record.getString("price_type"));
                    row.add(record.getString("tax_rate"));
                    row.add(record.getString("tax_type"));
                    row.add(record.getString("currency"));
                    row.add(record.getString("dummy"));
                    row.add(record.getString("vs_id"));
                    row.add(record.getString("variance_id"));
                    row.add(record.getString("variance_reason"));
                    row.add(record.getString("grn_quantity"));
                    row.add(record.getString("consignment_id"));
                    row.add(record.getString("ro_id"));
                    if(record.has("analytics_category")){
                        row.add(record.getString("analytics_category"));
                    }
                    else {
                        row.add("NULL");
                    }
                    row.add(record.getString("seller_id"));
                    row.add(record.getString("type"));

                    System.out.println(row.size());
                    System.out.println(row);
                    System.out.println(headers);
                    JSONObject row_hash = Utils.constructMap(row, headers);
                    ArrayList<String> final_row = this.transformer.transformRecord(row_hash);
                    String idList = final_row.toString();
                    String result = idList.substring(1, idList.length() - 1).replace(", ", ",");

                    System.out.println(result);

                    //String result = Joiner.on(",").join(items);

                    //String result = StringUtils.join(items, ",");
                    //try {

                        //Utils.displayCounts();
                        report.write(result + "\n");
                        System.out.println("wrote result");
                    //} catch (IOException e) {
                    //    report.close();
                    //    System.out.println("got error");
                    //    e.printStackTrace();

                    }
                }

                report.close();
                req_file.close();

                //break;
            }

         catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        catch (JSONException e){
            e.printStackTrace();
        }
        finally {
            try {
                if (report != null) {
                    report.close();
                }
                if (req_file != null) {
                    req_file.close();
                }
            }
            catch (IOException e){
                System.out.println("Got exception while closing report and opening/closing  file");
            }
        }
        return true;


    }


    public Boolean generateReport()  {

        System.out.println("Generating report");
        try {

            String final_headers = this.headers.toString();
            final_headers = final_headers.substring(1, final_headers.length() - 1).replace(", ", ",");

            BufferedWriter report = new BufferedWriter(new FileWriter(this.filename));

            //Utils.displayCounts();
            report.write(final_headers+"\n");
            report.close();
        } catch (IOException e) {

            System.out.println("got error while opening file: "+ this.filename);
            e.printStackTrace();

        }
        System.out.println("Wrote headers");

        Boolean added_opening = addOpeningClosing("opening",this.opening_file);
        if (!added_opening){
            System.out.println("Could not add opening");
            return false;
        }
        Boolean added_movements = null;
        try {
            added_movements = this.movements.getMovements(transformer);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (!added_movements){
            System.out.println("Could not add movements");
            return false;
        }


        Boolean added_closing = addOpeningClosing("closing",this.opening_file);
        if (!added_closing){
            System.out.println("Could not add closing");
            return false;
        }
        return true;



    }


    public void run() {
        try {
            generateReport();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
