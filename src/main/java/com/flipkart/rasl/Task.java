package com.flipkart.rasl;

import com.google.common.base.Joiner;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Task implements Runnable {

    public Task() {

    }

    DataSource dataSource;
    Integer start;
    String filename;

    Integer offset_start;
    Integer end;
    Integer offset_end;
    Integer range_start;
    Integer range_end;
    Integer per_transaction;
    Integer count;
    Integer per_thread;
    String seller_id;
    String file_prefix;

    BufferedWriter writer = null;

    public Task(DataSource dataSource, Integer count, Integer range_start, Integer per_thread, Integer range_end, Integer per_transaction, String seller_id, String final_file) {
        this.range_start = range_start;
        this.range_end = range_end;
        this.count = count;
        this.seller_id = seller_id;
        this.per_thread = per_thread;
        this.dataSource = dataSource;
        this.per_transaction = per_transaction;
        this.filename = "files/" + seller_id + "/stock_ledger_" + seller_id + "_temp_" + Integer.toString(count) + ".csv";
        this.file_prefix = "stock_ledger_" + seller_id + "_temp_";
        this.start = range_start + per_thread * count;
        this.offset_start = range_start + count * per_thread;
        this.end = Math.min(range_start + (count + 1) * per_thread, range_end + 1);
        this.offset_end = Math.min(this.offset_start + per_transaction, this.end);
        System.out.println(" per transaction : " + Integer.toString(per_transaction));
        System.out.println(" range start, range end, count, offset_start, offset_end, start, end " + Integer.toString(range_start) + " "
                + Integer.toString(range_end) + " " + Integer.toString(count) + " "
                + Integer.toString(offset_start) + " " + Integer.toString(offset_end) + " " + Integer.toString(start) + " "
                + Integer.toString(end)
        );

        try {
            this.writer = new BufferedWriter(new FileWriter(filename));
        } catch (IOException e) {
            System.out.println("Got exception while writing");
        }

        System.out.println("***************Initialized new task " + Integer.toString(count));


    }


    public HashMap<String, String> getReferenceHash(String prefix, JSONObject references) {

        Iterator<?> keys = references.keys();
        HashMap<String, String> cur_hash = new HashMap<String, String>();
        try {
            while (keys.hasNext()) {

                String key = (String) keys.next();
                if (references.get(key) instanceof Integer) {
                    cur_hash.put(prefix + "_" + key, Integer.toString(references.getInt(key)));
                } else {
                    if (references.get(key) instanceof String) {
                        cur_hash.put(prefix + "_" + key, references.getString(key));
                    } else if (references.get(key) instanceof JSONObject) {
                        HashMap<String, String> new_hash = getReferenceHash(prefix + "_" + key, references.getJSONObject(key));
                        cur_hash.putAll(new_hash);
                    }
                }

            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return cur_hash;

    }


    public void processResult(JSONArray resultSet, BufferedWriter writer) {
        try {
            int total_cnt = resultSet.length();
            //System.out.println("total_cnt for thread: " + Integer.toString(total_cnt));
            for (int idx = 0; idx < total_cnt; idx++) {
                JSONObject event = resultSet.getJSONObject(idx);
                //System.out.println(event.getString("references"));
                String json = event.getString("references");


                JSONObject references = new JSONObject(json);//event.getJSONObject("references");
                //System.out.println(json);
                HashMap<String, String> reference_hash = getReferenceHash("", references);

                // System.out.println(reference_hash);
            /*if (mm_valuation.valuation_referenced.nil? == true)
            puts("ERROR FOR #{id}\t#{mm_valuation.id}")
            next
            end */


                JSONObject pca = null;
                if (event.has("price_component_attributes")) {
                    pca = new JSONObject(event.getString("price_component_attributes"));

                }


                List<String> items = new ArrayList<String>();
                //System.out.println("****************************" + event.toString());
                if (pca == null) {
                    double ratio = 1.0;
                    double unit_price_without_tax = (event.getDouble("unit_price_without_tax") * ratio);
                    double actual_unit_price_without_tax = (event.getDouble("actual_unit_price_without_tax") * ratio);
                    double unit_tax_amount = (event.getDouble("unit_tax_amount") * ratio);
                    double actual_unit_tax_amount = (event.getDouble("actual_unit_tax_amount") * ratio);

                    items.add(event.getString("event"));
                    items.add(Integer.toString(event.getInt("rasl_mm_event_id")));
                    items.add(event.getString("source_event"));
                    items.add(event.getString("source_event_date"));
                    items.add(event.getString("fsn"));
                    items.add(event.getString("sku"));
                    if (event.has("package_id")) {
                        items.add(event.getString("package_id"));
                    } else {
                        items.add("null");
                    }
                    items.add(event.getString("warehouse"));
                    items.add(Integer.toString(event.getInt("rasl_mm_event_quantity")));
                    items.add(event.getString("from_stock_type"));
                    items.add(event.getString("to_stock_type"));
                    items.add(event.getString("mm_valuation_quantity"));
                    items.add(reference_hash.get("_shipment_id"));
                    items.add(reference_hash.get("_order_id"));
                    items.add(reference_hash.get("_grn_id"));
                    items.add(event.getString("mm_valuation_transaction_date"));
                    items.add(event.getString("component_id"));
                    items.add(reference_hash.get("_irn_id"));
                    items.add(reference_hash.get("_irn_irn_date"));
                    items.add(Double.toString(unit_price_without_tax));
                    items.add(Double.toString(actual_unit_price_without_tax));
                    items.add(Double.toString(unit_tax_amount));
                    items.add(Double.toString(actual_unit_tax_amount));
                    items.add("0.0");
                    items.add("0.0");
                    items.add("0.0");
                    items.add("0.0");
                    items.add("0.0");
                    items.add("0.0");
                    items.add("0.0");
                    items.add("0.0");
                    items.add("0.0");
                    items.add("0.0");
                    items.add("0.0");
                    items.add("0.0");
                    items.add("nil");
                    items.add("0.0");

                    items.add(event.getString("price_type"));
                    items.add(Double.toString(event.getDouble("tax_rate")));
                    items.add(event.getString("tax_type"));
                    items.add(event.getString("currency"));
                    items.add(event.getString("price_component_transaction_date"));
                    items.add(reference_hash.get("_vs_id"));
                    items.add(reference_hash.get("_variance_id"));
                    items.add(reference_hash.get("_variance_reason"));
                    items.add(reference_hash.get("_grn_quantity"));

                    items.add(reference_hash.get("_consignment_id"));

                    items.add(reference_hash.get("_ro_id"));
                    items.add(event.getString("analytics_category"));
                    items.add(event.getString("seller_id"));
                    items.add("movement");
                    items.add("1");


                } else {

                    double ratio = 1.0;
                    if (pca.has("price_ratio_for_package")) {
                        ratio = pca.getDouble("price_ratio_for_package");
                    }
                    double unit_price_without_tax = (event.getDouble("unit_price_without_tax") * ratio);
                    double actual_unit_price_without_tax = (event.getDouble("actual_unit_price_without_tax") * ratio);
                    double unit_tax_amount = (event.getDouble("unit_tax_amount") * ratio);
                    double actual_unit_tax_amount = (event.getDouble("actual_unit_tax_amount") * ratio);


                    double default_value = 0.0 * ratio;
                    double igst_tax_rate = default_value, igst_unit_tax_amount = default_value, igst_actual_unit_tax_amount = default_value, sgst_tax_rate = default_value,
                            sgst_unit_tax_amount = default_value, sgst_actual_unit_tax_amount = default_value, cgst_tax_rate = default_value, cgst_unit_tax_amount = default_value,
                            cgst_actual_unit_tax_amount = default_value, cess_tax_rate = default_value, cess_unit_tax_amount = default_value, cess_actual_unit_tax_amount = default_value, mrp = default_value;

                    if (pca.has("igst_tax_rate")) {
                        igst_tax_rate = pca.getDouble("igst_tax_rate");
                    }
                    if (pca.has("igst_unit_tax_amount")) {
                        igst_unit_tax_amount = pca.getDouble("igst_unit_tax_amount");
                    }
                    if (pca.has("igst_actual_unit_tax_amount")) {
                        igst_actual_unit_tax_amount = pca.getDouble("igst_actual_unit_tax_amount");
                    }
                    if (pca.has("sgst_tax_rate")) {
                        sgst_tax_rate = pca.getDouble("sgst_tax_rate");
                    }
                    if (pca.has("sgst_unit_tax_amount")) {
                        sgst_unit_tax_amount = pca.getDouble("sgst_unit_tax_amount");
                    }
                    if (pca.has("sgst_actual_unit_tax_amount")) {
                        sgst_actual_unit_tax_amount = pca.getDouble("sgst_actual_unit_tax_amount");
                    }
                    if (pca.has("cgst_tax_rate")) {
                        cgst_tax_rate = pca.getDouble("cgst_tax_rate");
                    }
                    if (pca.has("cgst_unit_tax_amount")) {
                        cgst_unit_tax_amount = pca.getDouble("cgst_unit_tax_amount");
                    }
                    if (pca.has("cgst_actual_unit_tax_amount")) {
                        cgst_actual_unit_tax_amount = pca.getDouble("cgst_actual_unit_tax_amount");
                    }
                    if (pca.has("cess_tax_rate")) {
                        cess_tax_rate = pca.getDouble("cess_tax_rate");
                    }
                    if (pca.has("cess_unit_tax_amount")) {
                        cess_unit_tax_amount = pca.getDouble("cess_unit_tax_amount");
                    }
                    if (pca.has("cess_actual_unit_tax_amount")) {
                        cess_actual_unit_tax_amount = pca.getDouble("cess_actual_unit_tax_amount");
                    }

                    if (pca.has("mrp")) {
                        mrp = pca.getDouble("mrp");
                    }


                    items.add(event.getString("event"));
                    items.add(Integer.toString(event.getInt("rasl_mm_event_id")));
                    items.add(event.getString("source_event"));
                    items.add(event.getString("source_event_date"));
                    items.add(event.getString("fsn"));
                    items.add(event.getString("sku"));
                    if (event.has("package_id")) {
                        items.add(event.getString("package_id"));
                    } else {
                        items.add("null");
                    }
                    items.add(event.getString("warehouse"));
                    items.add(Integer.toString(event.getInt("rasl_mm_event_quantity")));
                    items.add(event.getString("from_stock_type"));
                    items.add(event.getString("to_stock_type"));
                    items.add(event.getString("mm_valuation_quantity"));
                    items.add(reference_hash.get("_shipment_id"));
                    items.add(reference_hash.get("_order_id"));
                    items.add(reference_hash.get("_grn_id"));
                    items.add(event.getString("mm_valuation_transaction_date"));
                    items.add(event.getString("component_id"));
                    items.add(reference_hash.get("_irn_id"));
                    items.add(reference_hash.get("_irn_irn_date"));
                    items.add(Double.toString(unit_price_without_tax));
                    items.add(Double.toString(actual_unit_price_without_tax));
                    items.add(Double.toString(unit_tax_amount));
                    items.add(Double.toString(actual_unit_tax_amount));

                    items.add(Double.toString(igst_tax_rate));
                    items.add(Double.toString(igst_unit_tax_amount));
                    items.add(Double.toString(igst_actual_unit_tax_amount));
                    items.add(Double.toString(sgst_tax_rate));
                    items.add(Double.toString(sgst_unit_tax_amount));
                    items.add(Double.toString(sgst_actual_unit_tax_amount));
                    items.add(Double.toString(cgst_tax_rate));
                    items.add(Double.toString(cgst_unit_tax_amount));
                    items.add(Double.toString(cgst_actual_unit_tax_amount));
                    items.add(Double.toString(cess_tax_rate));
                    items.add(Double.toString(cess_unit_tax_amount));
                    items.add(Double.toString(cess_actual_unit_tax_amount));

                    items.add(pca.getString("hsn"));
                    items.add(Double.toString(mrp));

                    items.add(event.getString("price_type"));
                    items.add(Double.toString(event.getDouble("tax_rate")));
                    items.add(event.getString("tax_type"));
                    items.add(event.getString("currency"));
                    items.add(event.getString("price_component_transaction_date"));
                    items.add(reference_hash.get("_vs_id"));
                    items.add(reference_hash.get("_variance_id"));
                    items.add(reference_hash.get("_variance_reason"));
                    items.add(reference_hash.get("_grn_quantity"));

                    items.add(reference_hash.get("_consignment_id"));

                    items.add(reference_hash.get("_ro_id"));
                    items.add(event.getString("analytics_category"));
                    items.add(event.getString("seller_id"));
                    items.add("movement");
                    items.add(Double.toString(ratio));


                }

                //Utils.increaseEventCount();
                String idList = items.toString();
                String result = idList.substring(1, idList.length() - 1).replace(", ", ",");
                writer.write(result + "\n");

                //String result = Joiner.on(",").join(items);

                //String result = StringUtils.join(items, ",");


                //Utils.addResult(result);
                //Utils.increaseProcessedCount();


            }
            //Utils.displayCounts();
        } catch (IOException e) {
            System.out.println("got error");
            e.printStackTrace();

        } catch(JSONException e){
            e.printStackTrace();
        }
    }


    public void run() {

        System.out.println("Processing tasks");
        Boolean flag = true;
        while (true) {
            //System.out.println(Integer.toString(this.offset_start) + " " +  Integer.toString(this.offset_end));
            if ((this.offset_start >= this.offset_end) || (this.offset_start >= this.end)) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            }


            /*System.out.println("select pc.currency, pc.id as price_component_id, pc.price_component_attributes, pc.unit_price_without_tax, pc.`actual_unit_price_without_tax`,pc.`unit_tax_amount`,pc.`actual_unit_tax_amount`,pc.`price_type`,pc.`component_id`,pc.`tax_rate`, pc.`tax_type`,pc.`transaction_date` as price_component_transaction_date," +
                    "rme.id as rasl_mm_event_id, rme.`references`, rme.`to_stock_type`, rme.`event`,rme.`source_event`,rme.`source_event_date`,rme.`fsn`, rme.`sku`, rme.`warehouse`, rme.`quantity` as rasl_mm_event_quantity,rme.`from_stock_type`, rme.`seller_id`, rme.`analytics_category`," +
                            "mv.id as mm_valuation_id, mv.`stock_type`, mv.id, mv.`valuation_ref_id`, mv.`package_id` as package_id , mv.`quantity`as mm_valuation_quantity, mv.`transaction_date` as mm_valuation_transaction_date, mv.is_invoice_adjusted" +
                            " from rasl_mm_events as rme inner  join rasl_events as re on rme.id=re.eventable_id inner  join rasl_event_mm_valuations as rem on re.id=rem.rasl_event_id inner join mm_valuations as mv on (rem.mm_valuation_id=mv.id and rme.to_stock_type=mv.stock_type) inner join mm_valuation_price_components as mvp on mv.valuation_ref_id = mvp.`mm_valuation_id` inner join price_components as pc on mvp.price_component_id=pc.id where ((rme.id>= " + Integer.toString(offset_start) + " and rme.id< " + Integer.toString(offset_end) + " and  rme.source_event != \"inventory_location_update\" and rme.source_event != \"return_cancelled\" and source_event != \"return_created\") and re.eventable_type = \"RaslMmEvent\" and ((mv.is_invoice_adjusted=1 and pc.price_type=\"invoice\") or (mv.is_invoice_adjusted=0 and pc.price_type != \"invoice\")));");


*/
            JSONArray results;
            if (seller_id == "FKI" || seller_id == "WSR") {
                results = Utils.processQuery(this.dataSource, " select pc.currency, pc.id as price_component_id, pc.price_component_attributes, pc.unit_price_without_tax, pc.`actual_unit_price_without_tax`,pc.`unit_tax_amount`,pc.`actual_unit_tax_amount`,pc.`price_type`,pc.`component_id`,pc.`tax_rate`, pc.`tax_type`,pc.`transaction_date` as price_component_transaction_date," +
                        "rme.id as rasl_mm_event_id, rme.`references`, rme.`to_stock_type`, rme.`event`,rme.`source_event`,rme.`source_event_date`,rme.`fsn`, rme.`sku`, rme.`warehouse`, rme.`quantity` as rasl_mm_event_quantity,rme.`from_stock_type`, rme.`seller_id`, rme.`analytics_category`," +
                        "mv.id as mm_valuation_id, mv.`stock_type`, mv.id, mv.`valuation_ref_id`, mv.`package_id` as package_id , mv.`quantity`as mm_valuation_quantity, mv.`transaction_date` as mm_valuation_transaction_date, mv.is_invoice_adjusted" +
                        " from rasl_mm_events as rme inner  join rasl_events as re on rme.id=re.eventable_id inner  join rasl_event_mm_valuations as rem on re.id=rem.rasl_event_id inner join mm_valuations as mv on (rem.mm_valuation_id=mv.id and rme.to_stock_type=mv.stock_type) inner join mm_valuation_price_components as mvp on mv.valuation_ref_id = mvp.`mm_valuation_id` inner join price_components as pc on mvp.price_component_id=pc.id where ((rme.id>= " + Integer.toString(offset_start) + " and rme.id< " + Integer.toString(offset_end) + "  and rme.source_event != \"inventory_location_update\" and rme.source_event != \"return_cancelled\" and rme.source_event != \"return_created\") and re.eventable_type = \"RaslMmEvent\" and rme.seller_id=\"" + this.seller_id + "\" and ((mv.is_invoice_adjusted=1 and pc.price_type=\"invoice\") or (mv.is_invoice_adjusted=0 and pc.price_type != \"invoice\")));");
            } else {
                results = Utils.processQuery(this.dataSource, " select pc.currency, pc.id as price_component_id, pc.price_component_attributes, pc.unit_price_without_tax, pc.`actual_unit_price_without_tax`,pc.`unit_tax_amount`,pc.`actual_unit_tax_amount`,pc.`price_type`,pc.`component_id`,pc.`tax_rate`, pc.`tax_type`,pc.`transaction_date` as price_component_transaction_date," +
                        "rme.id as rasl_mm_event_id, rme.`references`, rme.`to_stock_type`, rme.`event`,rme.`source_event`,rme.`source_event_date`,rme.`fsn`, rme.`sku`, rme.`warehouse`, rme.`quantity` as rasl_mm_event_quantity,rme.`from_stock_type`, rme.`seller_id`, rme.`analytics_category`," +
                        "mv.id as mm_valuation_id, mv.`stock_type`, mv.id, mv.`valuation_ref_id`, mv.`package_id` as package_id , mv.`quantity`as mm_valuation_quantity, mv.`transaction_date` as mm_valuation_transaction_date, mv.is_invoice_adjusted" +
                        " from rasl_mm_events as rme inner  join rasl_events as re on rme.id=re.eventable_id inner  join rasl_event_mm_valuations as rem on re.id=rem.rasl_event_id inner join mm_valuations as mv on (rem.mm_valuation_id=mv.id and rme.to_stock_type=mv.stock_type) inner join mm_valuation_price_components as mvp on mv.valuation_ref_id = mvp.`mm_valuation_id` inner join price_components as pc on mvp.price_component_id=pc.id where ((rme.id>= " + Integer.toString(offset_start) + " and rme.id< " + Integer.toString(offset_end) + " and  rme.source_event != \"inventory_location_update\" and rme.source_event != \"return_cancelled\" and rme.source_event != \"return_created\") and re.eventable_type = \"RaslMmEvent\" and ((mv.is_invoice_adjusted=1 and pc.price_type=\"invoice\") or (mv.is_invoice_adjusted=0 and pc.price_type != \"invoice\")));");

            }
            /*System.out.println(" select pc.currency, pc.id as price_component_id, pc.price_component_attributes, pc.unit_price_without_tax, pc.`actual_unit_price_without_tax`,pc.`unit_tax_amount`,pc.`actual_unit_tax_amount`,pc.`price_type`,pc.`component_id`,pc.`tax_rate`, pc.`tax_type`,pc.`transaction_date` as price_component_transaction_date," +
                    "rme.id as rasl_mm_event_id, rme.`references`, rme.`to_stock_type`, rme.`event`,rme.`source_event`,rme.`source_event_date`,rme.`fsn`, rme.`sku`, rme.`warehouse`, rme.`quantity` as rasl_mm_event_quantity,rme.`from_stock_type`, rme.`seller_id`, rme.`analytics_category`," +
                    "mv.id as mm_valuation_id, mv.`stock_type`, mv.id, mv.`valuation_ref_id`, mv.`package_id` as package_id , mv.`quantity`as mm_valuation_quantity, mv.`transaction_date` as mm_valuation_transaction_date, mv.is_invoice_adjusted" +
                    " from rasl_mm_events as rme inner  join rasl_events as re on rme.id=re.eventable_id inner  join rasl_event_mm_valuations as rem on re.id=rem.rasl_event_id inner join mm_valuations as mv on (rem.mm_valuation_id=mv.id and rme.to_stock_type=mv.stock_type) inner join mm_valuation_price_components as mvp on mv.valuation_ref_id = mvp.`mm_valuation_id` inner join price_components as pc on mvp.price_component_id=pc.id where ((rme.id>= " + Integer.toString(offset_start) + " and rme.id< " + Integer.toString(offset_end) + " and rme.source_event != \"inventory_location_update\" and rme.source_event != \"return_cancelled\" and rme.source_event != \"return_created\") and re.eventable_type = \"RaslMmEvent\" and ((mv.is_invoice_adjusted=1 and pc.price_type=\"invoice\") or (mv.is_invoice_adjusted=0 and pc.price_type != \"invoice\")));");
            try {
                System.out.println(results.getJSONObject(0).toString());
            } catch (JSONException e) {
                e.printStackTrace();
            }*/


                processResult(results, writer);
                System.out.println("got exception");
            /*System.out.println("select * from rasl_mm_events as rme inner  join rasl_events as re on rme.id=re.eventable_id inner  join rasl_event_mm_valuations as rem on re.id=rem.rasl_event_id inner join mm_valuations as mv on " +
                    "(rem.mm_valuation_id=mv.id and rme.from_stock_type=mv.stock_type) inner join mm_valuation_price_components as mvp on mv.id = mvp.`mm_valuation_id` inner join price_components as pc on mvp.price_component_id=pc.id where ((rme.id> " + Integer.toString(offset_start) + " and rme.id< " + Integer.toString(offset_end) + " ) and ((mv.is_invoice_adjusted=1 and pc.price_type=\"invoice\") or (mv.is_invoice_adjusted=0 and pc.price_type != \"invoice\")))"
            ); */
            /*try {
                System.out.println(results.getJSONObject(0));
            } catch (JSONException e) {
                e.printStackTrace();
            }*/
            this.offset_start = Math.min(this.offset_start + this.per_transaction, this.end);
            this.offset_end = Math.min(this.offset_end + this.per_transaction, this.end);
        }

    }
}
