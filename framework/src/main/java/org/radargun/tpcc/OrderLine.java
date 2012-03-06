package org.radargun.tpcc;

import org.radargun.CacheWrapper;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: sebastiano
 * Date: 4/26/11
 * Time: 4:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class OrderLine implements Serializable {

   private long ol_o_id;
   private long ol_d_id;
   private long ol_w_id;
   private long ol_number;
   private long ol_i_id;
   private long ol_supply_w_id;
   private long ol_delivery_d;
   private long ol_quantity;
   private double ol_amount;
   private String ol_dist_info;

   public OrderLine() {
   }

   public OrderLine(long ol_o_id, long ol_d_id, long ol_w_id, long ol_number, long ol_i_id, long ol_supply_w_id, Date ol_delivery_d, long ol_quantity, double ol_amount, String ol_dist_info) {
      this.ol_o_id = ol_o_id;
      this.ol_d_id = ol_d_id;
      this.ol_w_id = ol_w_id;
      this.ol_number = ol_number;
      this.ol_i_id = ol_i_id;
      this.ol_supply_w_id = ol_supply_w_id;
      this.ol_delivery_d = (ol_delivery_d==null)?-1:ol_delivery_d.getTime();
      this.ol_quantity = ol_quantity;
      this.ol_amount = ol_amount;
      this.ol_dist_info = ol_dist_info;
   }



   public long getOl_o_id() {
      return ol_o_id;
   }

   public long getOl_d_id() {
      return ol_d_id;
   }

   public long getOl_w_id() {
      return ol_w_id;
   }

   public long getOl_number() {
      return ol_number;
   }

   public long getOl_i_id() {
      return ol_i_id;
   }

   public long getOl_supply_w_id() {
      return ol_supply_w_id;
   }

   public Date getOl_delivery_d() {
      return (ol_delivery_d==-1)?null:new Date(ol_delivery_d);
   }

   public long getOl_quantity() {
      return ol_quantity;
   }

   public double getOl_amount() {
      return ol_amount;
   }

   public String getOl_dist_info() {
      return ol_dist_info;
   }

   public void setOl_o_id(long ol_o_id) {
      this.ol_o_id = ol_o_id;
   }

   public void setOl_d_id(long ol_d_id) {
      this.ol_d_id = ol_d_id;
   }

   public void setOl_w_id(long ol_w_id) {
      this.ol_w_id = ol_w_id;
   }

   public void setOl_number(long ol_number) {
      this.ol_number = ol_number;
   }

   public void setOl_i_id(long ol_i_id) {
      this.ol_i_id = ol_i_id;
   }

   public void setOl_supply_w_id(long ol_supply_w_id) {
      this.ol_supply_w_id = ol_supply_w_id;
   }

   public void setOl_delivery_d(Date ol_delivery_d) {
      this.ol_delivery_d = (ol_delivery_d==null)?-1:ol_delivery_d.getTime();
   }

   public void setOl_quantity(long ol_quantity) {
      this.ol_quantity = ol_quantity;
   }

   public void setOl_amount(double ol_amount) {
      this.ol_amount = ol_amount;
   }

   public void setOl_dist_info(String ol_dist_info) {
      this.ol_dist_info = ol_dist_info;
   }

   private String getKey(){
      return "ORDERLINE_"+this.ol_w_id+"_"+this.ol_d_id+"_"+this.ol_o_id+"_"+this.ol_number;
   }

   public void store(CacheWrapper wrapper)throws Throwable{

      wrapper.put(null,this.getKey(), this);
   }

   public boolean load(CacheWrapper wrapper)throws Throwable{

      OrderLine loaded=(OrderLine)wrapper.get(null,this.getKey());

      if(loaded==null) return false;

      this.ol_i_id=loaded.ol_i_id;
      this.ol_supply_w_id=loaded.ol_supply_w_id;
      this.ol_delivery_d =loaded.ol_delivery_d;
      this.ol_quantity =loaded.ol_quantity;
      this.ol_amount  =loaded.ol_amount;
      this.ol_dist_info =loaded.ol_dist_info;



      return true;
   }
}
