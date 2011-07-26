/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.radargun.tpcc;

import java.io.UnsupportedEncodingException;
import java.util.Random;

/**
 * 
 *
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 */
public final class TpccTools {
   
   public static final double WAREHOUSE_YTD = 300000.00;
   
   public static final int NB_MAX_DISTRICT = 10;
   
   public static final long NB_MAX_ITEM = 100000;
   
   public static final int NB_MAX_CUSTOMER = 3000;
   
   public static final int NB_MAX_ORDER = 3000;
   
   public static final String CHAINE_5_1 = "11111";
   
   public final static int MIN_C_LAST = 0;
   
   public final static int MAX_C_LAST = 999;
   
   public final static String[] C_LAST = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
   
   public final static int LIMIT_ORDER = 2101;
   
   public static final int NULL_NUMBER = -1;
   
   public final static int S_DATA_MINN = 26;
   
   public static int A_C_LAST = 255;
   
   public static int A_OL_I_ID = 8191;
   
   public static int A_C_ID = 1023;
   
   private final static int DEFAULT_RADIX = 10;
   
   private final static int DEFAULT_MINL = 65;
   
   private final static int DEFAULT_MAXL = 90;
   
   private final static int DEFAULT_MINN = 48;
   
   private final static int DEFAULT_MAXN = 57;
   
   private final static int S_DATA_MAXN = 50;
   
   public final static String ORIGINAL = "ORIGINAL";
   
   private final static int unicode[][] = { {65, 126}, {192, 259}};
   
   private static Random _randUniform = new Random(System.nanoTime());
   
   private static Random _randNonUniform=new Random(System.nanoTime());
   
   private static Random _randAlea=new Random(System.nanoTime());
   
   private TpccTools(){}
   

   private static String aleaChaine(int deb, int fin, int min, int max, int radix) {
      if (deb > fin) return null;
      String chaine = new String();
      int lch = fin;

      if (deb != fin) lch = aleaNumber(deb, fin);

      for (int i=0; i<lch; i++) {
         int random = _randAlea.nextInt(max-min+1)+min;
         char c = (char) (((byte) random) & 0xff);
         chaine += c;
      }
      return chaine;
   }


   public static String aleaChainel(int deb, int fin, int radix) {
      return aleaChaine(deb, fin, DEFAULT_MINL, DEFAULT_MAXL, radix);
   }

   public static String aleaChainel(int deb, int fin) {
      return aleaChainel(deb, fin, DEFAULT_RADIX);
   }


   public static String aleaChainec(int deb, int fin, int radix) {
      if (deb > fin) return null;
      String chaine = "";
      String str = null;

      int lch = fin;
      if (deb != fin) lch = aleaNumber(deb, fin);

      for (int i=0; i<lch; i++) {
         int ref = _randAlea.nextInt(2);
         int min = unicode[ref][0];
         int max = unicode[ref][1];
         int random = _randAlea.nextInt(max-min+1)+min;

         char c = (char) (((byte) random));
         chaine += c;
      }
      try {
         str = new String(chaine.toString().getBytes(), "UTF-8");
      } catch (UnsupportedEncodingException e) {
         System.out.println("----------- Error "+e.getMessage());
      }
      return str;
   }

   public static String aleaChainec(int deb, int fin) {
      return aleaChainec(deb, fin, DEFAULT_RADIX);
   }

   public static String sData() {
      String alea = aleaChainec(S_DATA_MINN, S_DATA_MAXN);
      if (aleaNumber(1, 10) == 1) {
         long number = randomNumber(0, alea.length()-8);
         alea = alea.substring(0, (int)number)+ORIGINAL+alea.substring((int)number+8, alea.length());
      }
      return alea;
   }


   public static String aleaChainen(int deb, int fin, int radix) {
      return aleaChaine(deb, fin, DEFAULT_MINN, DEFAULT_MAXN, radix);
   }

   public static String aleaChainen(int deb, int fin) {
      return aleaChainen(deb, fin, DEFAULT_RADIX);
   }


   public static int aleaNumber(int deb, int fin) {
      return _randAlea.nextInt(fin-deb+1)+deb;
   }


   public static long aleaNumber(long deb, long fin) {
      long random = _randAlea.nextLong()%(fin+1);
      while (random < deb) random += fin-deb;
      return random;
   }

   public static float aleaFloat(float deb, float fin, int virg) {
      if(deb > fin || virg < 1) return 0;
      long pow = (long)Math.pow(10, virg);
      long amin =  (long) (deb * pow);
      long amax =  (long) (fin * pow);
      long random = (long) (_randAlea.nextDouble() * (amax-amin) + amin);
      return (float) random/pow;
   }

   public static double aleaDouble(double deb, double fin, int virg) {
      if(deb >= fin || virg < 1) return 0.;
      long pow = (long)Math.pow(10, virg);
      long amin =  (long) (deb * pow);
      long amax =  (long) (fin * pow);
      long random = (long) (_randAlea.nextDouble() * (amax-amin) + amin);
      return (double) random/pow;
   }

   public static long randomNumber(long min, long max)
   {
      return (long)(_randUniform.nextDouble() * (max-min+1) + min);
   }

   public static double doubleRandomNumber(long min, long max){
      return _randUniform.nextDouble() * (max-min+1) + min;
   }

   public static long randomNumberForNonUniform(long min, long max){
      return (long)(_randNonUniform.nextDouble() * (max-min+1) + min);
   }

   public static long nonUniformRandom(long type, long x, long min, long max)
   {
      return (((randomNumberForNonUniform(0, x) | randomNumberForNonUniform(min, max)) + type) % (max-min+1)) + min;
   }




}