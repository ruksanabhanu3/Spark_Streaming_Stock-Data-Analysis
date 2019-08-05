package com.ruksana.sparkstreaming.EquityDataAnalysis;

import java.io.PrintWriter;
import java.io.Serializable;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import com.esotericsoftware.minlog.Log;
import scala.Tuple2;

/*
 * This Class contains the required computations for Analyses 4 to calculate the Maximum Trading Volume
 * and decide on whichh trade to purchase among the stocks in a 5 min sliding window.
 */

public class TradingVolume implements Serializable {
	private static final long serialVersionUID = 1L;

	public void calcTradingVolume(JavaDStream<EquityData> streams, String outputPath) {
		Log.info("***Calculating Maximum Trading Volume Of Dstreams ***");
		
		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,tradingVolume> 
		 * ("stocksymbolname", 5000.0)
		 */
		JavaPairDStream<String, Double> StockVolume = streams
				.mapToPair(new PairFunction<EquityData, String, Double>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Double> call(EquityData EquityData) throws Exception {
						return new Tuple2<String, Double>(EquityData.getStockNameSymbol(), EquityData.getVolume());
					}
				});

		
		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,Sum of tradingVolume> 
		 * ("stocksymbolname", 15000.0)
		 */
		JavaPairDStream<String, Double> sumOfStockVolume = StockVolume
				.reduceByKey((x, y) -> (x + y));


		/*
		 * The resultant of below DStream swapped version of above dstream.
		 * It  is a JavaPairDStream with Key and value as
		 * <Sum of tradingVolume,StockNameSymbol> 
		 * (15000.0,"stocksymbolname")
		 * This is done to sort by key in the next step
		 */
		JavaPairDStream<Double, String> swappedPair = sumOfStockVolume.mapToPair(m -> m.swap());

		
		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <Sum of tradingVolume,StockNameSymbol> 
		 * (15000.0,"stocksymbolname")
		 * This Dstream is will be in sorted order.
		 */
		JavaPairDStream<Double, String> sortedValuesOfTradingVolume = swappedPair
				.transformToPair(s -> s.sortByKey(false));

		/*
		 * The resultant of above DStream is iterated in for loop and first element is fetched 
		 * as a max trading volume.
		 * Hence it is decided that the one which has max trading volume is returned to the output
		 */
		sortedValuesOfTradingVolume.foreachRDD(rdd -> {
			if (rdd.isEmpty()) {
				Log.info("Calculating Trading Volume is in process");
			} else {
				String maxTradingVolume;
				Tuple2<Double, String> val = rdd.take(1).get(0);
				maxTradingVolume = "Stock_Symbol_Name_To_Purchase_Is:==>" + val._2() + " ,Maximum_Trading_Volume_Of_Stock:==>" + val._1();
				//String outputPath1 = "C:\\Users\\Public\\Outputs\\Analyses4_MaxTradingVol";
				String outputPath1=outputPath+"\\\\Analyses4_MaxTradingVolume_";
				String messageId = String.valueOf(System.currentTimeMillis()) + ".txt";
				String fileName = outputPath1 + messageId;
				try (PrintWriter out = new PrintWriter(fileName)) {
					Log.info("Generating Output File for Trading_Volume RSI_Analyses4");
					out.println(maxTradingVolume);
				}
			}
		});
	}
}
