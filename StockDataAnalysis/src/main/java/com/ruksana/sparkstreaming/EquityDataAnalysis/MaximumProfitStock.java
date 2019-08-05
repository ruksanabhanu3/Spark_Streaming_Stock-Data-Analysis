package com.ruksana.sparkstreaming.EquityDataAnalysis;

import java.io.PrintWriter;
import java.io.Serializable;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import com.esotericsoftware.minlog.Log;
import scala.Tuple2;

/*
 * This Class contains the required computations for Analyses 2 to calculate the Maximum Profit
 * of the stocks in a 5 min sliding window.
 */
public class MaximumProfitStock implements Serializable {
	private static final long serialVersionUID = 1L;
	public void calcMaximumProfitStock(JavaDStream<EquityData> streams, String outputPath) {
		Log.info("***Calculating Maximum Profit Of Dstreams***");

		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,<closingstockprice,1>> 
		 * ("stocksymbolname", Tuple<1000.00,1>)
		 */
		JavaPairDStream<String, Tuple2<Double, Long>> pairDStreamOfClosingPrice = streams
				.mapToPair(new PairFunction<EquityData, String, Tuple2<Double, Long>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Tuple2<Double, Long>> call(EquityData EquityData) throws Exception {
						return new Tuple2<String, Tuple2<Double, Long>>(EquityData.getStockNameSymbol(),
								new Tuple2<Double, Long>(EquityData.getClose(), (long) 1));
					}

				});

		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,<Openingstockprice,1>> 
		 * ("stocksymbolname", Tuple<10001.00,1>)
		 */
		JavaPairDStream<String, Tuple2<Double, Long>> pairDStreamOfOpeningPrice = streams
				.mapToPair(new PairFunction<EquityData, String, Tuple2<Double, Long>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Tuple2<Double, Long>> call(EquityData EquityData) throws Exception {
						return new Tuple2<String, Tuple2<Double, Long>>(EquityData.getStockNameSymbol(),
								new Tuple2<Double, Long>(EquityData.getOpen(), (long) 1));
					}
				});

		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,<SumOfClosingstockprice,1>> 
		 * ("stocksymbolname",Tuple<1000009.00, 1>)
		 */
		JavaPairDStream<String, Tuple2<Double, Long>> sumOfClosingPrice = pairDStreamOfClosingPrice.reduceByKey(
				(tuple1, tuple2) -> new Tuple2<Double, Long>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));

		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,<SumOfOpeningstockprice,1>>
		 * ("stocksymbolname",Tuple<1000999.00, 1>)
		 */
		JavaPairDStream<String, Tuple2<Double, Long>> sumOfOpeningPrice = pairDStreamOfOpeningPrice.reduceByKey(
				(tuple1, tuple2) -> new Tuple2<Double, Long>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));

		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,<AvgOfClosingstockprice,1>> 
		 * ("stocksymbolname",Tuple<100.00,1>)
		 */
		JavaPairDStream<String, Double> avgOfClosingPrice = sumOfClosingPrice
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Long>>, String, Double>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Long>> tuple) throws Exception {
						Tuple2<Double, Long> val = tuple._2;
						Double total = val._1;
						Long count = val._2;
						Double average = (Double) (total / count);
						return new Tuple2<String, Double>(tuple._1, average);
					}
				});

		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,<AvgOfOpeniningstockprice,1>>
		 * ("stocksymbolname",Tuple<300.00, 1>)
		 */
		JavaPairDStream<String, Double> avgOfOpeningPrice = sumOfOpeningPrice
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Long>>, String, Double>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Long>> tuple) throws Exception {
						Tuple2<Double, Long> val = tuple._2;
						Double total = val._1;
						Long count = val._2;
						Double average = (Double) (total / count);
						return new Tuple2<String, Double>(tuple._1, average);
					}
				});

		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,<AvgOfClosingstockprice,AvgOfOpeniningstockprice>>
		 * ("stocksymbolname",Tuple<100.00, 200.0>)
		 */
		JavaPairDStream<String, Tuple2<Double, Double>> avgOpenCloseDs = avgOfClosingPrice
				.join(avgOfOpeningPrice);

		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,(AvgOfClosingstockprice-AvgOfOpeniningstockprice)>
		 * <StockNameSymbol,profit>
		 *  ("stocksymbolname",100.0)
		 */
		JavaPairDStream<String, Double> stockProfit = avgOpenCloseDs
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Double>>, String, Double>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Double>> tuple) throws Exception {
						Tuple2<Double, Double> val = tuple._2;
						Double profit = val._1 - val._2;
						Double netProfit = profit > 0 ? profit : 0.00;
						return new Tuple2<String, Double>(tuple._1, netProfit);
					}
				});

		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <profit,StockNameSymbol> 
		 * (100.0,"stocksymbolname") 
		 * the Dstreams are sorted by key.
		 */
		JavaPairDStream<Double, String> sortedProfit = stockProfit.mapToPair(m -> m.swap())
				.transformToPair(t -> t.sortByKey(false));


		/*
		 * The resultant of above DStream is a parsed in a loop to get the first element
		 * and Saved to a file as maximum profit for that particular stream.
		 * 
		 */
		sortedProfit.foreachRDD(rdd -> {

			String profit;
			if (rdd.isEmpty()) {
				// If RDD is empty,ignore otherwise fetch the first element
				Log.info("MaximumProfit of a Stock is still processing");
			} else {
				Tuple2<Double, String> val = rdd.take(1).get(0);
				String message = "No profitable stock found during last 10 minutes";
				//Checks for the profit value greater than 0
				profit = (val._1 > 0) ? "Stock_Symbol_Name:==>" + val._2() + " ,Maximum_Profit_Of_Stock:==>" + val._1(): message;
				String outputPath1=outputPath+"\\\\Analyses2_MaxProfit_";
				String messageId = String.valueOf(System.currentTimeMillis()) + ".txt";
				String fileName = outputPath1 + messageId;
				try (PrintWriter out = new PrintWriter(fileName)) {
					Log.info("Generating Output File for Maximum_Profit_Analyses2");
					out.println(profit);
				}
			}
		});

	}

}
