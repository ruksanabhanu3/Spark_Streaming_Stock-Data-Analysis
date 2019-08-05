package com.ruksana.sparkstreaming.EquityDataAnalysis;

import java.io.Serializable;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.esotericsoftware.minlog.Log;

import scala.Tuple2;

/*
 * This Class contains the required computations for Analyses 1 to calculate the Simple Moving Average
 * of the stocks in a 5 min sliding window.
 */

public class SimpleMovingAvgClosingPrice implements Serializable {
	private static final long serialVersionUID = 1L;

	public void calcSimpleMvgAvgClsngPrice(JavaDStream<EquityData> streams, String outputPath) {
		Log.info("***Calculating Simple Moving Average Of Dstreams***");

		/*
		 * The resultant of this DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,<closingstokprice,1>>
		 * ("stocksymbolname", Tuple<1000.00, 1>)
		 */
		JavaPairDStream<String, Tuple2<Double, Long>> pairDSSimpleMvgAvgClsngPrice = streams
				.mapToPair(new PairFunction<EquityData, String, Tuple2<Double, Long>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Tuple2<Double, Long>> call(EquityData equityData) throws Exception {
						return new Tuple2<String, Tuple2<Double, Long>>(equityData.getStockNameSymbol(),
								new Tuple2<Double, Long>(equityData.getClose(), (long) 1));
					}

				});

		/*
		 * The resultant of this DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,<closingstokprice,addedelements>> by using reduceByKey
		 * ("stocksymbolname", Tuple<1000.00, 9>)
		 */
		JavaPairDStream<String, Tuple2<Double, Long>> addClsngPriceWithKeyAsSymbol = pairDSSimpleMvgAvgClsngPrice
				.reduceByKey(
						(tuple1, tuple2) -> new Tuple2<Double, Long>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));

		/*
		 * The resultant of this DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,avgClosingPrice)==>("StockName",1232) ("stocksymbolname",
		 * Tuple<1000.00, 9>) calcAvg method is used to do the computations
		 */
		JavaPairDStream<String, Double> avgClsngPrice = addClsngPriceWithKeyAsSymbol.mapToPair(calcAvg);

		/* Saving to the file */
		avgClsngPrice.foreachRDD(rdd -> {
			Log.info("Generating Output File for Simple_Moving_Avg_Analyses1");
			rdd.coalesce(1).saveAsTextFile(outputPath + "\\\\Analyses1_SMA_" + System.currentTimeMillis());
		});
		streams.print();
	}

	/*
	 * This method returns the average closing price for that particular Stream.
	 * input : JavaPairdDStream containing (StockName,<sum,noOfElements>) output:
	 * (StockName,SimpleMovingAvgClosingP)
	 */
	private static PairFunction<Tuple2<String, Tuple2<Double, Long>>, String, Double> calcAvg = (tuple) -> {
		Tuple2<Double, Long> val = tuple._2;
		Double sum = val._1;
		Long noOfElements = val._2;
		Tuple2<String, Double> StockAvgClsngPrice = new Tuple2<String, Double>(tuple._1, (Double) (sum / noOfElements));
		return StockAvgClsngPrice;
	};
}
