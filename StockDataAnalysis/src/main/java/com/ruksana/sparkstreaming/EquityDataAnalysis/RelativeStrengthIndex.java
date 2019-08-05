package com.ruksana.sparkstreaming.EquityDataAnalysis;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import com.esotericsoftware.minlog.Log;
import scala.Tuple2;
/*
 * This Class contains the required computations for Analyses 3 to calculate the Relative Strength Index
 * of the stocks in a 1 min sliding window.
 */

public class RelativeStrengthIndex implements Serializable {
	
	PreviousCurrentGainLoss obj1 = new PreviousCurrentGainLoss();
	private static final long serialVersionUID = 1L;
	Double avgGainCurrent = 0.0;
	Double avgLossCurrent = 0.0;
	Double avgGainPrevious = 0.0;
	Double avgLossPrevious = 0.0;
	String avgPreviousArry[];
	int period = 11;
	int counter = 0;
	String MSFT=null;
	String FB=null;
	String GOOGL=null;
	String ADBE=null;
	Double avgGainPre = 0.0;
	Double avgLossPre = 0.0;
	private static final Map<String, PreviousCurrentGainLoss> map = new HashMap<String, PreviousCurrentGainLoss>();
	
	public void calcRsi(JavaDStream<EquityData> streams, String outputPath) {
		
		Log.info("***Calculating RSI Of Dstreams***");
		
		
		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,difference of open and close prices> 
		 * ("stocksymbolname", 50.0)
		 */
		JavaPairDStream<String, Double> profitLoss = streams
				.mapToPair(f -> new Tuple2<String, Double>(f.getStockNameSymbol(), (f.getClose() - f.getOpen())));

		
		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,filtered streams with gain>0
		 * ("stocksymbolname", 20.0)
		 */
		JavaPairDStream<String, Double> SumOfgainStreams = profitLoss.filter(f -> f._2 > 0).reduceByKey((x, y) -> x + y);

		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,filtered streams with loss<0
		 * ("stocksymbolname", -10.0)
		 */
		JavaPairDStream<String, Double> SumOflossStreams = profitLoss.filter(f -> f._2 < 0)
				.reduceByKey((x, y) -> Math.abs(x) + Math.abs(y));

		/*
		 * The resultant of below DStream is a JavaPairDStream with Key and value as
		 * <StockNameSymbol,<gain,loss>>) 
		 * ("stocksymbolname", <20.0,-10.0>)
		 */
		JavaPairDStream<String, Tuple2<Double, Double>> SumOfgainlossStream = SumOfgainStreams.join(SumOflossStreams);

		// RSI computation
		JavaPairDStream<String, Double> stockRsi = SumOfgainlossStream
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Double>>, String, Double>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Double>> tuple) throws Exception {
						Tuple2<Double, Double> val = tuple._2;
						Double RS = 0.0;
						String stockSymbolName=tuple._1.toString();
						avgGainCurrent = val._1 / 10;
						avgLossCurrent = val._2 / 10;
						
						/*If the Map already contains the stockname, get the details and calculate avg gain and loss using those values
						After calculating the avg gain and loss, set the new values to the map again.
						This way map will retain the previous price values which can be used in next iteration
						If the map doesnot contain the stockname, then simply the details are added to the map and the process continues*/
						if(map.containsKey(stockSymbolName))
						{
							PreviousCurrentGainLoss obj1 = new PreviousCurrentGainLoss();
							obj1 = map.get(stockSymbolName);
							//period value is decremented for each of the iteration and the avg gain and loss are calculated accordingly
							avgGainPrevious = ((obj1.getAvgGainPrevious() * period) + avgGainCurrent) / 10;
							avgLossPrevious = ((obj1.getAvgLossPrevious() * period) + avgLossCurrent) / 10;
							obj1.setAvgGainPrevious(avgGainPrevious);
							obj1.setAvgLossPrevious(avgLossPrevious);
							map.put(stockSymbolName, obj1);
						}
						else
						{
							PreviousCurrentGainLoss obj = new PreviousCurrentGainLoss();
							avgGainPrevious=avgGainCurrent;
							avgLossPrevious=avgLossCurrent;
							obj.setAvgGainPrevious(avgGainCurrent);
							obj.setAvgLossPrevious(avgLossCurrent);
							map.put(stockSymbolName, obj);
						}
						if (avgLossPrevious != 0) {
							RS = avgGainPrevious / avgLossPrevious;
						}
						Double RSI = 100 - (100 / (1 + RS));
						return new Tuple2<String, Double>(tuple._1, RSI);
					}
				});
		
		/*
		 * The Resultant of above Dstream after counter value>=10 is saved as tuples in the form of 
		 * (stocksymbolname,RSI)
		 */

		stockRsi.foreachRDD(rdd -> {
			counter++;
			if (counter >= 10) {
				period--;
				Log.info("Generating Output File for RSI_Analyses3");
				rdd.coalesce(1).saveAsTextFile(outputPath + "\\\\Analyses3_Rsi_" + System.currentTimeMillis());
			}
		});

	}


}
