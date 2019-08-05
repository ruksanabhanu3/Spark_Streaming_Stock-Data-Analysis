package com.ruksana.sparkstreaming.EquityDataAnalysis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import com.esotericsoftware.minlog.Log;

/*
 * This Class is the main class for this application. java streaming contetx and spark conf are created and 
 * 4 analyses are being performed from this class
 */

public class SparkAnalysesDriver implements Serializable{
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		Logger.getRootLogger().setLevel(Level.ERROR);	
		if(args!=null && args.length!=2)
		{
			Log.info("Input and Output paths are not provided properly");
			System.exit(0);
		}
		
		Log.info("Input Path"+args[0]);
		Log.info("Output Path"+args[1]);
		
		//StreamingPath is the directory where python scripts generates the jsons
		String StreamingPath = args[0];
		//outputPath is the directory where the output files are created
		String outputPath = args[1];
		
		//WINUTILSconfiguration
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

		// Create Spark Configuration Object
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkAnalysesDriver").set("spark.serializer","org.apache.spark.serializer.KryoSerializer");

		// Create Java Streaming Context with batch duration as 1 minute/60 seconds.
		//JavaStreamingContext jssc = new JavaStreamingContext(Constants.BATCH_INTERVAL);
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(Constants.BATCH_INTERVAL));

		// Read the Incoming files (Streams) from the path specified using java streaming context
		JavaDStream<String> incomingStream = jssc.textFileStream(StreamingPath);
		
		
		/* 
		 * The resultant of below Dstream from the computation is passed as an input to
		 * analyses 1 and 2.It  Gets the Stream of data as per window time and transform as a
		 * stream of Object of EquityData For Analyses 1&2 with Sliding window as 5 mins and
		 * window period is 10 mins
		 */
		JavaDStream<EquityData> streams_analyses_1_2 = incomingStream.window(Durations.seconds(Constants.BATCH_DURATION_ANALYSES_1_2), Durations.seconds(Constants.SLIDING_INTERVAL_ANALYSES_1_2))
				.flatMap(jsonInput -> {
					List<EquityData> EquityDataList = new ArrayList<EquityData>();
					//json parser object is created
					JSONParser jsonParser = new JSONParser();
					//Stream in the json format is parsed and converted as a object and added to the list
					JSONArray jsonInputAsArray = (JSONArray) jsonParser.parse(jsonInput);
					for (Object object : jsonInputAsArray) {
						JSONObject jsonObjectAsInput = (JSONObject) object;
						// Converts the input of json type to the class type of EquityData.
						EquityData EquityData = convertJsonToEquityType(jsonObjectAsInput);
						EquityDataList.add(EquityData);
					}
					return EquityDataList.iterator();
				});

		/*
		 * The resultant of below Dstream from the computation is passed as an input to analyse 3
		 */
		JavaDStream<EquityData> streams_analyses_3 = incomingStream.window(Durations.seconds(Constants.BATCH_DURATION_ANALYSES_3))
				.flatMap(jsonInput -> {
					List<EquityData> EquityDataList = new ArrayList<EquityData>();
					//json parser object is created
					JSONParser jsonParser = new JSONParser();
					JSONArray jsonInputAsArray = (JSONArray) jsonParser.parse(jsonInput);
					for (Object object : jsonInputAsArray) {
						JSONObject jsonObjectAsInput = (JSONObject) object;
						// Convert the input of json type to the class type of EquityData
						EquityData EquityData = convertJsonToEquityType(jsonObjectAsInput);
						EquityDataList.add(EquityData);
					}
					return EquityDataList.iterator();
				});
		

		/*
		 * The resultant of below Dstream from the computation is passed as an input to analyse 4
		 */
		JavaDStream<EquityData> streams_analyses_4 = incomingStream.window(Durations.seconds(Constants.BATCH_DURATION_ANALYSES_4), Durations.seconds(Constants.SLIDING_INTERVAL_ANALYSES_4))
				.flatMap(jsonInput -> {
					List<EquityData> EquityDataList = new ArrayList<EquityData>();
					//json parser object is created
					JSONParser jsonParser = new JSONParser();
					JSONArray jsonInputAsArray = (JSONArray) jsonParser.parse(jsonInput);
					for (Object object : jsonInputAsArray) {
						JSONObject jsonObjectAsInput = (JSONObject) object;
						// Convert the input of json type to the class type of EquityData
						EquityData EquityData = convertJsonToEquityType(jsonObjectAsInput);
						EquityDataList.add(EquityData);
					}
					return EquityDataList.iterator();
				});

		// Create objects for the individual classes for each of the Analyses.
		SimpleMovingAvgClosingPrice analyses1 = new SimpleMovingAvgClosingPrice();
		MaximumProfitStock analyses2 = new MaximumProfitStock();
		RelativeStrengthIndex analyses3 = new RelativeStrengthIndex();
		TradingVolume analyses4 = new TradingVolume();

		/*
		 * Calling the methods of respective classes to get the output for each of the
		 * Analyses. For Calculating the 
		 * Simple Moving Average and Maximum profit,input stream is streams1_2 
		 * for calculating RSI, input is streams_3
		 * for Max Trading Volume, input is streams_4
		 */
		analyses1.calcSimpleMvgAvgClsngPrice(streams_analyses_1_2, outputPath);
		analyses2.calcMaximumProfitStock(streams_analyses_1_2, outputPath);
		analyses3.calcRsi(streams_analyses_3, outputPath);
		analyses4.calcTradingVolume(streams_analyses_4, outputPath);
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		jssc.close();
	}

	/*
	 * This method converts the input json to the object type of EquityData And all
	 * the attributes are set accordingly. input: Json Object output: Object of
	 * EquityData
	 */
	private static EquityData convertJsonToEquityType(JSONObject jsonObjectAsInput) {
		JSONObject priceData = (JSONObject) jsonObjectAsInput.get(Constants.JSON_KEY_FOR_PRICE_DATA);
		String symbol = jsonObjectAsInput.get("symbol").toString();
		String close = priceData.get("close").toString();
		String open = priceData.get("open").toString();
		String high = priceData.get("high").toString();
		String low = priceData.get("low").toString();
		String volume = priceData.get("volume").toString();
		EquityData equityData = new EquityData();
		equityData.setStockNameSymbol(symbol);
		equityData.setClose(Double.parseDouble(close));
		equityData.setOpen(Double.parseDouble(open));
		equityData.setHigh(Double.parseDouble(high));
		equityData.setLow(Double.parseDouble(low));
		equityData.setVolume((Double.parseDouble(volume)));
		return equityData;
	}
}
