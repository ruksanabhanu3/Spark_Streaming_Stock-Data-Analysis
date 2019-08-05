package com.ruksana.sparkstreaming.EquityDataAnalysis;

/*
 * This class holds all the constants that are required for this application
 */

public class Constants {

	public static final int BATCH_INTERVAL=60;
	
	//For analyses 1 and 2 batch duration and sliding interval are same. Output file gets generated for every 5 mins.
	public static final int BATCH_DURATION_ANALYSES_1_2=600;
	public static final int SLIDING_INTERVAL_ANALYSES_1_2=300;
	
	//For analyses 3 slidinginterval equals to batch interval. But output file generation happens after 10th iteration for each minute
	public static final int BATCH_DURATION_ANALYSES_3=600;
	
	//For analyses 4 output files are generated for every 10 mins
	public static final int BATCH_DURATION_ANALYSES_4=600;
	public static final int SLIDING_INTERVAL_ANALYSES_4=600;
	
	
	//From the inputjson, key of the pricedetails.
	public static final String JSON_KEY_FOR_PRICE_DATA="priceData";
	

}


