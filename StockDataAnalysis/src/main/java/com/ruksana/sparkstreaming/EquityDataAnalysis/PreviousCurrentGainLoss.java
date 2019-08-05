package com.ruksana.sparkstreaming.EquityDataAnalysis;

import java.io.Serializable;

public class PreviousCurrentGainLoss implements Serializable{
	/**
	 * This Class is used as an value object for the hashmap created to calculate the Relative Strength Index
	 * to hold the previous gain and loss for the particular stockname
	 */
	
	private static final long serialVersionUID = 1L;
	double avgGainPrevious;
	double avgLossPrevious;
	public double getAvgGainPrevious() {
		return avgGainPrevious;
	}
	public void setAvgGainPrevious(double avgGainPrevious) {
		this.avgGainPrevious = avgGainPrevious;
	}
	public double getAvgLossPrevious() {
		return avgLossPrevious;
	}
	public void setAvgLossPrevious(double avgLossPrevious) {
		this.avgLossPrevious = avgLossPrevious;
	}
}
