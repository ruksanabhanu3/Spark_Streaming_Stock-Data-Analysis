package com.ruksana.sparkstreaming.EquityDataAnalysis;

import java.util.Date;

/*
 * This class holds the attributes of the json. Used during the json parsing
 */
public class EquityData {
	private String StockNameSymbol;
	private Double volume;
	private Double high;
	private Double low;
	private Double close;
	private Double open;
	private Date timestamp;
	public String getStockNameSymbol() {
		return StockNameSymbol;
	}
	public void setStockNameSymbol(String stockNameSymbol) {
		StockNameSymbol = stockNameSymbol;
	}
	public Double getVolume() {
		return volume;
	}
	public void setVolume(Double volume) {
		this.volume = volume;
	}
	public Double getHigh() {
		return high;
	}
	public void setHigh(Double high) {
		this.high = high;
	}
	public Double getLow() {
		return low;
	}
	public void setLow(Double low) {
		this.low = low;
	}
	public Double getClose() {
		return close;
	}
	public void setClose(Double close) {
		this.close = close;
	}
	public Double getOpen() {
		return open;
	}
	public void setOpen(Double open) {
		this.open = open;
	}
	public Date getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

}
