package com.sky.tt.portfolio;

import java.util.Map;

public class SecurityHolding {
	
	protected double quantity;
	protected String cusip;
	protected String ticker;
	protected double marketValue;
	
	public SecurityHolding(Map<String, Object> holdingData) {
		quantity = Double.parseDouble(holdingData.get("quantity").toString());
		cusip = holdingData.get("cusip").toString();
		ticker = holdingData.get("ticker").toString();
		marketValue = Double.parseDouble(holdingData.get("marketValue").toString()); //currently includes accrued interest
	}
	
	public double getQuantity() {
		return quantity;
	}
	
	public String getCusip() {
		return cusip;
	}
	
	public String getTicker() {
		return ticker;
	}
	
	public double getMarketValue() {
		return marketValue;
	}
}
