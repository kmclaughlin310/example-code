package com.sky.tt.restriction;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sky.tt.db.connection.TradeTicketDBException;
import com.sky.tt.db.filter.FilterClause;
import com.sky.tt.db.filter.FilterClause.FieldComparator;
import com.sky.tt.db.filter.GenericFilter;
import com.sky.tt.db.query.TableQuery;
import com.sky.tt.portfolio.MarketValueSingleton;
import com.sky.tt.portfolio.Portfolio;
import com.sky.tt.security.Security;

public class AggregateRestrictionChecker {
	
	private static final Logger log = Logger.getLogger(AggregateRestrictionChecker.class);

	public Map<String, Double> checkAllRestrictions(Security security, String action, double estimatedPrice, Map<String,Double> allocations) throws Exception {
		return checkAllRestrictions(security, action, estimatedPrice, allocations, null);
	}
	
	public static Map<String, Double> checkAllRestrictions(Security security, String action, double estimatedPrice, Map<String, Double> allocations, String strategy) throws Exception {
		MarketValueSingleton singleton = null;
		
		try {
			singleton = MarketValueSingleton.getInstance();
		} catch (TradeTicketDBException e) {
			e.printStackTrace();
			log.error(e);
			return null;
		}
		
		Portfolio portfolio;
		
		List<Map<String, Object>> aggRestrictionList = null;
		AggregateRestriction aggRestriction = null;
		
		List<Map<String, Object>> aggValRestrictionList = null;
		AggregateValRestriction aggValRestriction = null;
		
		List<Map<String, Object>> aggPortRestrictionList = null;
		AggregatePortRestriction aggPortRestriction = null;
		
		double allocation = 0;
		
		double restrictionValue = 0;
		Map<String, Double> restrictionResults = new HashMap<String, Double>();
		
		//for strategy, if passed
		if (strategy != null) {
			GenericFilter filter = new GenericFilter();
			filter.addFilterClause(new FilterClause("Strategy", FieldComparator.EQ, strategy));
			filter.addFilterClause(new FilterClause("Strategy", FieldComparator.ISNULL, null, false));

			try {
				aggRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregateRestrictionList", filter);
				aggValRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregateValRestrictionList", filter);
				aggPortRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregatePortRestrictionList", filter);
			} catch (TradeTicketDBException e) {
				e.printStackTrace();
				log.error(e);
				return null;
			}
		} else {
				try {
				aggRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregateRestrictionList");
				aggValRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregateValRestrictionList");
				aggPortRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregatePortRestrictionList");
			} catch (TradeTicketDBException e) {
				e.printStackTrace();
				log.error(e);
				return null;
			}
		}
		
		//vAggregateRestrictionList
		for(Map<String, Object> rest : aggRestrictionList) {
			Class<AggregateRestriction> clazz = null;
			Constructor<AggregateRestriction> ctor = null;
				
			clazz = (Class<AggregateRestriction>) Class.forName(rest.get("JavaClassName").toString());
			ctor = clazz.getConstructor(int.class);
			aggRestriction = ctor.newInstance(Integer.parseInt(rest.get("AggregateRestrictionID").toString()));
			aggRestriction.init();
				
			portfolio = new Portfolio(aggRestriction.getPortfolioID(), rest.get("PortfolioCode").toString());
			
			if (allocations == null) {
				restrictionValue = aggRestriction.checkRestriction(security, portfolio, action, 0, estimatedPrice);
			} else {
				//get allocation or group allocation
				if (singleton.isRestrictionGroup(Integer.parseInt(rest.get("PortfolioID").toString()))) {
					for (int member : singleton.getPortfolioGroupMembers(Integer.parseInt(rest.get("PortfolioID").toString()))) {
						//have to check for allocation because of null option for portfolio restriction strategy
						allocation = allocation + (allocations.containsKey(singleton.getPortCode(member)) ? allocations.get(singleton.getPortCode(member)) : 0);
					}
				} else {
					allocation = allocations.get(rest.get("PortfolioCode").toString());
				}
				
				restrictionValue = aggRestriction.checkRestriction(security, portfolio, action, allocation, estimatedPrice);
				allocation = 0;
			}
			//log.debug(allocation + " " + rest.get("PortfolioCode") + " " + rest.get("AggregateRestrictionDescription") + " " + restrictionValue);
			//if restrictionResults doesn't have the portfolio already OR if it does have the portfolio but a lower restriction value, add new value; else do nothing
			if (! restrictionResults.containsKey(rest.get("PortfolioCode").toString() + "AggregateRestriction") || (restrictionResults.containsKey(rest.get("PortfolioCode").toString() + "AggregateRestriction") && restrictionResults.get(rest.get("PortfolioCode").toString() + "AggregateRestriction") < restrictionValue)) {
				restrictionResults.put(rest.get("PortfolioCode").toString() + "AggregateRestriction", restrictionValue);
			}
		}
		
		//vAggregateValRestrictionList
		for(Map<String, Object> rest : aggValRestrictionList) {
			Class<AggregateValRestriction> clazz = null;
			Constructor<AggregateValRestriction> ctor = null;
				
			clazz = (Class<AggregateValRestriction>) Class.forName(rest.get("JavaClassName").toString());
			ctor = clazz.getConstructor(int.class);
			aggValRestriction = ctor.newInstance(Integer.parseInt(rest.get("AggregateRestrictionID").toString()));
			aggValRestriction.init();
				
			portfolio = new Portfolio(aggValRestriction.getPortfolioID(), rest.get("PortfolioCode").toString());
			
			if (allocations == null) {
				restrictionValue = aggValRestriction.checkRestriction(security, portfolio, action, 0);
			} else {
				//get allocation or group allocation
				if (singleton.isRestrictionGroup(Integer.parseInt(rest.get("PortfolioID").toString()))) {
					for (int member : singleton.getPortfolioGroupMembers(Integer.parseInt(rest.get("PortfolioID").toString()))) {
						allocation = allocation + (allocations.containsKey(singleton.getPortCode(member)) ? allocations.get(singleton.getPortCode(member)) : 0);
					}
				} else {
					allocation = allocations.get(rest.get("PortfolioCode").toString());
				}
				
				restrictionValue = aggValRestriction.checkRestriction(security, portfolio, action, allocation);
				allocation = 0;
			}
			//log.debug(allocation + " " + rest.get("PortfolioCode") + " " + rest.get("AggregateRestrictionDescription") + " " + restrictionValue);
			//if restrictionResults doesn't have the portfolio already OR if it does have the portfolio but a lower restriction value, add new value; else do nothing
			if (! restrictionResults.containsKey(rest.get("PortfolioCode").toString() + "AggregateRestriction") || (restrictionResults.containsKey(rest.get("PortfolioCode").toString() + "AggregateRestriction") && restrictionResults.get(rest.get("PortfolioCode").toString() + "AggregateRestriction") < restrictionValue)) {
				restrictionResults.put(rest.get("PortfolioCode").toString() + "AggregateRestriction", restrictionValue);
			}
		}
				
		//vAggregatePortRestrictionList
		for(Map<String, Object> rest : aggPortRestrictionList) {
			Class<AggregatePortRestriction> clazz = null;
			Constructor<AggregatePortRestriction> ctor = null;
				
			clazz = (Class<AggregatePortRestriction>) Class.forName(rest.get("JavaClassName").toString());
			ctor = clazz.getConstructor(int.class);
			aggPortRestriction = ctor.newInstance(Integer.parseInt(rest.get("AggregateRestrictionID").toString()));
			aggPortRestriction.init();
				
			portfolio = new Portfolio(aggPortRestriction.getPortfolioID(), rest.get("PortfolioCode").toString());
			
			if (allocations == null) {
				restrictionValue = aggPortRestriction.checkRestriction(security, portfolio, action, 0, estimatedPrice);
			} else {
				//get allocation or group allocation
				if (singleton.isRestrictionGroup(Integer.parseInt(rest.get("PortfolioID").toString()))) {
					
					for (int member : singleton.getPortfolioGroupMembers(Integer.parseInt(rest.get("PortfolioID").toString()))) {
						allocation = allocation + (allocations.containsKey(singleton.getPortCode(member)) ? allocations.get(singleton.getPortCode(member)) : 0);
					}
				} else {
					allocation = allocations.get(rest.get("PortfolioCode").toString());
				}
				
				restrictionValue = aggPortRestriction.checkRestriction(security, portfolio, action, allocation, estimatedPrice);
				allocation = 0;
			}
			//log.debug(allocation + " " + rest.get("PortfolioCode") + " " + rest.get("AggregateRestrictionDescription") + " " + restrictionValue);
			//if restrictionResults doesn't have the portfolio already OR if it does have the portfolio but a lower restriction value, add new value; else do nothing
			if (! restrictionResults.containsKey(rest.get("PortfolioCode").toString() + "AggregateRestriction") || (restrictionResults.containsKey(rest.get("PortfolioCode").toString() + "AggregateRestriction") && restrictionResults.get(rest.get("PortfolioCode").toString() + "AggregateRestriction") < restrictionValue)) {
				restrictionResults.put(rest.get("PortfolioCode").toString() + "AggregateRestriction", restrictionValue);
			}
		}

		Map<String, Double> additionalResults = new HashMap<String, Double>();
		for (String k : restrictionResults.keySet()) {
			String port = k.substring(0, k.length() - 20);
			if (singleton.isRestrictionGroup(singleton.getPortID(port))) {
				for (int member : singleton.getPortfolioGroupMembers(singleton.getPortID(port))) {
					additionalResults.put(singleton.getPortCode(member) + "AggregateRestriction", restrictionResults.get(k));
				}
			}
		}
		if (!additionalResults.isEmpty()) { restrictionResults.putAll(additionalResults); }

		return restrictionResults;
	}
	
	public static double checkPortfolioRestrictions(Security security, String action, double estimatedPrice, double allocation, int portID) throws Exception {
		//allocation needs to be group allocation of portfolio is part of a group
		
		MarketValueSingleton singleton = null;
		
		try {
			singleton = MarketValueSingleton.getInstance();
		} catch (TradeTicketDBException e) {
			log.error(e);
			e.printStackTrace();
			return 0;
		}
		
		Portfolio portfolio = null;
		
		List<Map<String, Object>> aggRestrictionList = null;
		AggregateRestriction aggRestriction = null;
		
		List<Map<String, Object>> aggValRestrictionList = null;
		AggregateValRestriction aggValRestriction = null;
		
		List<Map<String, Object>> aggPortRestrictionList = null;
		AggregatePortRestriction aggPortRestriction = null;
		
		double restrictionValue = 0;
		double restrictionResults = 0;
		GenericFilter filter = new GenericFilter();
		
		if (singleton.isInRestrictionGroup(portID)) {
			filter.addFilterClause(new FilterClause("PortfolioID", FieldComparator.EQ, singleton.getParentPortfolioGroup(portID)));
		} else {
			filter.addFilterClause(new FilterClause("PortfolioID", FieldComparator.EQ, portID));
		}
		
		try {
			aggRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregateRestrictionList", filter);
			aggValRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregateValRestrictionList", filter);
			aggPortRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregatePortRestrictionList", filter);
		} catch (TradeTicketDBException e) {
			e.printStackTrace();
			log.error(e);
			return 100.0;
		}
		
		
		//vAggregateRestrictionList
		for(Map<String, Object> rest : aggRestrictionList) {
			Class<AggregateRestriction> clazz = null;
			Constructor<AggregateRestriction> ctor = null;
				
			clazz = (Class<AggregateRestriction>) Class.forName(rest.get("JavaClassName").toString());
			ctor = clazz.getConstructor(int.class);
			aggRestriction = ctor.newInstance(Integer.parseInt(rest.get("AggregateRestrictionID").toString()));
			aggRestriction.init();
			
			if (portfolio == null) {
				portfolio = new Portfolio(aggRestriction.getPortfolioID(), rest.get("PortfolioCode").toString());
			}
			
			restrictionValue = aggRestriction.checkRestriction(security, portfolio, action, allocation, estimatedPrice);
			restrictionResults = (restrictionValue > restrictionResults ? restrictionValue : restrictionResults);
			
		}
		
		//vAggregateValRestrictionList
		for(Map<String, Object> rest : aggValRestrictionList) {
			Class<AggregateValRestriction> clazz = null;
			Constructor<AggregateValRestriction> ctor = null;
				
			clazz = (Class<AggregateValRestriction>) Class.forName(rest.get("JavaClassName").toString());
			ctor = clazz.getConstructor(int.class);
			aggValRestriction = ctor.newInstance(Integer.parseInt(rest.get("AggregateRestrictionID").toString()));
			aggValRestriction.init();
				
			restrictionValue = aggValRestriction.checkRestriction(security, portfolio, action, allocation);
			restrictionResults = (restrictionValue > restrictionResults ? restrictionValue : restrictionResults);
		}
				
		//vAggregatePortRestrictionList
		for(Map<String, Object> rest : aggPortRestrictionList) {
			Class<AggregatePortRestriction> clazz = null;
			Constructor<AggregatePortRestriction> ctor = null;
				
			clazz = (Class<AggregatePortRestriction>) Class.forName(rest.get("JavaClassName").toString());
			ctor = clazz.getConstructor(int.class);
			aggPortRestriction = ctor.newInstance(Integer.parseInt(rest.get("AggregateRestrictionID").toString()));
			aggPortRestriction.init();
			
			restrictionValue = 0;
			restrictionValue = aggPortRestriction.checkRestriction(security, portfolio, action, allocation, estimatedPrice);
			restrictionResults = (restrictionValue > restrictionResults ? restrictionValue : restrictionResults);
		}

		return restrictionResults;
	}
	
	public static List<Map<String, Object>> getPortfolioRestrictionDetails(Security security, String action, double estimatedPrice, double allocation, int portID) throws Exception {
		//allocation needs to be group allocation of portfolio is part of a group
		MarketValueSingleton singleton = null;
		
		try {
			singleton = MarketValueSingleton.getInstance();
		} catch (TradeTicketDBException e) {
			e.printStackTrace();
			log.error(e);
			return null;
		}
		
		Portfolio portfolio = null;
		
		List<Map<String, Object>> aggRestrictionList = null;
		AggregateRestriction aggRestriction = null;
		
		List<Map<String, Object>> aggValRestrictionList = null;
		AggregateValRestriction aggValRestriction = null;
		
		List<Map<String, Object>> aggPortRestrictionList = null;
		AggregatePortRestriction aggPortRestriction = null;
		
		Map<String, Object> restrictionResultsMap = new HashMap<String, Object>(); 
		List<Map<String, Object>> resultDetails = new ArrayList<Map<String, Object>>();
		
		double restrictionValue = 0;
		GenericFilter filter = new GenericFilter();
	
		if (singleton.isInRestrictionGroup(portID)) {
			filter.addFilterClause(new FilterClause("PortfolioID", FieldComparator.EQ, singleton.getParentPortfolioGroup(portID)));
		} else {
			filter.addFilterClause(new FilterClause("PortfolioID", FieldComparator.EQ, portID));
		}
		
		try {
			aggRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregateRestrictionList", filter);
			aggValRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregateValRestrictionList", filter);
			aggPortRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregatePortRestrictionList", filter);
		} catch (TradeTicketDBException e) {
			e.printStackTrace();
			log.error(e);
			return null;
		}
		
		
		//vAggregateRestrictionList
		for(Map<String, Object> rest : aggRestrictionList) {
			Class<AggregateRestriction> clazz = null;
			Constructor<AggregateRestriction> ctor = null;
				
			clazz = (Class<AggregateRestriction>) Class.forName(rest.get("JavaClassName").toString());
			ctor = clazz.getConstructor(int.class);
			aggRestriction = ctor.newInstance(Integer.parseInt(rest.get("AggregateRestrictionID").toString()));
			aggRestriction.init();
			
			if (portfolio == null) {
				portfolio = new Portfolio(aggRestriction.getPortfolioID(), rest.get("PortfolioCode").toString());
			}
			
			restrictionValue = aggRestriction.checkRestriction(security, portfolio, action, allocation, estimatedPrice);
			
			restrictionResultsMap.put("RestrictionDescription", rest.get("AggregateRestrictionDescription").toString());
			restrictionResultsMap.put("RestrictionResult", restrictionValue);
			resultDetails.add(restrictionResultsMap);
			restrictionResultsMap = new HashMap<String, Object>();
		}
		
		//vAggregateValRestrictionList
		for(Map<String, Object> rest : aggValRestrictionList) {
			Class<AggregateValRestriction> clazz = null;
			Constructor<AggregateValRestriction> ctor = null;
				
			clazz = (Class<AggregateValRestriction>) Class.forName(rest.get("JavaClassName").toString());
			ctor = clazz.getConstructor(int.class);
			aggValRestriction = ctor.newInstance(Integer.parseInt(rest.get("AggregateRestrictionID").toString()));
			aggValRestriction.init();
				
			restrictionValue = aggValRestriction.checkRestriction(security, portfolio, action, allocation);
			
			restrictionResultsMap.put("RestrictionDescription", rest.get("AggregateRestrictionDescription").toString());
			restrictionResultsMap.put("RestrictionResult", restrictionValue);
			resultDetails.add(restrictionResultsMap);
			restrictionResultsMap = new HashMap<String, Object>();
		}
				
		//vAggregatePortRestrictionList
		for(Map<String, Object> rest : aggPortRestrictionList) {
			Class<AggregatePortRestriction> clazz = null;
			Constructor<AggregatePortRestriction> ctor = null;
				
			clazz = (Class<AggregatePortRestriction>) Class.forName(rest.get("JavaClassName").toString());
			ctor = clazz.getConstructor(int.class);
			aggPortRestriction = ctor.newInstance(Integer.parseInt(rest.get("AggregateRestrictionID").toString()));
			aggPortRestriction.init();
			
			restrictionValue = aggPortRestriction.checkRestriction(security, portfolio, action, allocation, estimatedPrice);
			
			restrictionResultsMap.put("RestrictionDescription", rest.get("AggregateRestrictionDescription").toString());
			restrictionResultsMap.put("RestrictionResult", restrictionValue);
			resultDetails.add(restrictionResultsMap);
			restrictionResultsMap = new HashMap<String, Object>();
		}

		return resultDetails;
	}
	
	public static List<Map<String, Object>> getRestrictionsForArchive(Security security, String action, double estimatedPrice, Map<String, Double> allocations) throws Exception {
		MarketValueSingleton singleton = null;
		
		try {
			singleton = MarketValueSingleton.getInstance();
		} catch (TradeTicketDBException e) {
			e.printStackTrace();
			log.error(e);
			return null;
		}
		
		Portfolio portfolio;
		
		List<Map<String, Object>> aggRestrictionList = null;
		AggregateRestriction aggRestriction = null;
		
		List<Map<String, Object>> aggValRestrictionList = null;
		AggregateValRestriction aggValRestriction = null;
		
		List<Map<String, Object>> aggPortRestrictionList = null;
		AggregatePortRestriction aggPortRestriction = null;
		
		double allocation = 0;
		double restrictionValue;
		List<Map<String, Object>> allResults = new ArrayList<Map<String, Object>>();
		Map<String, Object> restrictionResult = new HashMap<String, Object>();

		try {
			aggRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregateRestrictionList");
			aggValRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregateValRestrictionList");
			aggPortRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregatePortRestrictionList");
		} catch (TradeTicketDBException e) {
			e.printStackTrace();
			log.error(e);
			return null;
		}

		
		//vAggregateRestrictionList
		for(Map<String, Object> rest : aggRestrictionList) {
			Class<AggregateRestriction> clazz = null;
			Constructor<AggregateRestriction> ctor = null;
				
			clazz = (Class<AggregateRestriction>) Class.forName(rest.get("JavaClassName").toString());
			ctor = clazz.getConstructor(int.class);
			aggRestriction = ctor.newInstance(Integer.parseInt(rest.get("AggregateRestrictionID").toString()));
			aggRestriction.init();
				
			portfolio = new Portfolio(aggRestriction.getPortfolioID(), rest.get("PortfolioCode").toString());
			
			//get allocation or group allocation
			if (singleton.isRestrictionGroup(Integer.parseInt(rest.get("PortfolioID").toString()))) {
				for (int member : singleton.getPortfolioGroupMembers(Integer.parseInt(rest.get("PortfolioID").toString()))) {
					allocation = allocation + allocations.get(singleton.getPortCode(member));
				}
			} else {
				allocation = allocations.get(rest.get("PortfolioCode").toString());
			}
			
			restrictionValue = aggRestriction.checkRestriction(security, portfolio, action, allocation, estimatedPrice);
			allocation = 0;
			
			restrictionResult.put("PortfolioID", aggRestriction.getPortfolioID());
			restrictionResult.put("RestrictionID", aggRestriction.getRestrictionID());
			restrictionResult.put("RestrictionType", "AggregateRestrictionList");
			restrictionResult.put("Result", restrictionValue);
			restrictionResult.put("PortfolioCode", rest.get("PortfolioCode"));
			restrictionResult.put("RestrictionDesc", rest.get("AggregateRestrictionDescription"));
			
			allResults.add(restrictionResult);
			restrictionResult = new HashMap<String, Object>();
		}
		
		//vAggregateValRestrictionList
		for(Map<String, Object> rest : aggValRestrictionList) {
			Class<AggregateValRestriction> clazz = null;
			Constructor<AggregateValRestriction> ctor = null;
				
			clazz = (Class<AggregateValRestriction>) Class.forName(rest.get("JavaClassName").toString());
			ctor = clazz.getConstructor(int.class);
			aggValRestriction = ctor.newInstance(Integer.parseInt(rest.get("AggregateRestrictionID").toString()));
			aggValRestriction.init();
				
			portfolio = new Portfolio(aggValRestriction.getPortfolioID(), rest.get("PortfolioCode").toString());
			
			//get allocation or group allocation
			if (singleton.isRestrictionGroup(Integer.parseInt(rest.get("PortfolioID").toString()))) {
				for (int member : singleton.getPortfolioGroupMembers(Integer.parseInt(rest.get("PortfolioID").toString()))) {
					allocation = allocation + allocations.get(singleton.getPortCode(member));
				}
			} else {
				allocation = allocations.get(rest.get("PortfolioCode").toString());
			}

			restrictionValue = aggValRestriction.checkRestriction(security, portfolio, action, allocation);
			allocation = 0;
			
			restrictionResult.put("PortfolioID", aggValRestriction.getPortfolioID());
			restrictionResult.put("RestrictionID", aggValRestriction.getRestrictionID());
			restrictionResult.put("RestrictionType", "AggregateValRestrictionList");
			restrictionResult.put("Result", restrictionValue);
			restrictionResult.put("PortfolioCode", rest.get("PortfolioCode"));
			restrictionResult.put("RestrictionDesc", rest.get("AggregateRestrictionDescription"));
			
			allResults.add(restrictionResult);
			restrictionResult = new HashMap<String, Object>();
		}
				
		//vAggregatePortRestrictionList
		for(Map<String, Object> rest : aggPortRestrictionList) {
			Class<AggregatePortRestriction> clazz = null;
			Constructor<AggregatePortRestriction> ctor = null;
				
			clazz = (Class<AggregatePortRestriction>) Class.forName(rest.get("JavaClassName").toString());
			ctor = clazz.getConstructor(int.class);
			aggPortRestriction = ctor.newInstance(Integer.parseInt(rest.get("AggregateRestrictionID").toString()));
			aggPortRestriction.init();
				
			portfolio = new Portfolio(aggPortRestriction.getPortfolioID(), rest.get("PortfolioCode").toString());
			
			//get allocation or group allocation
			if (singleton.isRestrictionGroup(Integer.parseInt(rest.get("PortfolioID").toString()))) {
				for (int member : singleton.getPortfolioGroupMembers(Integer.parseInt(rest.get("PortfolioID").toString()))) {
					allocation = allocation + allocations.get(singleton.getPortCode(member));
				}
			} else {
				allocation = allocations.get(rest.get("PortfolioCode").toString());
			}
			
			restrictionValue = aggPortRestriction.checkRestriction(security, portfolio, action, allocation, estimatedPrice);
			allocation = 0;
			
			restrictionResult.put("PortfolioID", aggPortRestriction.getPortfolioID());
			restrictionResult.put("RestrictionID", aggPortRestriction.getRestrictionID());
			restrictionResult.put("RestrictionType", "AggregatePortRestrictionList");
			restrictionResult.put("Result", restrictionValue);
			restrictionResult.put("PortfolioCode", rest.get("PortfolioCode"));
			restrictionResult.put("RestrictionDesc", rest.get("AggregateRestrictionDescription"));
			
			allResults.add(restrictionResult);
			restrictionResult = new HashMap<String, Object>();
		}

		return allResults;
	}
}
