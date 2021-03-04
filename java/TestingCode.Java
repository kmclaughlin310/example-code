package com.sky.tt.testing;

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.json.JSONObject;

import com.sky.tt.db.connection.TradeTicketDBException;
import com.sky.tt.db.filter.FilterClause;
import com.sky.tt.db.filter.GenericFilter;
import com.sky.tt.db.query.TableQuery;
import com.sky.tt.portfolio.MarketValueSingleton;
import com.sky.tt.portfolio.Portfolio;
import com.sky.tt.restriction.AggregatePortRestriction;
import com.sky.tt.restriction.AggregateRestriction;
import com.sky.tt.restriction.AggregateValRestriction;
import com.sky.tt.restriction.SecurityRestriction;
import com.sky.tt.restriction.security.SecurityNumRatingsRestriction;
import com.sky.tt.security.Security;
import com.sky.tt.security.SecurityConverter;
import com.sky.tt.security.SecurityField;
import com.sky.tt.security.ratingcalc.SecurityRatingCalculator;
import com.sky.tt.logic.DashboardHoldings;
import com.sky.tt.jsonconvert.*;

public class Testing {

	public static void main(String[] args) throws Exception {
		//test APX security 97381WAJ3
		/*APXDatabaseUtility dbRequester = APXDatabaseUtility.getInstance();
		APXSecurity apxSecurity = dbRequester.getAPXSecurity("BL1161803");
		SecurityConverter converter = SecurityConverter.getInstance();
		
		Security security = converter.convertAPXMapToSecurity(apxSecurity);
		
		System.out.println(security.toString());*/
		
		//test BBG security 03852UAA4
		String JSONBbgData = "{\"CPN\":\"8.5\",\"AMT_OUTSTANDING\":\"2.5E8\",\"REAL_BLOOMBERG_DEPT_DES\":\"Corp\",\"DDIS_AMT_OUTSTANDING_ISSUER_SUBS\":\"250000.0\",\"INT_ACC_DT\":\"2010-05-10\",\"MATURITY\":\"2020-05-15\",\"COLLAT_TYP\":\"COMPANY GUARNT\",\"DEFAULTED\":\"N\",\"CNTRY_OF_INCORPORATION\":\"US\",\"IS_PERPETUAL\":\"N\",\"CPN_TYP\":\"FIXED\",\"MIN_PIECE\":\"2000.0\",\"ID_BB_ULTIMATE_PARENT_CO_NAME\":\"Ironshore Inc\",\"ISSUE_DT\":\"2010-05-10\",\"HYBRID\":\"N\",\"DAY_CNT_DES\":\"30/360\",\"144A_REG_RIGHTS\":\"N\",\"CPN_FREQ\":\"2\",\"RTG_MOODY_NO_WATCH\":\"Baa3\",\"CNTRY_OF_DOMICILE\":\"US\",\"RTG_SP_NO_WATCH\":\"BBB\",\"INDUSTRY_SUBGROUP_NUM\":\"163\",\"FIRST_CPN_DT\":\"2010-11-15\",\"YLD_CNV_BID\":\"8.4948033\",\"PENULTIMATE_CPN_DT\":\"2019-11-15\",\"SECURITY_DES\":\"IRONHG 8 1/2 05/15/20\",\"TICKER\":\"IRONHG\",\"SECURITY_FACTORABLE\":\"N\",\"ISSUER\":\"IRONSHORE HOLDGS US INC\",\"ID_SEDOL1\":\"B44CK21\",\"ID_CUSIP\":\"46322VAA3\",\"144A_FLAG\":\"Y\",\"INT_ACC\":\"1.86527778\",\"ID_ISIN\":\"US46322VAA35\",\"CNTRY_OF_RISK\":\"US\",\"DRD_ELIGIBLE\":\"No\"}";
		//String JSONBbgData = "{\"AMT_OUTSTANDING\":\"1.1E9\",\"CPN_FREQ\":\"4\",\"RTG_MOODY_NO_WATCH\":\"B1\",\"CNTRY_OF_DOMICILE\":\"US\",\"RTG_SP_NO_WATCH\":\"B\",\"DDIS_AMT_OUTSTANDING_ISSUER_SUBS\":\"630000.0\",\"INDUSTRY_SUBGROUP_NUM\":\"163\",\"LN_CURRENT_MARGIN\":\"325.0\",\"MATURITY\":\"2019-12-27\",\"COLLAT_TYP\":\"Company Assets,\",\"YLD_CNV_BID\":\"4.24917030403358\",\"SECURITY_DES\":\"COMINV TL B 1L USD\",\"DEFAULTED\":\"N\",\"CNTRY_OF_INCORPORATION\":\"US\",\"CPN_TYP\":\"FLOATING\",\"TICKER\":\"COMINV\",\"ISSUER\":\"USI INC\",\"ID_CUSIP\":\"20451XAD1\",\"ID_BB_ULTIMATE_PARENT_CO_NAME\":\"Onex Corp\",\"ISSUE_DT\":\"2013-12-30\",\"INT_ACC\":\"0.08447531\",\"ID_ISIN\":\"US20451XAD12\",\"CNTRY_OF_RISK\":\"US\",\"DAY_CNT_DES\":\"ACT/360\"}";
		//String JSONBbgData = "{\"SECTYPE_OVERRIDE\":\"blus\",\"CPN\":\"9.75\",\"AMT_OUTSTANDING\":\"3.0E8\",\"REAL_BLOOMBERG_DEPT_DES\":\"Corp\",\"DDIS_AMT_OUTSTANDING_ISSUER_SUBS\":\"1370000.0\",\"INT_ACC_DT\":\"2011-04-15\",\"MATURITY\":\"2017-10-15\",\"COLLAT_TYP\":\"COMPANY GUARNT\",\"DEFAULTED\":\"N\",\"CNTRY_OF_INCORPORATION\":\"US\",\"IS_PERPETUAL\":\"N\",\"CPN_TYP\":\"FIXED\",\"MIN_PIECE\":\"2000.0\",\"ID_BB_ULTIMATE_PARENT_CO_NAME\":\"Blackstone Group LP/The\",\"ISSUE_DT\":\"2011-10-07\",\"HYBRID\":\"N\",\"DAY_CNT_DES\":\"30/360\",\"144A_REG_RIGHTS\":\"N\",\"CPN_FREQ\":\"2\",\"RTG_MOODY_NO_WATCH\":\"Caa2\",\"CNTRY_OF_DOMICILE\":\"US\",\"RTG_SP_NO_WATCH\":\"CCC\",\"INDUSTRY_SUBGROUP_NUM\":\"117\",\"FIRST_CPN_DT\":\"2011-10-15\",\"YLD_CNV_BID\":\"9.7373955\",\"PENULTIMATE_CPN_DT\":\"2017-04-15\",\"SECURITY_DES\":\"ENMC 9 3/4 10/15/17\",\"TICKER\":\"ENMC\",\"SECURITY_FACTORABLE\":\"N\",\"ISSUER\":\"DJO FIN LLC/DJO FIN CORP\",\"ID_SEDOL1\":\"B6ZD0V6\",\"ID_CUSIP\":\"23327BAD5\",\"144A_FLAG\":\"N\",\"INT_ACC\":\"3.92708333\",\"ID_ISIN\":\"US23327BAD55\",\"CNTRY_OF_RISK\":\"US\",\"DRD_ELIGIBLE\":\"No\"}";
		
		Security security = SecurityConverter.convertBBGMapToSecurity(JSONBbgData);
		
		System.out.println(security.toString());
		
				
		
		//test security restrictions
		List<Map<String, Object>> secRestrictionList = null;
		SecurityRestriction restriction;
		StringBuffer restrictionResults = new StringBuffer();
		Portfolio portfolio = null;
		
		try {
			//for testing all restrictions
			secRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vSecurityRestrictionList");
			
			//for testing a specific restriction
			/*GenericFilter filter = new GenericFilter();
			filter.addFilterClause(new FilterClause("SecurityRestrictionID", FilterClause.FieldComparator.EQ, 110));
			secRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vSecurityRestrictionList", filter);*/
		} catch (TradeTicketDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		for(Map<String, Object> rest : secRestrictionList) {
				Class<SecurityRestriction> clazz = (Class<SecurityRestriction>) Class.forName(rest.get("JavaClassName").toString());
				Constructor<SecurityRestriction> ctor = clazz.getConstructor(int.class);
				
				restriction = ctor.newInstance(Integer.parseInt(rest.get("SecurityRestrictionID").toString()));
				
				//System.out.println(rest.get("SecurityRestrictionID").toString());
				
				restriction.init();
				restrictionResults.append("Restriction: " + rest.get("SecurityRestrictionID").toString() + ", " + rest.get("SecurityRestrictionDescription").toString() + "; Portfolio Code: " + rest.get("PortfolioCode").toString() + "; Result: " + (restriction.checkRestriction(security, portfolio) == false ? "false" : "true"));
				restrictionResults.append("\n");
		}
		
		
		System.out.println(restrictionResults.toString());
		
		//test agg restrictions
		/*List<Map<String, Object>> aggRestrictionList = null;
		AggregateRestriction aggRestriction;
		
		List<Map<String, Object>> aggValRestrictionList = null;
		AggregateValRestriction aggValRestriction;
		
		List<Map<String, Object>> aggPortRestrictionList = null;
		AggregatePortRestriction aggPortRestriction;
		
		StringBuffer restrictionResults = new StringBuffer();
		Portfolio portfolio = null;
		
		try {
			//for testing all restrictions
			aggRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregateRestrictionList");
			aggValRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregateValRestrictionList");
			aggPortRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.vAggregatePortRestrictionList");
			
			//for testing a specific restriction
			//GenericFilter filter = new GenericFilter();
			filter.addFilterClause(new FilterClause("AggregateRestrictionID", FilterClause.FieldComparator.EQ, 20));
			//aggRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.AggregateRestrictionList", filter);
			aggRestrictionList = TableQuery.getRows("CustomTradeTktRestriction.AggregateRestrictionList", filter);
		} catch (TradeTicketDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		double restrictionValue;
		for(Map<String, Object> rest : aggRestrictionList) {
				Class<AggregateRestriction> clazz = (Class<AggregateRestriction>) Class.forName(rest.get("JavaClassName").toString());
				Constructor<AggregateRestriction> ctor = clazz.getConstructor(int.class);
				
				//System.out.println(rest.get("AggregateRestrictionID").toString());
				aggRestriction = ctor.newInstance(Integer.parseInt(rest.get("AggregateRestrictionID").toString()));
				
				aggRestriction.init();
				portfolio = new Portfolio(aggRestriction.getPortfolioID());
				
				//checkRestriction(Security security, Portfolio portfolio, String action, double quantity, double estimatedPrice);
				//val - checkRestriction(Security security, Portfolio portfolio, String action, double quantity)
				restrictionValue = aggRestriction.checkRestriction(security, portfolio, "buy", 100, 100);
				
				restrictionResults.append("Restriction: " + aggRestriction.getRestrictionID() + ", " + rest.get("AggregateRestrictionDescription").toString() + "; Portfolio ID: " + aggRestriction.getPortfolioID() + "; Result: " + (restrictionValue > 100 ? "true " + restrictionValue : "false " + restrictionValue));
				restrictionResults.append("\n");
		}
		for(Map<String, Object> rest : aggRestrictionList) {
			Class<AggregateRestriction> clazz = (Class<AggregateRestriction>) Class.forName(rest.get("JavaClassName").toString());
			Constructor<AggregateRestriction> ctor = clazz.getConstructor(int.class);
			
			//System.out.println(rest.get("AggregateRestrictionID").toString());
			aggRestriction = ctor.newInstance(Integer.parseInt(rest.get("AggregateRestrictionID").toString()));
			
			aggRestriction.init();
			portfolio = new Portfolio(aggRestriction.getPortfolioID());
			
			//checkRestriction(Security security, Portfolio portfolio, String action, double quantity, double estimatedPrice);
			//val - checkRestriction(Security security, Portfolio portfolio, String action, double quantity)
			restrictionValue = aggRestriction.checkRestriction(security, portfolio, "buy", 100, 100);
			
			restrictionResults.append("Restriction: " + aggRestriction.getRestrictionID() + ", " + rest.get("AggregateRestrictionDescription").toString() + "; Portfolio ID: " + aggRestriction.getPortfolioID() + "; Result: " + (restrictionValue > 100 ? "true " + restrictionValue : "false " + restrictionValue));
			restrictionResults.append("\n");
		}
		
		
		
		System.out.println(restrictionResults.toString());*/
		
		
		
		
		/*MarketValueSingleton mktValCalc = null;
		mktValCalc = MarketValueSingleton.getInstance();
		System.out.println(mktValCalc.toString());*/
	
		
		//test dashboard holdings
		/*String cusip = "877249AA0";
		String ticker = "TAYMON";
		String action = "buy";
		double price = 101;
		Map<String, Double> allocations = new HashMap<String, Double>(); //maps portfolio code to quantity
		double accrInt =  2.626;
		List<Map<String, Object>> dashHoldingsList = new ArrayList<Map<String, Object>>(); //should be map with key as portfolio code
		Map<String, Object> ttMap = new HashMap<String, Object>();
		
		allocations.put("APK-SDHY",55.0);
		allocations.put("AXIS-SDHY",560.0);
		allocations.put("AXISRE-SDHY",50.0);
		allocations.put("AXRESE-SDHY",20.0);
		allocations.put("AXSEUR-SDHY",10.0);
		allocations.put("BBBANK-SDHY",0.0);
		allocations.put("CHUBB-SDHY",0.0);
		allocations.put("ERIEEX-SDHY",50.0);
		allocations.put("ERIEID-SDHY",0.0);
		allocations.put("SEI-SDHY",90.0);
		allocations.put("SHYP-SDHY",180.0);
		allocations.put("SKYSHO-SDHY",3165.0);
		allocations.put("USHMM-SDHY",5.0);
		allocations.put("WWEB-SDHY",35.0);
		allocations.put("WWPT-SDHY",0.0);
		allocations.put("WWPT-CORE",270.0);
		allocations.put("WWEB-CORE",105.0);
		allocations.put("BCBS-CORE",375.0);
		allocations.put("HIGHM-CORE",300.0);
		allocations.put("HIGH2-CORE",155.0);
		allocations.put("HHIC-CORE",110.0);
		allocations.put("SKYBY-CORE",1740.0);
		allocations.put("KZVK-CORE",715.0);
		allocations.put("BHYP-CORE",1575.0);
		allocations.put("OPPHY-CORE",10.0);
		
		ttMap.put("CUSIP", cusip);
		ttMap.put("ticker", ticker);
		ttMap.put("action", action);
		ttMap.put("price", price);
		ttMap.put("accruedInt", accrInt);
		ttMap.put("allocations", allocations);

		dashHoldingsList = DashboardHoldings.getDashHoldings(ttMap);

		System.out.println(dashHoldingsList.toString());	*/
		
		
		
		/*String json = "{\"CUSIP\":\"877249AA0\",\"accruedInt\":\"2.77708333\",\"action\":\"buy\",\"ticker\":\"TAYMON\",\"price\":\"101\",\"allocations\":[{\"PORTCODE\":\"APK-SDHY\",\"QUANTITY\":\"55\"},{\"PORTCODE\":\"AXIS-SDHY\",\"QUANTITY\":\"560\"}]}";
		
		Map<String, Object> parsedMap = new HashMap<String, Object>();
				
		parsedMap = JSONStringConverter.getTTMapFromJSONString(json);



		Map<String, Map<String, Object>> dashHoldingsList = new HashMap<String, Map<String, Object>>();
		dashHoldingsList = DashboardHoldings.getDashHoldings(parsedMap);
		
		System.out.print(dashHoldingsList.toString());
		
		String jsonObj;
		jsonObj = JSONStringConverter.getJSONForTTHoldings(dashHoldingsList);
		
		System.out.println("test");
		System.out.println(jsonObj);*/
	}
}












