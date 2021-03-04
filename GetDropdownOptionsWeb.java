package com.sky.util;

import java.util.*;

import com.sky.tt.db.connection.TradeTicketDBException;
import com.sky.tt.db.filter.FilterClause;
import com.sky.tt.db.filter.GenericFilter;
import com.sky.tt.db.filter.OrderByClause;
import com.sky.tt.db.query.TableQuery;

public class GetDropdownOptions {
	
	
	public static String getMLSectorOptions() throws TradeTicketDBException {
		StringBuffer html = new StringBuffer();
		
		List<Map<String, Object>> groupList = null;
		GenericFilter filter = new GenericFilter();
		filter.addOrderByClause(new OrderByClause("SectorName", "asc"));
		
		groupList = TableQuery.getRows("AdvApp.vIndustrySector", filter);
		
		for (Map<String, Object> group : groupList) {
			html.append("<option value=\"" + group.get("SectorID") + "\">" + group.get("SectorName") + "</option>");
		}
		
		return html.toString();
	}
	
	public static String getMLGroupOptions(int sectorID) throws TradeTicketDBException {
		StringBuffer html = new StringBuffer();
		
		List<Map<String, Object>> sectorList = null;
		GenericFilter filter = new GenericFilter();
		filter.addFilterClause(new FilterClause("SectorID", FilterClause.FieldComparator.EQ, sectorID));
		filter.addOrderByClause(new OrderByClause("IndustryGroupName", "asc"));
		
		sectorList = TableQuery.getRows("AdvApp.vIndustryGroup", filter);
		
		for (Map<String, Object> sector : sectorList) {
			html.append("<option value=\"" + sector.get("IndustryGroupCode") + "\">" + sector.get("IndustryGroupName") + "</option>");
		}
		
		return html.toString();
	}

}
