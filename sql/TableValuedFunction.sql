USE [APXFirm]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



ALTER FUNCTION [CustomCDB].[fAccountsFlattened] (@AsOfDate DATETIME)
RETURNS TABLE
AS
RETURN
(
select e.EntityID 
	, e.EntityName as AccountCode
	, dh.InceptionDate
	, dh.TerminationDate
	, max(case d.entitypropertyid when 34 then v.EntityPropertyValueDesc end) as SKYStrategy
	, max(case d.entitypropertyid when 32 then v.EntityPropertyValueDesc end) as Vehicle
	, max(case d.entitypropertyid when 30 then v.EntityPropertyValueDesc end) as AccountType
	, max(case d.entitypropertyid when 29 then v.EntityPropertyValueDesc end) as TaxStatus
	, max(case d.entitypropertyid when 31 then v.EntityPropertyValueDesc end) as ClientType
	, max(case d.entitypropertyid when 366 then d.textvalue end) as Client
	, max(case d.entitypropertyid when 33 then v.EntityPropertyValueDesc end) as AccountDomicile
	, max(case d.entitypropertyid when 35 then v.EntityPropertyValueDesc end) as GeneralStrategy
	, max(case d.entitypropertyid when 38 then v.EntityPropertyValueDesc end) as GeographicMarket
	, max(case when d.entitypropertyid = 372 and d.bitvalue = 1 then 'Yes' else 'No' end) as NonDiscretionaryAcct
	, max(case d.entitypropertyid when 373 then d.datevalue end) as ChangeToNonDiscret
	, max(case d.entitypropertyid when 377 then v.EntityPropertyValueDesc end) as WilshirePlanType
	, max(case d.entitypropertyid when 378 then v.EntityPropertyValueDesc end) as WilshireAcctBreakdown
	, max(case d.entitypropertyid when 379 then v.entitypropertyvaluedesc end) as AccountCountry
	, mv.MarketValue
from CustomCDB.Entities e 
inner join CustomCDB.EntityDetails d on e.EntityID = d.EntityID
inner join CustomCDB.EntityProperties p on d.EntityPropertyID = p.EntityPropertyID
inner join customcdb.vAccountDatesHelper dh on e.EntityName = dh.AccountCode
left join CustomCDB.EntityPropertyValues v on p.EntityPropertyID = v.EntityPropertyID and d.SpecificValueID=v.EntityPropertyValueID
left join CustomCDB.fportfoliomarketvalue(@AsOfDate) mv on e.entityname = mv.portfoliobasecode
left join CustomCDB.EntityDetails sp ON e.EntityID = sp.EntityID AND sp.EntityPropertyID = 376
where e.entitytype=2
and dh.inceptiondate<=@AsOfDate
and isnull(dh.terminationdate,'1/1/3000')>@AsOfDate
and sp.TextValue is null
group by e.EntityName, e.EntityID, mv.MarketValue, dh.inceptiondate, dh.terminationdate

)
