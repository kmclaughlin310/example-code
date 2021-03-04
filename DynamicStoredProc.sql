USE [APXFirm]
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [CustomIndex].[pDailyReturnsCompare2]
      @SecUniverse nvarchar(50), --current options: SecMLIndex, SecStrategy, SecCapStructure, SecSectorL2 (L3, L4) 
      @SecSpecific nvarchar(max), 
      @CompUniverse nvarchar(50), --current options: CompIndex, CompStrategy, CompCapStructure, CompAccount 
      @CompSpecific nvarchar (50),
      @StartDate datetime,
      @EndDate datetime  --will always be previous bus day
AS

BEGIN

DECLARE @Securities NVARCHAR(max)
DECLARE @SecuritiesIn nvarchar(max)


SET @Securities = 
	case
	when @SecUniverse = 'SecMLIndex' then CustomIndex.fGetSecuritiesIndex(@EndDate)
	when @SecUniverse = 'SecStrategy' then CustomIndex.fGetSecuritiesStrategy(@SecSpecific,@EndDate)
	when @SecUniverse = 'SecCapStructure' then CustomIndex.fGetSecuritiesTicker(@SecSpecific)
	when @SecUniverse = 'SecSectorL2' then CustomIndex.fGetSecuritiesSector(2,@SecSpecific,@EndDate)
	when @SecUniverse = 'SecSectorL3' then CustomIndex.fGetSecuritiesSector(3,@SecSpecific,@EndDate)
	when @SecUniverse = 'SecSectorL4' then CustomIndex.fGetSecuritiesSector(4,@SecSpecific,@EndDate)
	when @SecUniverse = 'SecTickerList' then @SecSpecific
	end


DECLARE @query NVARCHAR(MAX)

if @SecUniverse = 'SecStrategy'
begin
	SET @query = 
		case	
		--done
		when @CompUniverse = 'CompMLIndex' then
			N'SELECT AsOfDate		
			, ' + @Securities + '
			, IndexReturn
			FROM 
			(select d.CUSIP
				, d.AsOfDate
				, d.Perf
				, c.TotalReturn as IndexReturn
			from CustomIndex.vSecuritiesStrategy s
			inner join CustomIndex.vAPXSecurityPerformance d on s.CUSIP = d.Cusip
			inner join CustomIndex.fReturnsIndex(''' + convert(nvarchar(10),@StartDate,101) + ''',''' + convert(nvarchar(10),@EndDate,101) + ''') c on d.AsOfDate = c.AsOfDate 
			where d.AsOfDate between ''' + convert(nvarchar(10),@StartDate,101) + ''' and ''' + convert(nvarchar(10),@EndDate,101) + '''
			and s.PortfolioCompositeCode = ''' + @SecSpecific + '''
			) p
			PIVOT
				(MAX(p.Perf)
				FOR p.cusip IN (' + @Securities + ')) AS pvt order by asofdate desc'
		--done
		when @CompUniverse in ('CompStrategy','CompAccount') then
			N'SELECT AsOfDate		
			, ' + @Securities + '
			, IRR
			FROM 
			(select d.CUSIP
				, d.AsOfDate
				, d.Perf
				, c.IRR
			from CustomIndex.vSecuritiesStrategy s
			inner join CustomIndex.vAPXSecurityPerformance d on s.CUSIP = d.Cusip
			inner join CustomIndex.fReturnsPortfolio(''' + @CompSpecific + ''',''' + convert(nvarchar(10),@StartDate,101) + ''',''' + convert(nvarchar(10),@EndDate,101) + ''') c on d.AsOfDate = c.AsOfDate 
			where d.AsOfDate between ''' + convert(nvarchar(10),@StartDate,101) + ''' and ''' + convert(nvarchar(10),@EndDate,101) + '''
			and s.PortfolioCompositeCode = ''' + @SecSpecific + '''
			) p
			PIVOT
				(MAX(p.Perf)
				FOR p.cusip IN (' + @Securities + ')) AS pvt order by asofdate desc'
		--done
		when @CompUniverse = 'CompCapStructure' then
			N'SELECT AsOfDate		
			, ' + @Securities + '
			, CompositeReturn
			FROM 
			(select d.CUSIP
				, d.AsOfDate
				, d.Perf
				, c.CompositeReturn
			from CustomIndex.vSecuritiesStrategy s
			inner join CustomIndex.vAPXSecurityPerformance d on s.CUSIP = d.Cusip
			inner join CustomIndex.fReturnsComposite(''' + @CompSpecific + ''',''' + convert(nvarchar(10),@StartDate,101) + ''',''' + convert(nvarchar(10),@EndDate,101) + ''') c on d.AsOfDate = c.AsOfDate 
			where d.AsOfDate between ''' + convert(nvarchar(10),@StartDate,101) + ''' and ''' + convert(nvarchar(10),@EndDate,101) + '''
			and s.PortfolioCompositeCode = ''' + @SecSpecific + '''
			) p
			PIVOT
				(MAX(p.Perf)
				FOR p.cusip IN (' + @Securities + ')) AS pvt order by asofdate desc'	
		end
end
else
begin
	SET @query = 
		case	
		when @CompUniverse = 'CompMLIndex' then
			N'SELECT AsOfDate		
			, ' + @Securities + '
			, IndexReturn
			FROM 
			(select d.CUSIP
				, d.AsOfDate
				, d.TotalReturn
				, c.TotalReturn as IndexReturn
			from CustomIndex.vSecuritiesIndexOptions s
			inner join CustomIndex.vSecurityReturn d on s.CUSIP = d.CUSIP
			inner join CustomIndex.fReturnsIndex(''' + convert(nvarchar(10),@StartDate,101) + ''',''' + convert(nvarchar(10),@EndDate,101) + ''') c on d.AsOfDate = c.AsOfDate 
			where d.AsOfDate between ''' + convert(nvarchar(10),@StartDate,101) + ''' and ''' + convert(nvarchar(10),@EndDate,101) + '''
			' + case when @SecUniverse = 'SecCapStructure' then 'and s.ticker = ''' + @SecSpecific + ''''  
					when @SecUniverse = 'SecSectorL2' then 'and s.SectorL2 = ''' + @SecSpecific + ''''  
					when @SecUniverse = 'SecSectorL3' then 'and s.SectorL3 = ''' + @SecSpecific + ''''  
					when @SecUniverse = 'SecSectorL4' then 'and s.SectorL4 = ''' + @SecSpecific + '''' 
					when @SecUniverse = 'SecMLIndex' then '' 
					when @SecUniverse = 'SecTickerList' then 'and s.CUSIP in (' + CustomIndex.fGetSecuritiesTickerList(@SecSpecific) + ')' end 
			+ '	) p
			PIVOT
				(MAX(p.totalreturn)
				FOR p.cusip IN (' + @Securities + ')) AS pvt order by asofdate desc'
		when @CompUniverse in ('CompStrategy','CompAccount') then
			N'SELECT AsOfDate		
			, ' + @Securities + '
			, IRR
			FROM 
			(select d.CUSIP
				, d.AsOfDate
				, d.TotalReturn
				, c.IRR
			from CustomIndex.vSecuritiesIndexOptions s
			inner join CustomIndex.vSecurityReturn d on s.CUSIP = d.CUSIP
			inner join CustomIndex.fReturnsPortfolio(''' + @CompSpecific + ''',''' + convert(nvarchar(10),@StartDate,101) + ''',''' + convert(nvarchar(10),@EndDate,101) + ''') c on d.AsOfDate = c.AsOfDate 
			where d.AsOfDate between ''' + convert(nvarchar(10),@StartDate,101) + ''' and ''' + convert(nvarchar(10),@EndDate,101) + '''
			' + case when @SecUniverse = 'SecCapStructure' then 'and s.ticker = ''' + @SecSpecific + ''''  
					when @SecUniverse = 'SecSectorL2' then 'and s.SectorL2 = ''' + @SecSpecific + ''''  
					when @SecUniverse = 'SecSectorL3' then 'and s.SectorL3 = ''' + @SecSpecific + ''''  
					when @SecUniverse = 'SecSectorL4' then 'and s.SectorL4 = ''' + @SecSpecific + '''' 
					when @SecUniverse = 'SecMLIndex' then '' 
					when @SecUniverse = 'SecTickerList' then 'and s.CUSIP in (' + CustomIndex.fGetSecuritiesTickerList(@SecSpecific) + ')' end  
			+ '	) p
			PIVOT 
				(MAX(p.totalreturn)
				FOR p.cusip IN (' + @Securities + ')) AS pvt order by asofdate desc'
		when @CompUniverse = 'CompCapStructure' then
			N'SELECT AsOfDate		
			, ' + @Securities + '
			, CompositeReturn
			FROM 
			(select d.CUSIP
				, d.AsOfDate
				, d.TotalReturn
				, c.CompositeReturn
			from CustomIndex.vSecuritiesIndexOptions s
			inner join CustomIndex.vSecurityReturn d on s.CUSIP = d.CUSIP
			inner join CustomIndex.fReturnsComposite(''' + @CompSpecific + ''',''' + convert(nvarchar(10),@StartDate,101) + ''',''' + convert(nvarchar(10),@EndDate,101) + ''') c on d.AsOfDate = c.AsOfDate 
			where d.AsOfDate between ''' + convert(nvarchar(10),@StartDate,101) + ''' and ''' + convert(nvarchar(10),@EndDate,101) + '''
			' + case when @SecUniverse = 'SecCapStructure' then 'and s.ticker = ''' + @SecSpecific + ''''  
					when @SecUniverse = 'SecSectorL2' then 'and s.SectorL2 = ''' + @SecSpecific + ''''  
					when @SecUniverse = 'SecSectorL3' then 'and s.SectorL3 = ''' + @SecSpecific + ''''  
					when @SecUniverse = 'SecSectorL4' then 'and s.SectorL4 = ''' + @SecSpecific + '''' 
					when @SecUniverse = 'SecMLIndex' then '' 
					when @SecUniverse = 'SecTickerList' then 'and s.CUSIP in (' + CustomIndex.fGetSecuritiesTickerList(@SecSpecific) + ')' end  
			+ '	) p
			PIVOT
				(MAX(p.totalreturn)
				FOR p.cusip IN (' + @Securities + ')) AS pvt order by asofdate desc'	
		end
end


print @query
EXECUTE (@query)      


END



GO
