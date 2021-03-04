USE [InvestmentEngine]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [CustomGHPIA].[pProcessDailyEquityBMViolations] (@AsOfDate datetime)
AS
BEGIN
	declare cur cursor for select * from CustomGHPIA.EquityBenchmark
	open cur
	
	declare @EquityBenchmarkID as int
	declare @FieldID as int
	declare @EquityStyle as int
	declare @BMVal as float
	declare @ThresholdType as int
	declare @ThresholdVal as float
	
	fetch next from cur into @EquityBenchmarkID, @FieldID, @EquityStyle, @BMVal, @ThresholdType, @ThresholdVal
	
	while (@@FETCH_STATUS = 0)	
	begin
		if @ThresholdType = 1 or @ThresholdType = 4 or @ThresholdType = 14 --more than x% variance above bm, more than x above bm, more than threshold
			insert into CustomGHPIA.EquityBenchmarkViolation
				select @EquityBenchmarkID
					, @AsOfDate
					, sec.SecurityID
					, v.FloatValue
					, 1 --status id for 'active'
					, null --violation ignore until date
					, null --violation ignore until value
					, null --violation resolution date
					, null --notes
					, GETDATE()
					, 10 --userid for automated process
				from CustomGHPIA.BuyList buy
				left join CustomBloomberg.Security sec on buy.Ticker = sec.Ticker
				left join CustomBloomberg.FieldValue v on sec.SecurityID = v.SecurityID
				inner join CustomBloomberg.Field f on v.FieldID = f.FieldID
				left join lnkPortfolioCenter.PortfolioCenter.Custom.vSecurityDetails details on buy.Ticker = details.Symbol
				where AsOfDate = @AsOfDate
				and v.FieldID = @FieldID
				and buy.Status in (1,2)
				and details.Sector = (case when @EquityStyle > 0 then (select EquityStyleDescription from CustomGHPIA.EquityStyleType where EquityStyleTypeID = @EquityStyle) else details.Sector end)
				and isnull(v.FloatValue, 100) > (case @ThresholdType when 1 then @BMVal * (1 + @ThresholdVal / 100.0) when 4 then @BMVal + @ThresholdVal when 14 then @ThresholdVal end)
				and v.FloatValue is not null
				and not exists (select 1 from CustomGHPIA.EquityBenchmarkViolation where EquityBenchmarkID = @EquityBenchmarkID and BloombergSecurityID = sec.SecurityID and ViolationStatusID in (1,2))
		else if @ThresholdType = 2 or @ThresholdType = 5 or @ThresholdType = 13 --more than x% variance below bm, more than x below bm, less than threshold
			insert into CustomGHPIA.EquityBenchmarkViolation
				select @EquityBenchmarkID
					, @AsOfDate
					, sec.SecurityID
					, v.FloatValue
					, 1 --status id for 'active'
					, null --violation ignore until date
					, null --violation ignore until value
					, null --violation resolution date
					, null --notes
					, GETDATE()
					, 10 --userid for automated process
				from CustomGHPIA.BuyList buy
				left join CustomBloomberg.Security sec on buy.Ticker = sec.Ticker
				left join CustomBloomberg.FieldValue v on sec.SecurityID = v.SecurityID
				inner join CustomBloomberg.Field f on v.FieldID = f.FieldID
				left join lnkPortfolioCenter.PortfolioCenter.Custom.vSecurityDetails details on buy.Ticker = details.Symbol
				where AsOfDate = @AsOfDate
				and v.FieldID = @FieldID
				and buy.Status in (1,2)
				and details.Sector = (case when @EquityStyle > 0 then (select EquityStyleDescription from CustomGHPIA.EquityStyleType where EquityStyleTypeID = @EquityStyle) else details.Sector end)
				and isnull(v.FloatValue, 100) < (case @ThresholdType when 2 then @BMVal * (1 + @ThresholdVal / 100.0) when 5 then @BMVal + @ThresholdVal when 13 then @ThresholdVal end)
				and v.FloatValue is not null
				and not exists (select 1 from CustomGHPIA.EquityBenchmarkViolation where EquityBenchmarkID = @EquityBenchmarkID and BloombergSecurityID = sec.SecurityID and ViolationStatusID in (1,2))
		else if @ThresholdType = 3 or @ThresholdType = 6 --more than x% variance above or below bm, more than x above or below bm
			insert into CustomGHPIA.EquityBenchmarkViolation
				select @EquityBenchmarkID
					, @AsOfDate
					, sec.SecurityID
					, v.FloatValue
					, 1 --status id for 'active'
					, null --violation ignore until date
					, null --violation ignore until value
					, null --violation resolution date
					, null --notes
					, GETDATE()
					, 10 --userid for automated process
				from CustomGHPIA.BuyList buy
				left join CustomBloomberg.Security sec on buy.Ticker = sec.Ticker
				left join CustomBloomberg.FieldValue v on sec.SecurityID = v.SecurityID
				inner join CustomBloomberg.Field f on v.FieldID = f.FieldID
				left join lnkPortfolioCenter.PortfolioCenter.Custom.vSecurityDetails details on buy.Ticker = details.Symbol
				where AsOfDate = @AsOfDate
				and v.FieldID = @FieldID
				and buy.Status in (1,2)
				and details.Sector = (case when @EquityStyle > 0 then (select EquityStyleDescription from CustomGHPIA.EquityStyleType where EquityStyleTypeID = @EquityStyle) else details.Sector end)
				and (isnull(v.FloatValue, 100) > (case @ThresholdType when 3 then @BMVal * (1 + @ThresholdVal / 100.0) else @BMVal + @ThresholdVal end)
					or isnull(v.FloatValue, 100) < (case @ThresholdType when 3 then @BMVal * (1 + @ThresholdVal / 100.0) else @BMVal + @ThresholdVal end))
				and v.FloatValue is not null
				and not exists (select 1 from CustomGHPIA.EquityBenchmarkViolation where EquityBenchmarkID = @EquityBenchmarkID and BloombergSecurityID = sec.SecurityID and ViolationStatusID in (1,2))
		else if @ThresholdType = 7 or @ThresholdType = 10 ---subsector is fieldid 8; more than x% above subsector median, more than x above subsector median
			insert into CustomGHPIA.EquityBenchmarkViolation
				select @EquityBenchmarkID
					, @AsOfDate
					, sec.SecurityID
					, v.FloatValue
					, 1 --status id for 'active'
					, null --violation ignore until date
					, null --violation ignore until value
					, null --violation resolution date
					, null --notes
					, GETDATE()
					, 10 --userid for automated process
				from CustomGHPIA.BuyList buy
				left join CustomBloomberg.Security sec on buy.Ticker = sec.Ticker
				left join CustomBloomberg.FieldValue v on sec.SecurityID = v.SecurityID
				inner join CustomBloomberg.Field f on v.FieldID = f.FieldID
				left join lnkPortfolioCenter.PortfolioCenter.Custom.vSecurityDetails details on buy.Ticker = details.Symbol
				left join CustomBloomberg.FieldValue subsector on sec.SecurityID = subsector.SecurityID and subsector.FieldID = 8 and subsector.AsOfDate = @AsOfDate
				where v.AsOfDate = @AsOfDate
				and v.FieldID = @FieldID
				and buy.Status in (1,2)
				and isnull(v.FloatValue, 100) > (select Value from CustomGHPIA.CalculatedValues cv where BloombergFieldID = @FieldID and CalcTypeID = 1 and GroupTypeID = 2 and GroupDescription = subsector.TextValue and cv.AsOfDate = @AsOfDate)
												* (case when @ThresholdType = 7 then (1 + @ThresholdVal / 100.0) else 1 end)							
				and v.FloatValue is not null
				and not exists (select 1 from CustomGHPIA.EquityBenchmarkViolation where EquityBenchmarkID = @EquityBenchmarkID and BloombergSecurityID = sec.SecurityID and ViolationStatusID in (1,2))
		else if @ThresholdType = 8 or @ThresholdType = 11 ---subsector is fieldid 8; more than x% below subsector median, more than x below subsector median
			insert into CustomGHPIA.EquityBenchmarkViolation
				select @EquityBenchmarkID
					, @AsOfDate
					, sec.SecurityID
					, v.FloatValue
					, 1 --status id for 'active'
					, null --violation ignore until date
					, null --violation ignore until value
					, null --violation resolution date
					, null --notes
					, GETDATE()
					, 10 --userid for automated process
				from CustomGHPIA.BuyList buy
				left join CustomBloomberg.Security sec on buy.Ticker = sec.Ticker
				left join CustomBloomberg.FieldValue v on sec.SecurityID = v.SecurityID
				inner join CustomBloomberg.Field f on v.FieldID = f.FieldID
				left join lnkPortfolioCenter.PortfolioCenter.Custom.vSecurityDetails details on buy.Ticker = details.Symbol
				left join CustomBloomberg.FieldValue subsector on sec.SecurityID = subsector.SecurityID and subsector.FieldID = 8 and subsector.AsOfDate = @AsOfDate
				where v.AsOfDate = @AsOfDate
				and v.FieldID = @FieldID
				and buy.Status in (1,2)
				and isnull(v.FloatValue, 100) < (select Value from CustomGHPIA.CalculatedValues cv where BloombergFieldID = @FieldID and CalcTypeID = 1 and GroupTypeID = 2 and GroupDescription = subsector.TextValue and cv.AsOfDate = @AsOfDate)
												* (case when @ThresholdType = 8 then (1 - @ThresholdVal / 100.0) else 1 end)
				and v.FloatValue is not null
				and not exists (select 1 from CustomGHPIA.EquityBenchmarkViolation where EquityBenchmarkID = @EquityBenchmarkID and BloombergSecurityID = sec.SecurityID and ViolationStatusID in (1,2))
		else if @ThresholdType = 9 or @ThresholdType = 12 ---subsector is fieldid 8; more than x% above or below subsector median, more than x above or below subsector median
			insert into CustomGHPIA.EquityBenchmarkViolation
				select @EquityBenchmarkID
					, @AsOfDate
					, sec.SecurityID
					, v.FloatValue
					, 1 --status id for 'active'
					, null --violation ignore until date
					, null --violation ignore until value
					, null --violation resolution date
					, null --notes
					, GETDATE()
					, 10 --userid for automated process
				from CustomGHPIA.BuyList buy
				left join CustomBloomberg.Security sec on buy.Ticker = sec.Ticker
				left join CustomBloomberg.FieldValue v on sec.SecurityID = v.SecurityID
				inner join CustomBloomberg.Field f on v.FieldID = f.FieldID
				left join lnkPortfolioCenter.PortfolioCenter.Custom.vSecurityDetails details on buy.Ticker = details.Symbol
				left join CustomBloomberg.FieldValue subsector on sec.SecurityID = subsector.SecurityID and subsector.FieldID = 8 and subsector.AsOfDate = @AsOfDate
				where v.AsOfDate = @AsOfDate
				and v.FieldID = @FieldID
				and buy.Status in (1,2)
				and (isnull(v.FloatValue, 100) > (select Value from CustomGHPIA.CalculatedValues cv where BloombergFieldID = @FieldID and CalcTypeID = 1 and GroupTypeID = 2 and GroupDescription = subsector.TextValue and cv.AsOfDate = @AsOfDate)
												* (case when @ThresholdType = 9 then (1 + @ThresholdVal / 100.0) else 1 end)							
					or isnull(v.FloatValue, 100) < (select Value from CustomGHPIA.CalculatedValues cv where BloombergFieldID = @FieldID and CalcTypeID = 1 and GroupTypeID = 2 and GroupDescription = subsector.TextValue and cv.AsOfDate = @AsOfDate)
												* (case when @ThresholdType = 9 then (1 - @ThresholdVal / 100.0) else 1 end))
				and v.FloatValue is not null
				and not exists (select 1 from CustomGHPIA.EquityBenchmarkViolation where EquityBenchmarkID = @EquityBenchmarkID and BloombergSecurityID = sec.SecurityID and ViolationStatusID in (1,2))
	
		fetch next from cur into @EquityBenchmarkID, @FieldID, @EquityStyle, @BMVal, @ThresholdType, @ThresholdVal
	end
	close cur
	deallocate cur
	
	insert into CustomGHPIA.ProcRunLog values ('CustomGHPIA.pProcessDailyEquityBMViolations',GETDATE())
	
END

