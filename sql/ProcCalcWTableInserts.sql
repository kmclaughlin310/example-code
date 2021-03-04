USE [InvestmentEngine]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [CustomGHPIA].[pCalculateDailySectorMetrics] (@AsOfDate datetime)
AS
BEGIN
	declare @localAsOfDate datetime = @AsOfDate
	
	--table for calculating medians
	declare @medianRows table (ICBSubsector nvarchar(150), peC int, pcfC int, pbC int, daC int, pssC int, trrC int, mktCapC int)
	insert into @medianRows 
		select ICBSubSector, peC=COUNT(PE), pcfC=COUNT(PCF), pbC=COUNT(PB), daC=COUNT(DA), pssC=COUNT(PSS), trrC=COUNT(OneYrReturn), mktCapC=COUNT(MarketCap)
		from CustomBloomberg.vRussell3kFieldData where AsOfDate = @localAsOfDate
		group by ICBSubSector
	
	--P/E	
	insert into CustomGHPIA.CalculatedValues
		select 2, ICBSubSector = ICBSubSector, 1, 2, MedianPE = AVG(0.+PE), @localAsOfDate
		from 
		(
			SELECT a.ICBSubSector, PE
			FROM 
			(
				SELECT ICBSubSector, c1=(peC+1)/2, c2=CASE peC%2 WHEN 0 THEN 1+peC/2 ELSE (peC+1)/2 END
				FROM @medianRows
			) a
			JOIN 
			(
				SELECT ICBSubSector, PE
					,rn=ROW_NUMBER() OVER (PARTITION BY ICBSubSector ORDER BY PE)
				FROM CustomBloomberg.vRussell3kFieldData b
				where AsOfDate = @localAsOfDate
				and PE is not null
			) b
			on a.ICBSubSector = b.ICBSubSector and b.rn between a.c1 and a.c2
		) a
		group by ICBSubSector
	
	--P/CF
	insert into CustomGHPIA.CalculatedValues
		select 2, ICBSubSector = ICBSubSector, 1, 3, MedianPCF = AVG(0.+PCF), @localAsOfDate
		from 
		(
			SELECT a.ICBSubSector, PCF
			FROM 
			(
				SELECT ICBSubSector, c1=(pcfC+1)/2, c2=CASE pcfC%2 WHEN 0 THEN 1+pcfC/2 ELSE (pcfC+1)/2 END
				FROM @medianRows
			) a
			JOIN 
			(
				SELECT ICBSubSector, PCF
					,rn=ROW_NUMBER() OVER (PARTITION BY ICBSubSector ORDER BY PCF)
				FROM CustomBloomberg.vRussell3kFieldData b
				where AsOfDate = @localAsOfDate
				and PCF is not null
			) b
			on a.ICBSubSector = b.ICBSubSector and b.rn between a.c1 and a.c2
		) a
		group by ICBSubSector

	--P/B
	insert into CustomGHPIA.CalculatedValues
		select 2, ICBSubSector = ICBSubSector, 1, 4, MedianPB = AVG(0.+PB), @localAsOfDate
		from 
		(
			SELECT a.ICBSubSector, PB
			FROM 
			(
				SELECT ICBSubSector, c1=(pbC+1)/2, c2=CASE pbC%2 WHEN 0 THEN 1+pbC/2 ELSE (pbC+1)/2 END
				FROM @medianRows
			) a
			JOIN 
			(
				SELECT ICBSubSector, PB
					,rn=ROW_NUMBER() OVER (PARTITION BY ICBSubSector ORDER BY PB)
				FROM CustomBloomberg.vRussell3kFieldData b
				where AsOfDate = @localAsOfDate
				and PB is not null
			) b
			on a.ICBSubSector = b.ICBSubSector and b.rn between a.c1 and a.c2
		) a
		group by ICBSubSector
		

	--D/A
	insert into CustomGHPIA.CalculatedValues
		select 2, ICBSubSector = ICBSubSector, 1, 5, MedianDA = AVG(0.+DA), @localAsOfDate
		from 
		(
			SELECT a.ICBSubSector, DA
			FROM 
			(
				SELECT ICBSubSector, c1=(daC+1)/2, c2=CASE daC%2 WHEN 0 THEN 1+daC/2 ELSE (daC+1)/2 END
				FROM @medianRows
			) a
			JOIN 
			(
				SELECT ICBSubSector, DA
					,rn=ROW_NUMBER() OVER (PARTITION BY ICBSubSector ORDER BY DA)
				FROM CustomBloomberg.vRussell3kFieldData b
				where AsOfDate = @localAsOfDate
				and DA is not null
			) b
			on a.ICBSubSector = b.ICBSubSector and b.rn between a.c1 and a.c2
		) a
		group by ICBSubSector
		
	--PSS
	insert into CustomGHPIA.CalculatedValues 
		select 2, ICBSubSector = ICBSubSector, 1, 6, MedianPSS = AVG(0.+PSS), @localAsOfDate
		from 
		(
			SELECT a.ICBSubSector, PSS
			FROM 
			(
				SELECT ICBSubSector, c1=(pssC+1)/2, c2=CASE pssC%2 WHEN 0 THEN 1+pssC/2 ELSE (pssC+1)/2 END
				FROM @medianRows
			) a
			JOIN 
			(
				SELECT ICBSubSector, PSS
					,rn=ROW_NUMBER() OVER (PARTITION BY ICBSubSector ORDER BY PSS)
				FROM CustomBloomberg.vRussell3kFieldData b
				where AsOfDate = @localAsOfDate
				and PSS is not null
			) b
			on a.ICBSubSector = b.ICBSubSector and b.rn between a.c1 and a.c2
		) a
		group by ICBSubSector

	
	--1-yr TRR
	insert into CustomGHPIA.CalculatedValues 
		select 2, ICBSubSector = ICBSubSector, 1, 9, MedianTRR = AVG(0.+OneYrReturn), @localAsOfDate
		from 
		(
			SELECT a.ICBSubSector, OneYrReturn
			FROM 
			(
				SELECT ICBSubSector, c1=(trrC+1)/2, c2=CASE trrC%2 WHEN 0 THEN 1+trrC/2 ELSE (trrC+1)/2 END
				FROM @medianRows
			) a
			JOIN 
			(
				SELECT ICBSubSector, OneYrReturn
					,rn=ROW_NUMBER() OVER (PARTITION BY ICBSubSector ORDER BY OneYrReturn)
				FROM CustomBloomberg.vRussell3kFieldData b
				where AsOfDate = @localAsOfDate
				and OneYrReturn is not null
			) b
			on a.ICBSubSector = b.ICBSubSector and b.rn between a.c1 and a.c2
		) a
		group by ICBSubSector
		
	--market cap
	insert into CustomGHPIA.CalculatedValues
		select 2 
			, ICBSubSector
			, 3
			, 1
			, SUM(data.MarketCap)
			, @localAsOfDate
		from CustomBloomberg.vRussell3kFieldData data
		where AsOfDate = @localAsOfDate
		and ICBSubSector is not null
		and ICBSubSector not in (select q.ICBSubSector from (
									select SUM(MarketCap) as mktcap, ICBSubSector
									from CustomBloomberg.vRussell3kFieldData 
									where AsOfDate = @localAsOfDate
									group by ICBSubSector
									having SUM(MarketCap) is null) q) 
		group by ICBSubSector
		order by ICBSubSector
		
		
	
	--do insert for CustomGHPIA.ProcRunLog
	insert into CustomGHPIA.ProcRunLog values ('CustomGHPIA.pCalculateDailySectorMetrics',GETDATE())
	
END


