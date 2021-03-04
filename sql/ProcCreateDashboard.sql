USE [InvestmentEngine]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






CREATE PROCEDURE [CustomGHPIA].[pSubsectorValuationsDashboard_ToTable]  --defaults to previous business day and excluding unamanged accounts
AS
	
	--can be change to pass in asofdate and portfolioid
	declare @AsOfDate nvarchar(10)
	set @AsOfDate = convert(nvarchar(10),(select CustomGHPIA.fGetPrevBusinessDay(GETDATE())),101)
	declare @OwningPortfolioID int
	set @OwningPortfolioID = -1 --ex unmanaged accounts
	
	--create table for GHPIA holdings
	declare @temp table (Ticker nvarchar(100), SecurityDesc nvarchar(500), Marketvalue float)		
	declare @qryString nvarchar(max)
	set @qryString = 'select * from openquery(lnkPortfolioCenter,''select * from PortfolioCenter.Custom.fGetPCEquityPortfolioStatement(''''' + @AsOfDate + ''''',' + CONVERT(varchar(5),@OwningPortfolioID) + ')'')'
	insert into @temp exec (@qryString)
	
	--table for calculating medians
	declare @medianRows table (ICBSubsector nvarchar(150), peC int, pcfC int, pbC int, daC int, pssC int, trrC int)
	insert into @medianRows 
		select ICBSubSector, peC=COUNT(PE), pcfC=COUNT(PCF), pbC=COUNT(PB), daC=COUNT(DA), pssC=COUNT(PSS), trrC=COUNT(OneYrReturn)
		from CustomBloomberg.vAllSecuritiesFieldData where AsOfDate = @AsOfDate and Ticker in (select Ticker from @temp)
		group by ICBSubSector
	
	--create table for GHPIA median PEs
	declare @ghpiaMedianPEs table (ICBSubsector nvarchar(150), MedianPE float)
	insert into @ghpiaMedianPEs 
		select ICBSubSector = ICBSubSector, MedianPE = AVG(0.+PE)
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
				FROM CustomBloomberg.vAllSecuritiesFieldData b
				where AsOfDate = @AsOfDate and Ticker in (select Ticker from @temp)
				and PE is not null
			) b
			on a.ICBSubSector = b.ICBSubSector and b.rn between a.c1 and a.c2
		) a
		group by ICBSubSector

	--create table for GHPIA median PCFs
	declare @ghpiaMedianPCFs table (ICBSubsector nvarchar(150), MedianPCF float)
	insert into @ghpiaMedianPCFs 
		select ICBSubSector = ICBSubSector, MedianPCF = AVG(0.+PCF)
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
				FROM CustomBloomberg.vAllSecuritiesFieldData b
				where AsOfDate = @AsOfDate and Ticker in (select Ticker from @temp)
				and PCF is not null
			) b
			on a.ICBSubSector = b.ICBSubSector and b.rn between a.c1 and a.c2
		) a
		group by ICBSubSector
		
	--create table for GHPIA median PBs
	declare @ghpiaMedianPBs table (ICBSubsector nvarchar(150), MedianPB float)
	insert into @ghpiaMedianPBs 
		select ICBSubSector = ICBSubSector, MedianPB = AVG(0.+PB)
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
				FROM CustomBloomberg.vAllSecuritiesFieldData b
				where AsOfDate = @AsOfDate and Ticker in (select Ticker from @temp)
				and PB is not null
			) b
			on a.ICBSubSector = b.ICBSubSector and b.rn between a.c1 and a.c2
		) a
		group by ICBSubSector
		
	--create table for GHPIA median DAs
	declare @ghpiaMedianDAs table (ICBSubsector nvarchar(150), MedianDA float)
	insert into @ghpiaMedianDAs 
		select ICBSubSector = ICBSubSector, MedianDA = AVG(0.+DA)
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
				FROM CustomBloomberg.vAllSecuritiesFieldData b
				where AsOfDate = @AsOfDate and Ticker in (select Ticker from @temp)
				and DA is not null
			) b
			on a.ICBSubSector = b.ICBSubSector and b.rn between a.c1 and a.c2
		) a
		group by ICBSubSector
		
	--create table for GHPIA median PSSs
	declare @ghpiaMedianPSSs table (ICBSubsector nvarchar(150), MedianPSS float)
	insert into @ghpiaMedianPSSs 
		select ICBSubSector = ICBSubSector, MedianPE = AVG(0.+PSS)
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
				FROM CustomBloomberg.vAllSecuritiesFieldData b
				where AsOfDate = @AsOfDate and Ticker in (select Ticker from @temp)
				and PSS is not null
			) b
			on a.ICBSubSector = b.ICBSubSector and b.rn between a.c1 and a.c2
		) a
		group by ICBSubSector

	--create table for GHPIA median 1-year TRRs
	declare @ghpiaMedianTRRs table (ICBSubsector nvarchar(150), MedianTRR float)
	insert into @ghpiaMedianTRRs 
		select ICBSubSector = ICBSubSector, MedianTRR = AVG(0.+OneYrReturn)
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
				FROM CustomBloomberg.vAllSecuritiesFieldData b
				where AsOfDate = @AsOfDate and Ticker in (select Ticker from @temp)
				and OneYrReturn is not null
			) b
			on a.ICBSubSector = b.ICBSubSector and b.rn between a.c1 and a.c2
		) a
		group by ICBSubSector


	--wipe out table before doing inserts
	delete from CustomGHPIA.DailySubsectorValuationsDashboard
	
	insert into CustomGHPIA.DailySubsectorValuationsDashboard
		select russ.ICBSubSector
			, SUM(ghpia.Marketvalue) / (select SUM(Marketvalue) from @temp) * 100 as GHPIAWeight
			, COUNT(ghpia.Ticker) as GHPIANumSecs
			, russ.Russell3kWeight
			, COUNT(russCount.Ticker) as Russ3kNumSecs
			, russ.ViolationCount
				+ case when isnull(ghpiaPE.MedianPE,1000000) > russ.CurrentMedianPE * 1.33 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end
				+ case when isnull(ghpiaPCF.MedianPCF,1000000) > russ.CurrentMedianPCF * 1.33 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end
				+ case when isnull(ghpiaPB.MedianPB,1000000) > russ.CurrentMedianPB * 1.33 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end 
				+ case when isnull(ghpiaDA.MedianDA,1000000) > russ.CurrentMedianDA * 1.33 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end 
				+ case when isnull(ghpiaPSS.MedianPSS,-1000000) < russ.CurrentMedianPSS * .66 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end 
				+ case when isnull(ghpiaTRR.MedianTRR,1000000) > russ.CurrentMedian1YrTRR * 1.33 or isnull(ghpiaTRR.MedianTRR,-1000000) < -15 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end
				as ViolationCount
			, russ.ViolationCount as SubsectorViolationCount
			, case when isnull(ghpiaPE.MedianPE,1000000) > russ.CurrentMedianPE * 1.33 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end
				+ case when isnull(ghpiaPCF.MedianPCF,1000000) > russ.CurrentMedianPCF * 1.33 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end
				+ case when isnull(ghpiaPB.MedianPB,1000000) > russ.CurrentMedianPB * 1.33 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end 
				+ case when isnull(ghpiaDA.MedianDA,1000000) > russ.CurrentMedianDA * 1.33 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end 
				+ case when isnull(ghpiaPSS.MedianPSS,-1000000) < russ.CurrentMedianPSS * .66 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end 
				+ case when isnull(ghpiaTRR.MedianTRR,1000000) > russ.CurrentMedian1YrTRR * 1.33 or isnull(ghpiaTRR.MedianTRR,-1000000) < -15 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end
				as GHPIAViolationCount
			, russ.CurrentMedianPE
			, russ.TenYrMedianPE
			, russ.PEViolation
			, ghpiaPE.MedianPE as GHPIApeRatio
			, case when isnull(ghpiaPE.MedianPE,1000000) > russ.CurrentMedianPE * 1.33 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end as GHPIApefViolation
			, russ.CurrentMedianPCF
			, russ.TenYrMedianPCF
			, russ.PCFViolation
			, ghpiaPCF.MedianPCF as GHPIApcfRatio
			, case when isnull(ghpiaPCF.MedianPCF,1000000) > russ.CurrentMedianPCF * 1.33 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end as GHPIApcfViolation
			, russ.CurrentMedianPB
			, russ.TenYrMedianPB
			, russ.PBViolation
			, ghpiaPB.MedianPB as GHPIApbRatio	
			, case when isnull(ghpiaPB.MedianPB,1000000) > russ.CurrentMedianPB * 1.33 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end as GHPIApbViolation
			, russ.CurrentMedianDA
			, russ.TenYrMedianDebtToAssets
			, russ.DAViolation
			, ghpiaDA.MedianDA as GHPIAdaRatio	
			, case when isnull(ghpiaDA.MedianDA,1000000) > russ.CurrentMedianDA * 1.33 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end as GHPIAdaViolation
			, russ.CurrentMedianPSS
			, russ.TenYrMedianPSS 
			, russ.PSSViolation
			, ghpiaPSS.MedianPSS as GHPIApss	
			, case when isnull(ghpiaPSS.MedianPSS,-1000000) < russ.CurrentMedianPSS * .66 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end as GHPIApssViolation
			, russ.CurrentMedian1YrTRR
			, russ.TenYrMedian1YrReturn
			, russ.ReturnViolation
			, ghpiaTRR.MedianTRR as GHPIA1yrTrr	
			, case when isnull(ghpiaTRR.MedianTRR,1000000) > russ.CurrentMedian1YrTRR * 1.33 or isnull(ghpiaTRR.MedianTRR,-1000000) < -15 and COUNT(ghpia.Ticker) > 0 then 1 else 0 end as GHPIAtrrViolation	
			, russ.AsOfDate
		from CustomGHPIA.vSubsectorValuationDashboard russ
		left join CustomBloomberg.vAllSecuritiesFieldData sec on sec.ICBSubSector = russ.ICBSubSector and sec.AsOfDate = @AsOfDate
		left join @temp ghpia on sec.Ticker = ghpia.Ticker and ghpia.Marketvalue > 10
		LEFT join @ghpiaMedianPEs ghpiaPE on russ.ICBSubSector = ghpiaPE.ICBSubsector
		LEFT join @ghpiaMedianPCFs ghpiaPCF on russ.ICBSubSector = ghpiaPCF.ICBSubsector
		LEFT join @ghpiaMedianPBs ghpiaPB on russ.ICBSubSector = ghpiaPB.ICBSubsector
		LEFT join @ghpiaMedianPSSs ghpiaPSS on russ.ICBSubSector = ghpiaPSS.ICBSubsector
		LEFT join @ghpiaMedianDAs ghpiaDA on russ.ICBSubSector = ghpiaDA.ICBSubsector
		left join @ghpiaMedianTRRs ghpiaTRR on russ.ICBSubSector = ghpiaTRR.ICBSubsector
		left join CustomBloomberg.vRussell3kFieldData russCount on sec.Ticker = russCount.Ticker and russCount.AsOfDate = @AsOfDate
		group by russ.ICBSubSector, russ.Russell3kWeight, russ.ViolationCount, russ.CurrentMedianPE, russ.TenYrMedianPE, russ.PEViolation, russ.CurrentMedianPCF, russ.TenYrMedianPCF
			, russ.PCFViolation, russ.CurrentMedianPB, russ.TenYrMedianPB, russ.PBViolation, russ.CurrentMedianDA, russ.TenYrMedianDebtToAssets, russ.DAViolation, russ.CurrentMedianPSS
			, russ.TenYrMedianPSS, russ.PSSViolation, russ.CurrentMedian1YrTRR, russ.TenYrMedian1YrReturn, russ.ReturnViolation, russ.AsOfDate, ghpiaPE.MedianPE
			, ghpiaPB.MedianPB, ghpiaPCF.MedianPCF, ghpiaDA.MedianDA, ghpiaPSS.MedianPSS, ghpiaTRR.MedianTRR
		order by russ.ViolationCount desc, russ.ICBSubSector


		insert into CustomGHPIA.ProcRunLog values ('CustomGHPIA.pSubsectorValuationsDashboard_ToTable',GETDATE())



