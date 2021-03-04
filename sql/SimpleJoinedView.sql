USE [InvestmentEngine]
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE VIEW [CustomScoring].[vSchema1SecAllScoreDetails]

AS
	select s.Ticker
		, f.CompanyName
		, f.ICBSubsector
		, isnull(sect.ICBSector,'N/A') as ICBSector
		, f.MarketCap
		, f.MarketCapCategory
		, f.Style
		, s.TotalScore
		, ROW_NUMBER() over (order by totalscore desc) as Ranking
		, c.ValuationScore
		, f.BestPEForScoring
		, f.BestPEScore
		, f.PCF
		, f.PCFScore
		, f.PB
		, f.PBScore
		, c.GrowthScore
		, f.LTMEPSGrowth
		, f.LTMEPSGrowthScore
		, f.EPS5YrCAGR
		, f.EPS5YrCAGRScore
		, f.EPS10YrCAGR
		, f.EPS10YrCAGRScore
		, c.OpRiskScore
		, f.PSS3Yr
		, f.PSS3YrScore
		, f.PSS5Yr
		, f.PSS5YrScore
		, f.PSS10Yr
		, f.PSS10YrScore
		, c.FinRiskScore
		, f.DA
		, f.DAScore
		, f.DA10YrAvg
		, f.DAAvg10YrScore
	from CustomScoring.vSchema1SecTotalScores s
	left join CustomScoring.vSchema1SecCatDetailedScores c on s.Ticker = c.Ticker
	left join CustomScoring.vSchema1SecFieldDetailedScores f on s.Ticker = f.Ticker
	left join CustomGHPIA.ICBSectorCrossRef sect on f.ICBSubsector = sect.ICBSubsector
	--order by TotalScore desc
















GO


