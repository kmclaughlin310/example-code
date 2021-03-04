USE [APXFirm]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [CustomCorpAction].[pPopulatePostingsTbl]
	@CUSIP NCHAR(9)
	, @CorpActionID INT
	, @DecisionID INT
AS
BEGIN



DECLARE @CustodianID INT
	, @PortfolioID INT
	, @Quantity FLOAT
	, @AffectedQuant FLOAT
	, @EstimatedQuant BIT
	, @Strategy VARCHAR(4)
	
DECLARE db_cursor CURSOR FOR


SELECT c.CustodianID, p.PortfolioID, SUM(p.Quantity), ca.EstPercentageCall*SUM(p.Quantity), RIGHT(p.PortfolioCode,4)
FROM AdvApp.vPortfolioTaxLotCurrent p 
LEFT JOIN AdvApp.vCustPortfolioCustodian c ON p.PortfolioID=c.PortfolioID 
INNER JOIN CustomCorpAction.CorpActions ca ON p.Symbol = ca.CUSIP AND ca.CorpActionID = @CorpActionID
WHERE p.Symbol = @CUSIP
GROUP BY c.CustodianID, p.PortfolioID, ca.EstPercentageCall, p.PortfolioCode


OPEN db_cursor 
FETCH NEXT FROM db_cursor INTO @CustodianID, @PortfolioID, @Quantity, @AffectedQuant, @Strategy

WHILE @@FETCH_STATUS = 0
BEGIN
	IF @AffectedQuant IS NOT NULL
		SET @EstimatedQuant = 1
	ELSE
		SET @EstimatedQuant = 0
	
	IF @DecisionID NOT IN (2,3) AND @CustodianID IS NOT NULL		
		INSERT INTO CustomCorpAction.Postings VALUES (@CorpActionID, @CustodianID, @PortfolioID, @Quantity, @AffectedQuant, NULL, NULL, @EstimatedQuant)
	ELSE
	BEGIN
		IF @DecisionID = 2 AND @Strategy = 'SDHY' AND @CustodianID IS NOT NULL
			INSERT INTO CustomCorpAction.Postings VALUES (@CorpActionID, @CustodianID, @PortfolioID, @Quantity, @AffectedQuant, NULL, NULL, @EstimatedQuant)
		IF @DecisionID = 3 AND @Strategy = 'CORE' AND @CustodianID IS NOT NULL
			INSERT INTO CustomCorpAction.Postings VALUES (@CorpActionID, @CustodianID, @PortfolioID, @Quantity, @AffectedQuant, NULL, NULL, @EstimatedQuant)
	END
	
	FETCH NEXT FROM db_cursor INTO @CustodianID, @PortfolioID, @Quantity, @AffectedQuant, @Strategy
END

CLOSE db_cursor
DEALLOCATE db_cursor
	
	
	
	
	
	
END
