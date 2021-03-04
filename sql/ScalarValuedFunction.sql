USE [APXFirm]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER FUNCTION [CustomCorpAction].[fGetNextDate](
      @AsOfDate datetime
) RETURNS datetime
AS
BEGIN
      
      DECLARE @NextBusDay datetime
      
      SELECT @NextBusDay = DATEADD(DAY, 1, @AsOfDate)
            
      WHILE DATEPART(WEEKDAY, @NextBusDay) IN (1, 7)
                  OR EXISTS(SELECT 1 FROM AdvApp.vHoliday WHERE HolidayDate = @NextBusDay and holidaytypeid=3)
            SELECT @NextBusDay = DATEADD(DAY, 1, @NextBusDay)
      
      RETURN @NextBusDay

END
