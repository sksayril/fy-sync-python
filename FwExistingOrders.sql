-- EXEC [dbo].[Med_MU_Insert_SO]
--                 @ValidationKey = N'N5ske9cBVjlVfgmQ9Vp2iDrW3NGQZfz3SSWviwCe7GFzSwzLLODhCU7/2THg',
--                 @LedId_Party = 1000,
--                 @OrderedBy = 'Fastwhistle',
--                 @OrderNo = '#PRD-SUR-78232',
--                 @OrderDate = '2024-11-13',
--                 @IssDate = '2024-11-13',
--                 @Note = '',
--                 @CompanyId = 1,
--                 @Patient = NULL,
--                 @Doctor = NULL,
--                 @ImpDateTime = '2024-11-13',
--                 @DespId = '',
--                 @YearId = 7,
--                 @LedId_SP = '',
                
--                     @BrchId = '',
--                     @ItemNo = 0,
--                     @ItemDetailId = 1751,
--                     @Qty = 1,
--                     @Free = 0,
--                     @SORemark = NULL,
--                         @Sch_Per = '',
--                 	    @Sch_Rate = '';

SELECT TOP (1000) [Settings_ID]
      ,[Comp_Year]
      ,[Comp_No]
      ,[Setting_Name]
      ,[Setting_Effect]
  FROM [Bonny24255].[dbo].[COMPANYSETTINGS]

