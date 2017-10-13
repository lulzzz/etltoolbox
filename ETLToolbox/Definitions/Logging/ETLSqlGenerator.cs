//using System;
//using System.Collections.Generic;
//using System.Linq;

//namespace ALE.ETLTools
//{
//    internal static class ETLSqlGenerator
//    {
//        internal static string StartETLProcess(int hkProcessID, string processType, Parameter parameter)
//        {
//            return $@"
//begin transaction
//  -- Init Variables
//  declare @lastParentProcessKey int = null
//  declare @processKey int = 0
//  declare @IsChildProcess bit = 0
  
//  select @lastParentProcessKey = ParentProcessKey
//  from biz.etlLoadProcess
//  where IsLastParentProcess = 1
  
//  update biz.etlLoadProcess
//  set IsLastProcess = 0
//  where IsLastProcess = 1
  
//  -- Create entry in etlLoadProcess
//  insert into biz.etlLoadProcess(
//   HKProcessID
//  , SSISExecutionID
//  , StartDate
//  , ProcessType
//  , ProcessName
//  , ProcessStep
//  , IsLastProcess
//  , ProcessCategory
//  , ProcessCategory2
//  , ProcessCategory3
//  , IsCompleted
//  , IsLastCompleted
//  , IsCopiedIntoRAW
//  , IsLastCopiedIntoRAW
//  )
//  select {hkProcessID}
//  , 0
//  , getdate()
//  , cast(N'{processType}' as nvarchar(50))
//  , cast(N'{parameter.ProcessName}' as nvarchar(50))
//  , cast(N'{parameter.ProcessStep}' as nvarchar(50))
//  , 1 as IsLastProcess
//  , cast(N'{parameter.ProcessCategory}' as nvarchar(50))
//  , cast(N'{parameter.ProcessCategory2}' as nvarchar(50))
//  , cast(N'{parameter.ProcessCategory3}' as nvarchar(50))
//  , 0 as IsCompleted
//  , 0 as IsLastCompleted
//  , 0 as IsCopiedIntoRAW
//  , 0 as IsLastCopiedIntoRAW
  
//  select @processKey = SCOPE_IDENTITY()
  
//  --update child/parent flags
//  set @IsChildProcess = {GetChildValue(parameter.ProcessChild)}
  
//  if (@IsChildProcess = 1 and @lastParentProcessKey is not null) 
//  begin
//      update biz.etlLoadProcess
//      set ParentProcessKey = @lastParentProcessKey,IsLastParentProcess = 0
//      where LoadProcessKey = @processKey
//  end
//  else
//  begin
//      update biz.etlLoadProcess
//      set ParentProcessKey = @processKey ,IsLastParentProcess = 1
//      where LoadProcessKey = @processKey
  
//      update biz.etlLoadProcess
//      set IsLastParentProcess = 0
//      where LoadProcessKey = @lastParentProcessKey
//  end
  
//  --return newly created process key
//  select @processKey

//commit;
//";

//        }

//        internal static string ReadLoadProcess(int processKey)
//        {
//            return $@"select LoadProcessKey, HKProcessID, SSISExecutionID, ParentProcessKey
//, StartDate, EndDate, ProcessType, ProcessName, ProcessStep
//, IsLastProcess, IsLastParentProcess, IsCompleted, IsLastCompleted
//, ProcessCategory, ProcessCategory2, ProcessCategory3
//, CopiedIntoRAWDate, IsCopiedIntoRAW, IsLastCopiedIntoRAW
//from biz.etlLoadProcess
//where LoadProcessKey = {processKey}
//";
//        }

//        internal static string WriteParameter(List<KeyValuePair<string, string>> keyValueList, int processKey)
//        {
//            string values = String.Join(",",
//                keyValueList.Select(keyValue => $"(N'{keyValue.Key}',N'{keyValue.Value}',{processKey})"));
//            return $@"insert into biz.etlParameter
//(Name, Value, LoadProcessKey)
//values {values}";
//        }

//        internal static string ReadLastCompletedProcessKey()
//        {
//            return $@"select LoadProcessKey
//from biz.etlLoadProcess
//where IsLastCompleted = 1
//";
//        }

//        internal static string ReadLastCopiedIntoRAWProcessKey()
//        {
//            return $@"select LoadProcessKey
//from biz.etlLoadProcess
//where IsLastCopiedIntoRAW = 1
//";
//        }

//        internal  static string GetChildValue(bool value) => value ? "1" : "0";

//        internal static string SuccesfullyCopiedIntoRAW(int processKey)
//        {
//            return $@"
//begin transaction 
//  update [biz].[etlLoadProcess]
//  set IsLastCopiedIntoRAW = 0
//  where IsLastCopiedIntoRAW = 1
  
//  update [biz].[etlLoadProcess]
//  set CopiedIntoRAWDate = getdate()
//  ,IsCopiedIntoRAW = 1
//  ,IsLastCopiedIntoRAW = 1
//  where LoadProcessKey = {processKey}
//commit;";
//        }

//        internal static string EndETLProcess(int processKey)
//        {
//            return $@"
//begin transaction 
//  update [biz].[etlLoadProcess]
//  set IsLastCompleted = 0
//  where IsLastCompleted = 1
  
//  update [biz].[etlLoadProcess]
//  set EndDate = getdate()
//  ,IsCompleted = 1
//  ,IsLastCompleted = 1
//  where LoadProcessKey = {processKey}
//commit;
//";

//        }

//        internal static string CleanUpETLLog(int daysToKeep)
//        {
//            string sql = $@"
//delete from etllog
// from biz.etlLog etllog
// inner join biz.etlLoadProcess process
//  on process.LoadProcessKey = etllog.LoadProcessKey
//where process.StartDate < Dateadd(day,-{daysToKeep},GETDATE())

//delete from par
// from biz.etlParameter par
// inner join biz.etlLoadProcess process
//  on process.LoadProcessKey = par.LoadProcessKey
// where process.StartDate < Dateadd(day,-{daysToKeep},GETDATE())
//";

//            return sql;
//        }

//    }
//}
