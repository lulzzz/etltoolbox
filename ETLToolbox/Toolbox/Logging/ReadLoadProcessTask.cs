using System;
using System.Collections.Generic;

namespace ALE.ETLTools {
    public class ReadLoadProcessTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "LOADPROCESS_READ";
        public override string TaskName => $"Read process with Key {LoadProcessKey}";
        public override void Execute() {
            LoadProcess = new LoadProcess();
            new SqlTask(this, Sql) {
                Actions = new List<Action<object>>() {
                col => LoadProcess.LoadProcessKey = (int)col,
                col => LoadProcess.StartDate = (DateTime)col,
                col => LoadProcess.TransferCompletedDate = (DateTime?)col,
                col => LoadProcess.EndDate = (DateTime?)col,
                col => LoadProcess.ProcessName = (string)col,
                col => LoadProcess.StartMessage = (string)col,
                col => LoadProcess.IsRunning = (bool)col,
                col => LoadProcess.EndMessage = (string)col,
                col => LoadProcess.WasSuccessful = (bool)col,
                col => LoadProcess.AbortMessage = (string)col,
                col => LoadProcess.WasAborted= (bool)col,
                col => LoadProcess.IsFinished= (bool)col,
                col => LoadProcess.IsTransferCompleted= (bool)col
                }
            }.ExecuteReader();
            ControlFlow.CurrentLoadProcess = LoadProcess;            
        }

        /* Public properties */       
        public int? LoadProcessKey { get; set; }
        public LoadProcess LoadProcess { get; private set; }

        public string Sql => $@"
select LoadProcessKey, StartDate, TransferCompletedDate, EndDate, ProcessName, StartMessage, IsRunning, EndMessage, WasSuccessful, AbortMessage, WasAborted, IsFinished, IsTransferCompleted
from etl.LoadProcess
where LoadProcessKey = {LoadProcessKey ?? ControlFlow.CurrentLoadProcess.LoadProcessKey}
";

        public ReadLoadProcessTask() {
            
        }
        public ReadLoadProcessTask(int? loadProcessKey) : this(){
            this.LoadProcessKey = loadProcessKey;
        }    

        public static void Read(int? loadProcessKey) => new ReadLoadProcessTask(loadProcessKey).Execute();


    }
}
