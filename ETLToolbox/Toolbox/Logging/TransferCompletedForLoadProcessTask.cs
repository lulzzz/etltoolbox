using System;


namespace ALE.ETLToolbox {
    public class TransferCompletedForLoadProcessTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "LOADPROCESS_END";
        public override string TaskName => $"End process with key {LoadProcessKey}";
        public override void Execute() {
            new SqlTask(this, Sql).ExecuteNonQuery();
            ReadLoadProcessTask.Read(LoadProcessKey ?? ControlFlow.CurrentLoadProcess.LoadProcessKey);
        }

        /* Public properties */        
        public int? LoadProcessKey { get; set; } 

        public string Sql => $@"EXECUTE etl.TransferCompletedForLoadProcess
	 @LoadProcessKey = '{LoadProcessKey ?? ControlFlow.CurrentLoadProcess.LoadProcessKey}'";

        public TransferCompletedForLoadProcessTask() {

        }

        public TransferCompletedForLoadProcessTask(int? loadProcessKey) : this() {
            this.LoadProcessKey = loadProcessKey;
        }

        public static void Complete() => new TransferCompletedForLoadProcessTask().Execute();
        public static void Complete(int? loadProcessKey) => new TransferCompletedForLoadProcessTask(loadProcessKey).Execute();


    }
}
