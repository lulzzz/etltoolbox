using System;


namespace ALE.ETLTools {
    public class EndLoadProcessTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "LOADPROCESS_END";
        public override string TaskName => $"End process with key {LoadProcessKey}";
        public override void Execute() {
            new SqlTask(this, Sql).ExecuteNonQuery();
            ReadLoadProcessTask.Read(LoadProcessKey ?? ControlFlow.CurrentLoadProcess.LoadProcessKey);
        }

        /* Public properties */        
        public int? LoadProcessKey { get; set; } 
        public string EndMessage { get; set; } = string.Empty;
        

        public string Sql => $@"EXECUTE etl.EndLoadProcess
	 @LoadProcessKey = '{LoadProcessKey ?? ControlFlow.CurrentLoadProcess.LoadProcessKey}',
	 @EndMessage = '{EndMessage}'";

        public EndLoadProcessTask() {

        }

        public EndLoadProcessTask(int? loadProcessKey) : this() {
            this.LoadProcessKey = loadProcessKey;
        }
        public EndLoadProcessTask(int? loadProcessKey, string endMessage) : this(loadProcessKey) {
            this.EndMessage = endMessage;
        }

        public static void End() => new EndLoadProcessTask().Execute();
        public static void End(int? loadProcessKey) => new EndLoadProcessTask(loadProcessKey).Execute();
        public static void End(int? loadProcessKey, string endMessage) => new EndLoadProcessTask(loadProcessKey, endMessage).Execute();


    }
}
