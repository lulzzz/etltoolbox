using System;


namespace ALE.ETLTools {
    public class AbortLoadProcessTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "LOADPROCESS_ABORT";
        public override string TaskName => $"Abort process with key {LoadProcessKey}";
        public override void Execute() {
            new SqlTask(this, Sql).ExecuteNonQuery();
            ReadLoadProcessTask.Read(LoadProcessKey ?? ControlFlow.CurrentLoadProcess.LoadProcessKey);
        }

        /* Public properties */
        public int? LoadProcessKey { get; set; }
        public string AbortMessage { get; set; } = string.Empty;


        public string Sql => $@"EXECUTE etl.AbortLoadProcess
	 @LoadProcessKey = '{LoadProcessKey ?? ControlFlow.CurrentLoadProcess.LoadProcessKey}',
	 @AbortMessage = '{AbortMessage}'";

        public AbortLoadProcessTask() {

        }

        public AbortLoadProcessTask(int? loadProcessKey) : this() {
            this.LoadProcessKey = loadProcessKey;
        }
        public AbortLoadProcessTask(int? loadProcessKey, string abortMessage) : this(loadProcessKey) {
            this.AbortMessage = abortMessage;
        }

        public static void Abort() => new AbortLoadProcessTask().Execute();
        public static void Abort(int? loadProcessKey) => new AbortLoadProcessTask(loadProcessKey).Execute();
        public static void Abort(int? loadProcessKey, string abortMessage) => new AbortLoadProcessTask(loadProcessKey, abortMessage).Execute();


    }
}
