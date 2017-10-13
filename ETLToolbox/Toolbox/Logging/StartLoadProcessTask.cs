using System;


namespace ALE.ETLTools {
    public class StartLoadProcessTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "LOADPROCESS_START";
        public override string TaskName => $"Start load process {ProcessName}";
        public override void Execute() {
            LoadProcessKey = new SqlTask(this, Sql).ExecuteScalar<int>();
            new ReadLoadProcessTask(LoadProcessKey ?? 0) { TaskType = this.TaskType, TaskHash = this.TaskHash }.Execute();
        }

        /* Public properties */
        public string ProcessName { get; set; } = "";
        public string StartMessage { get; set; } = "";
        public int? LoadProcessKey { get; private set; }

        public string Sql => $@"
 declare @LoadProcessKey int  
 EXECUTE etl.StartLoadProcess
	 @ProcessName = '{ProcessName}',
	 @StartMessage = '{StartMessage}',
     @LoadProcessKey = @LoadProcessKey OUTPUT
 SELECT	@LoadProcessKey";

        public StartLoadProcessTask() {

        }
        public StartLoadProcessTask(string processName) : this(){
            this.ProcessName = processName;
        }
        public StartLoadProcessTask(string processName,string startMessage) : this(processName) {
            this.StartMessage = startMessage;
        }

        public static void Start(string processName) => new StartLoadProcessTask(processName).Execute();
        public static void Start(string processName, string startMessage) => new StartLoadProcessTask(processName, startMessage).Execute();


    }
}
