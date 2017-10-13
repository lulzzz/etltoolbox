using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.IO;

namespace ALE.ETLTools {
    public class DropCubeTask : GenericTask, ITask {
        public override string TaskType { get; set; } = "DROPCUBE";
        public override string TaskName => $"Drops cube {ASConnectionManager.ConnectionString.CatalogName}";
        public override void Execute() {
            NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            using (ASConnectionManager) {
                ASConnectionManager.Open();
                ASConnectionManager.DropIfExists();
            }
            NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }
        public DropCubeTask() {
            NLogger = NLog.LogManager.GetLogger("Default");
        }

        public DropCubeTask(string name) : this() {
            this.TaskName = name;
        }

        public static void Execute(string name) => new DropCubeTask(name).Execute();

        NLog.Logger NLogger { get; set; }

    }
}
