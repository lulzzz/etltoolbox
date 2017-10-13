﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.IO;

namespace ALE.ETLTools
{
    public class ProcessCubeTask : GenericTask, ITask
    {
        public override string TaskType { get; set; } = "PROCESSCUBE";
        public override string TaskName => $"Process cube {ASConnectionManager.ConnectionString.CatalogName}";
        public override void Execute() {
            NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            using (ASConnectionManager) {
                ASConnectionManager.Open();
                ASConnectionManager.Process();
            }
            NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }       
        
        public ProcessCubeTask()
        {
            NLogger = NLog.LogManager.GetLogger("Default");
        }

        public ProcessCubeTask(string name) : this()
        {          
            this.TaskName = name;
        }              

        public static void Execute(string name) => new ProcessCubeTask(name).Execute();

        NLog.Logger NLogger { get; set; }
    }
}