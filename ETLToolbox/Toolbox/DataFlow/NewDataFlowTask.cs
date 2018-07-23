using ALE.ETLToolbox;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLToolbox
{
    public class NewDataFlowTask<TSource,TTarget> : GenericTask, ITask 
        where TSource : class,new() 
        where TTarget : class,new()
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "NEWDATAFLOW";
        public override string TaskName { get; set; }

        /* Public properties */
   
        NLog.Logger NLogger { get; set; }

        public Table InputTable { get; set; }
        public Table OutputTable { get; set; }

        public Func<TSource,TTarget> RowTrans { get; set; }

        public NewDataFlowTask()
        {
            NLogger = NLog.LogManager.GetLogger("Default");
        }

        public NewDataFlowTask(string name, Table inputTable, Table outputTable) : this()
        {
            TaskName = name;
            InputTable = inputTable;
            OutputTable = outputTable;

        }


        public override void Execute()
        {
            TransformBlock<TSource, TTarget> tb = new TransformBlock<TSource, TTarget>(input =>
            {
                //Do something here with inputTable?
                return RowTrans(input); 
            }
                );

        }
            
    

        //public static void Execute(string name, Table inputTable, Table outputTable) => new NewDataFlowTask(name, inputTable, outputTable).Execute();  
    }

    public class Table : List<TableColumn>
    {
        
    }
}
