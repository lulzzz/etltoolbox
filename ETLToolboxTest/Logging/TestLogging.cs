using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ALE.ETLToolboxTest {
    [TestClass]
    public class TestLogging {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            ControlFlow.STAGE = "SETUP";
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
            CreateLogTablesTask.CreateLog();
            Assert.IsFalse(new SqlTask("Check if logging was disabled for table creation", "select count(*) from etl.Log") { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.IsTrue(new SqlTask("Check etl.Log table", $"select count(*) from sys.tables where type = 'U' and name = 'Log' and schema_id('etl') = schema_id") { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.IsTrue(new SqlTask("Check etl.LoadProcess table", $"select count(*) from sys.tables where type = 'U' and name = 'LoadProcess' and schema_id('etl') = schema_id") { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.IsTrue(new SqlTask("Check proc StartLoadProcess", $"select count(*) from sys.procedures  where type = 'P' and name = 'StartLoadProcess' and schema_id = schema_id('etl')") { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.IsTrue(new SqlTask("Check proc EndLoadProcess", $"select count(*) from sys.procedures  where type = 'P' and name = 'EndLoadProcess' and schema_id = schema_id('etl')") { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.IsTrue(new SqlTask("Check proc AbortLoadProcess", $"select count(*) from sys.procedures  where type = 'P' and name = 'AbortLoadProcess' and schema_id = schema_id('etl')") { DisableLogging = true }.ExecuteScalarAsBool());
        }


        [TestMethod]
        public void TestErrorLogging() {
            LogTask.Error("Error");
            LogTask.Warn("Warn");
            LogTask.Info("Info");
            Assert.AreEqual(3, SqlTask.ExecuteScalar<int>("Check if default log works", "select count(*) from etl.Log where Message in ('Error','Warn','Info')"));
        }

        [TestMethod]
        public void TestStartLoadProcessTask() {
            DateTime beforeTask = DateTime.Now;
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            StartLoadProcessTask.Start("Test process 1");
            DateTime afterTask = DateTime.Now;
            Assert.IsNotNull(ControlFlow.CurrentLoadProcess);
            Assert.AreEqual("Test process 1", ControlFlow.CurrentLoadProcess.ProcessName);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.StartDate <= afterTask && ControlFlow.CurrentLoadProcess.StartDate >= beforeTask);
            
        }

        [TestMethod]
        public void TestEndLoadProcessTask() {
            StartLoadProcessTask.Start("Test process 2");
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.IsRunning == true);
            DateTime beforeTask = DateTime.Now;
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            EndLoadProcessTask.End();
            DateTime afterTask = DateTime.Now;
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.IsRunning == false);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.WasSuccessful == true);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.IsFinished == true);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.EndDate <= afterTask && ControlFlow.CurrentLoadProcess.EndDate >= beforeTask);
        }

        [TestMethod]
        public void TestAbortLoadProcessTask() {
            StartLoadProcessTask.Start("Test process 3");
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.IsRunning == true);
            DateTime beforeTask = DateTime.Now;
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            AbortLoadProcessTask.Abort(ControlFlow.CurrentLoadProcess.LoadProcessKey, "AbortMessage");
            DateTime afterTask = DateTime.Now;
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.IsRunning == false);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.WasAborted == true);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.EndDate <= afterTask && ControlFlow.CurrentLoadProcess.EndDate >= beforeTask);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.AbortMessage == "AbortMessage");
        }

        [TestMethod]
        public void TestIsTransferCompletedForLoadProcessTask() {
            StartLoadProcessTask.Start("Test process 4");
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.IsRunning == true);
            DateTime beforeTask = DateTime.Now;
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            TransferCompletedForLoadProcessTask.Complete(ControlFlow.CurrentLoadProcess.LoadProcessKey);
            DateTime afterTask = DateTime.Now;
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.IsRunning == true);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.TransferCompletedDate <= afterTask && ControlFlow.CurrentLoadProcess.TransferCompletedDate >= beforeTask);
        }

        [TestMethod]
        public void TestLoadProcessKeyInLog() {
            StartLoadProcessTask.Start("Test process 5");
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            Assert.AreEqual(2, SqlTask.ExecuteScalar<int>("Check if load process key is set", $"select count(*) from etl.Log where Message='Test Task' and LoadProcessKey = {ControlFlow.CurrentLoadProcess.LoadProcessKey}"));
        }

        [TestMethod]
        public void TestLoadProcessKeyIfRestarted() {
            StartLoadProcessTask.Start("Test process 6");
            int? processKey1 = ControlFlow.CurrentLoadProcess.LoadProcessKey;
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            Assert.AreEqual(2, SqlTask.ExecuteScalar<int>("Check if load process key is set", $"select count(*) from etl.Log where Message='Test Task' and LoadProcessKey = {processKey1}"));
            
            StartLoadProcessTask.Start("Test process2");
            int? processKey2 = ControlFlow.CurrentLoadProcess.LoadProcessKey;
            Assert.AreNotEqual(processKey1, processKey2);
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            Assert.AreEqual(2, SqlTask.ExecuteScalar<int>("Check if load process key is set", $"select count(*) from etl.Log where Message='Test Task' and LoadProcessKey = {processKey2}"));
        }

        [TestMethod]
        public void TestRemoveLogTablesTask() {
            RemoveLogTablesTask.Remove();
            CreateLogTablesTask.CreateLog();
            RemoveLogTablesTask.Remove();
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if tables are deleted", "select case when object_id('etl.LoadProcess')  is null then 1 else 0 end"));
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if tables are deleted", "select case when object_id('etl.Log')  is null then 1 else 0 end"));
        }

    }

}
