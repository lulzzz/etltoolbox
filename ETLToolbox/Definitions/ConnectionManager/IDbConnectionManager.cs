using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace ALE.ETLTools {
    public interface IDbConnectionManager : IConnectionManager  {      
        int ExecuteNonQuery(string command);
        object ExecuteScalar(string command);
        IDataReader ExecuteReader(string command);
        void BulkInsert(IDataReader data, IColumnMappingCollection columnMapping, string tableName);
    }
}
