using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace ALE.ETLTools {
    public interface IConnectionManager : IDisposable {
        ConnectionString ConnectionString { get; }
        void Open();
        void Close();       

    }
}
