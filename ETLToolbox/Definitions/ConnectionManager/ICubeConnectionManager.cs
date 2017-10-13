using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace ALE.ETLTools {
    public interface ICubeConnectionManager : IConnectionManager {
        void Process();
        void DropIfExists();
    }
}
