﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;


namespace ALE.ETLTools {
    public static class HashHelper {
        public static string Encrypt_Char40(string text) {
            if (text != null) {
                string hex = "";
                byte[] hashValue = new SHA1Managed().ComputeHash(Encoding.UTF8.GetBytes(text));
                foreach (byte hashByte in hashValue)
                    hex += hashByte.ToString("x2");
                return hex;
            }
            else
                return "";
        }

        public static string Encrypt_Char40(ITask task) => Encrypt_Char40(task.TaskName + "|" + task.TaskType);
    }
}
