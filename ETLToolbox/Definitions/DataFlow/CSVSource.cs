using ALE.ETLToolbox;
using CsvHelper;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLToolbox {
    public class CSVSource : IDisposable {

        public CsvReader CsvReader { get; set; }
        string FileName { get; set; }
        public string[] FieldHeaders {
            get {
                return CsvReader.FieldHeaders.Select(header => header.Trim()).ToArray();
            }
        }
        public CSVSource() { }

        StreamReader StreamReader { get; set; }

        public CSVSource(string fileName) {
            FileName = fileName;
        }

        public void Open() {
            StreamReader = new StreamReader(FileName, Encoding.UTF8);
            CsvReader = new CsvReader(StreamReader);
            ConfigureCSVReader();
        }

        public async void Read(ITargetBlock<string[]> target) {
            while (CsvReader.Read()) {
                string[] line = new string[CsvReader.CurrentRecord.Length];
                for (int idx = 0; idx < CsvReader.CurrentRecord.Length; idx++)
                    line[idx] = CsvReader.GetField(idx);
                await target.SendAsync(line);
            }
        }
        private void ConfigureCSVReader() {
            CsvReader.Configuration.Delimiter = ",";
            CsvReader.Configuration.Quote = '"';
            CsvReader.Configuration.AllowComments = true;
            CsvReader.Configuration.Comment = '/';
            CsvReader.Configuration.SkipEmptyRecords = true;
            CsvReader.Configuration.IgnoreBlankLines = true;
            CsvReader.Configuration.TrimHeaders = true;
            CsvReader.Configuration.TrimFields = true;
            CsvReader.Configuration.Encoding = Encoding.UTF8;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing) {
            if (!disposedValue) {
                if (disposing) {
                    if (CsvReader != null)
                        CsvReader.Dispose();
                    CsvReader = null;
                    if (StreamReader != null) StreamReader.Dispose();
                    StreamReader = null;
                }
                disposedValue = true;
            }
        }
        public void Close() {
            Dispose();

        }
        public void Dispose() {
            Dispose(true);
        }
        #endregion

    }
}
