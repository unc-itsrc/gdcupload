using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;

namespace upload2gdc
{
    //  This is a wrapper for the GDC Data Transfer Tool used to manage uploads
    //  of genomic sequence data files to the National Cancer Institute
    //  known to work on rc-dm2.its.unc.edu -- data mover node with .net core sdk installed
    //  requires that the data files are accessible via a file path.
    // 
    //  USAGE: 
    //  dotnet uoload2gdc.dll --help
    //  dotnet upload2gdc.dll --ur ~/gdc-upload-report.tsv --md ~/gdc-metadata-file.json --files /proj/seq/tracseq/delivery --token ~/token.txt

    class SeqFileInfo
    {
        public string Id { get; set; }
        public string Related_case { get; set; }
        public string EType { get; set; }
        public string Submitter_id { get; set; }
        public string DataFileName { get; set; }
        public string DataFileLocation { get; set; }
        public bool ReadyForUpload { get; set; }
        public long DataFileSize { get; set; }
        public int UploadAttempts { get; set; }
    }

    class Program
    {
        // The dictionary contains all needed details about each sequence data file
        // ConcurrentQueue contains dictionary Id's for all SeqFileInfo entities where the data files have been verified as present
        public static Dictionary<int, SeqFileInfo> SeqDataFiles = new Dictionary<int, SeqFileInfo>();
        private static ConcurrentQueue<int> SeqDataFilesQueue = new ConcurrentQueue<int>();

        private static int NumberOfThreads; // number of simultaneously executing uploads; 
                                            // these threads are actually multithreaded processes but calling them threads anyway

        // Each thread gets its own log file - prevents file contention between threads
        // using a dictionary to manage the set of log files with the TaskId of the process (thread) as the dictionary key
        private static Dictionary<int, string> LogFileSet = new Dictionary<int, string>();
        private static readonly string LogFileBaseName = "gdclogfile-";
        private static readonly string LogFileExtension = ".log";
        public static string LogFileLocation;

        // configuration stuff - need to figure out how to pass a json file with these config values
        private static string UploadReportFileName; // this file comes from the GDC after successful metadata upload via the portal
        private static string GDCMetaDataFile;      // this is the json file with gdcmetadata used to create RG and SUR objectsin the submission portal
        private static string DataTransferTool;
        private static string GDCTokenFile;
        private static int NumRetries;
        private static bool UseSimulator;
        private static bool OnlyScanLogFiles;
        private static string DataFilesBaseLocation;

        private static int NumberOfFilesToUpload;
        private static readonly bool TestMode = false;

        static void Main(string[] args)
        {
            string LogFileLocationFromConfig = "";

            Parser.Default.ParseArguments<Options>(args).WithParsed<Options>(o =>
                {
                    UploadReportFileName = o.URFile;
                    NumberOfThreads = o.NumThreads;
                    UseSimulator = o.UseSimulator;
                    NumRetries = o.Retries;
                    GDCTokenFile = o.TokenFile;
                    DataFilesBaseLocation = o.FilesBaseLocation;
                    GDCMetaDataFile = o.GDCMetadataFile;
                    DataTransferTool = o.DataTransferTool;
                    OnlyScanLogFiles = o.OnlyScanLogFiles;
                    LogFileLocationFromConfig = o.LogFileLocation;
                });

            LogFileLocation = Util.SetLocation4LogFiles(LogFileLocationFromConfig);
            if (OnlyScanLogFiles)
            {
                Console.WriteLine($"Examining *.log files in this location: {LogFileLocation}");
                Util.CheckLogFiles(LogFileLocation);
                return;  // skip everything else
            }

            Console.WriteLine($"Log files will be written here: {LogFileLocation}");

            if (!Util.ProcessGDCMetaDataFile(GDCMetaDataFile))
                return;

            if (!Util.ProcessGDCUploadReport(UploadReportFileName))
                return;

            int numFilesNotFound = Util.GoFindDataFiles(DataFilesBaseLocation);

            if (numFilesNotFound == SeqDataFiles.Count())
            {
                Console.WriteLine($"None of the {SeqDataFiles.Count()} files to be uploaded were found in the staging location {DataFilesBaseLocation}");
                if (!TestMode)
                    return;
            }
            else if (numFilesNotFound > 0)
            {
                Console.WriteLine($"*** {numFilesNotFound} files not found out of an expected {SeqDataFiles.Count()} files." );
            }
            else
                Console.WriteLine($"All {SeqDataFiles.Count()} of the files to be uploaded were found");


            // Load the work queue with the dictionary key of each data file in the 
            // dictionary where we have successfully located the file on disk
            foreach (KeyValuePair<int, SeqFileInfo> entry in SeqDataFiles)
            {
                if (entry.Value.ReadyForUpload)
                    SeqDataFilesQueue.Enqueue(entry.Key);
            }

            NumberOfFilesToUpload = SeqDataFilesQueue.Count();
            Console.WriteLine(" Number of items in Upload Report: " + SeqDataFiles.Count().ToString());
            Console.WriteLine("             Number of work items: " + SeqDataFilesQueue.Count().ToString());
            Console.WriteLine("  Number of work items per thread: " + (SeqDataFilesQueue.Count() / NumberOfThreads).ToString());

            
            //  todo: show known state to user, allow to continue, cancel, or perhaps change NumberOfThreads


            Task[] tasks = new Task[NumberOfThreads];
            for (int thread = 0; thread < NumberOfThreads; thread++)
            {
                tasks[thread] = Task.Run(() =>
                {
                    Console.WriteLine("Spinning up thread: " + thread.ToString());
                    string threadSpecificLogFile = Path.Combine(LogFileLocation, (LogFileBaseName + Task.CurrentId.ToString() + LogFileExtension));
                    LogFileSet.Add((int)Task.CurrentId, threadSpecificLogFile);
                    do
                    {
                        if (SeqDataFilesQueue.TryDequeue(out int WorkId))
                        {
                            int remainingItems = SeqDataFilesQueue.Count() + 1;
                            float percentComplete = 1 - (((float)remainingItems + 1) / NumberOfFilesToUpload);
                            string pc = String.Format("  Percent complete: {0:P0}", percentComplete);
                            Console.WriteLine($"Starting item {WorkId} on thread {Task.CurrentId}; Remaining items:{remainingItems}; {pc}");
                            UploadSequenceData(WorkId, remainingItems);
                        }
                    } while (!SeqDataFilesQueue.IsEmpty);
                    Thread.Sleep(500);
                });
                Thread.Sleep(500);  // wait just a bit between thread spinups
            }

            Task.WaitAll(tasks);

            Util.CheckLogFiles(LogFileLocation);

        }


        public static bool UploadSequenceData(int workId, int remainingItems)
        {
            SeqFileInfo SeqDataFile = new SeqFileInfo();
            StringBuilder LogMessage = new StringBuilder();

            if (!LogFileSet.TryGetValue((int)Task.CurrentId, out string logFile))
            {
                //File.AppendAllText(logFile, ("Unable to get logfile name from LogFileSet " + workId.ToString()) + Environment.NewLine);
                return false;
            }

            if (!SeqDataFiles.TryGetValue(workId, out SeqDataFile))
            {
                File.AppendAllText(logFile, ("Unable to get SeqFileInfo object out of SeqDataFiles " + workId.ToString()) + Environment.NewLine);
                return false;
            }

            string cmdLineArgs;
            string startTime = DateTime.Now.ToString("g");
            StringBuilder sb = new StringBuilder();

            if (UseSimulator)
            {
                cmdLineArgs = SeqDataFile.Submitter_id + " " + "fast";
                DataTransferTool = "gdcsim.exe";
            }
            else
                cmdLineArgs = ("upload -t " + GDCTokenFile + " " + SeqDataFile.Id);

            sb.Append("Begin:" + "\t");
            sb.Append(startTime + "\t");
            sb.Append(SeqDataFile.Id + "\t");
            sb.Append(SeqDataFile.Submitter_id);
            sb.Append(Environment.NewLine);

            sb.Append("uploading " + SeqDataFile.Id);
            sb.Append(SeqDataFile.Id);
            sb.Append(" on thread ");
            sb.Append(Task.CurrentId.ToString());
            sb.Append(" with ");
            sb.Append(remainingItems.ToString());
            sb.Append(" work items remaining.");
            sb.Append(Environment.NewLine);

            sb.Append("cmd = " + DataTransferTool + " " + cmdLineArgs);
            File.AppendAllText(logFile, sb.ToString());
            sb.Clear();

            string stdOut = "";
            string stdErr = "";

            if (TestMode)
            {
                Console.WriteLine(DataTransferTool + " " + cmdLineArgs + "; filename: " + SeqDataFile.DataFileName);
                
                // fake output of gdc tool indicating upload finished successfully
                stdOut = "Multipart upload finished for file " + SeqDataFile.Id + Environment.NewLine;  
            }
            else
            {
                using (var proc = new Process())
                {
                    ProcessStartInfo procStartInfo = new ProcessStartInfo();
                    procStartInfo.FileName = DataTransferTool;
                    procStartInfo.Arguments = cmdLineArgs;

                    // gdc-client requires execution of the xfer tool from withing the directory where the data file resides
                    procStartInfo.WorkingDirectory = SeqDataFile.DataFileLocation;

                    procStartInfo.CreateNoWindow = true;
                    procStartInfo.UseShellExecute = false;
                    procStartInfo.RedirectStandardOutput = true;
                    procStartInfo.RedirectStandardInput = true;
                    procStartInfo.RedirectStandardError = true;

                    proc.StartInfo = procStartInfo;
                    proc.Start();

                    stdOut = proc.StandardOutput.ReadToEnd();
                    stdErr = proc.StandardError.ReadToEnd();

                    proc.WaitForExit();
                }
            }

            string endTime = DateTime.Now.ToString("g");

            // two common error messages to check for:
            string knownErrorMessage1 = "File in validated state, initiate_multipart not allowed";  // file already exists at GDC
            string knownErrorMessage2 = "File with id " + SeqDataFile.Id + " not found";            // local file not found, gdc xfer tool likely not executed from within directory that contains the file

            // if file upload was successful, this substring will be found in stdOut
            int uploadSuccess = stdOut.IndexOf("Multipart upload finished for file " + SeqDataFile.Submitter_id);

            sb.Clear();
            bool keepWorking = true;
            string logDateTime = DateTime.Now.ToString("g");

            if (uploadSuccess == -1)  // upload was not successful
            {
                sb.Append(Environment.NewLine);
                string failBaseText = "***" + "\t" + logDateTime 
                       + "\t" + "File-NOT-UPLOADED:" 
                       + "\t" + SeqDataFile.Id 
                       + "\t" + SeqDataFile.Submitter_id 
                       + "\t";

                if (stdOut.IndexOf(knownErrorMessage1) != -1)
                {
                    sb.Append(failBaseText + "Fail: File already at GDC");
                    keepWorking = false;
                }
                else if (stdOut.IndexOf(knownErrorMessage2) != -1)
                {
                    sb.Append(failBaseText + "Fail: Local file not found");
                    keepWorking = false;
                }
                else if (SeqDataFile.UploadAttempts == NumRetries)
                {
                    sb.Append(failBaseText + "Fail: Reached Max Retries");
                    keepWorking = false;
                }

                if ((SeqDataFile.UploadAttempts < NumRetries) && keepWorking)
                {
                    SeqDataFiles.Remove(workId);

                    SeqDataFile.UploadAttempts++;
                    SeqDataFiles.Add(workId, SeqDataFile);

                    SeqDataFilesQueue.Enqueue(workId);
                    Thread.Sleep(200);

                    sb.Append("---");
                    sb.Append("\t" + logDateTime);
                    sb.Append("\t" + "Re-queuing");
                    sb.Append("\t" + SeqDataFile.Id);
                    sb.Append("\t" + SeqDataFile.Submitter_id);
                    sb.Append("\t" + "Re-queued: ");
                    sb.Append(SeqDataFile.UploadAttempts.ToString());
                    sb.Append(" of ");
                    sb.Append(NumRetries.ToString());
                    sb.Append(Environment.NewLine);

                    sb.Append("stdErr = " + stdErr);
                }
            }

            sb.Append(Environment.NewLine);
            sb.Append(stdOut);
            sb.Append("End: " + endTime + Environment.NewLine + Environment.NewLine);
            File.AppendAllText(logFile, sb.ToString());
            sb.Clear();

            return true;
        }

    }
}
