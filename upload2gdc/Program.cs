using CommandLine;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace upload2gdc
{
    class SeqFileInfo
    {
        public string Id { get; set; }
        public string Related_case { get; set; }
        public string EType { get; set; }
        public string Submitter_id { get; set; }
        public string DataFileName { get; set; }
        public string DataFileLocation { get; set; }
        public int UploadAttempts { get; set; }
    }

    //class UploadConfig
    //{
    //    public string TokenFile { get; set; }
    //    public int NumRetries { get; set; }
    //    public string DataTransferTool { get; set; }
    //    public bool UseSimulator { get; set; }
    //}

    //  USAGE: 
    //  dotnet upload2gdc.dll gdc-upload-report.tsv
    // 
    //


    class Program
    {
        private static string UploadReportFileName; // this file comes from the GDC after successful metadata upload via the portal
        private static int NumberOfThreads; // number of simultaneously executing uploads

        // the dictionary contains all necessary info for each sequence data file to be uploaded
        // Id in SeqDataFilesQue is the key to element item in the dictionary
        private static Dictionary<int, SeqFileInfo> SeqDataFiles = new Dictionary<int, SeqFileInfo>();
        private static ConcurrentQueue<int> SeqDataFilesQueue = new ConcurrentQueue<int>();

        // each thread gets its own log file - prevents file contention between threads
        private static Dictionary<int, string> LogFileSet = new Dictionary<int, string>();
        private static readonly string LogFileBaseName = "logfile-";
        private static readonly string LogFileExtension = ".log";
        private static int NumberOfFilesToUpload = 0;
        private static string DataTransferTool = "gdcsim.exe";
        private static string GDCTokenFile;
        private static int NumRetries;
        private static bool UseSimulator;

        static void Main(string[] args)
        {
            Parser.Default.ParseArguments<Options>(args);

            Parser.Default.ParseArguments<Options>(args)
                .WithParsed<Options>(o =>
                {
                    UploadReportFileName = o.URFile;
                    NumberOfThreads = o.NumThreads;
                    UseSimulator = o.UseSimulator;
                    NumRetries = o.Retries;

                    if (o.Verbose)
                    {
                        Console.WriteLine($"Verbose output enabled. Current Arguments: -v {o.Verbose}");
                        //Console.WriteLine($"Verbose output enabled. Current Arguments: -ur {o.URFile}");
                    }
                    else
                    {
                        Console.WriteLine($"Current Arguments: -v {o.Verbose}");
                        Console.WriteLine("Quick Start Example!");
                    }
                });

            Console.WriteLine($"Upload Report File from GDC: -ur { UploadReportFileName }");


            if (!ProcessGDCUploadReport(UploadReportFileName))
            {
                Console.WriteLine("Error processing Upload Rerport from the GDC");
                return;
            }


            // Load the work queue with the id of each sequence file in the dictionary
            // Id is the index into the Dictionary to get the full lSeqFileInfo details for the file to be uplaoded
            foreach (KeyValuePair<int, SeqFileInfo> entry in SeqDataFiles)
            {
                SeqDataFilesQueue.Enqueue(entry.Key);
            }

            NumberOfFilesToUpload = SeqDataFilesQueue.Count();

            Console.WriteLine("            Number of work items: " + SeqDataFilesQueue.Count().ToString());
            Console.WriteLine(" Number of work items per thread: " + (SeqDataFilesQueue.Count() / NumberOfThreads).ToString());

            Task[] tasks = new Task[NumberOfThreads];
            for (int thread = 0; thread < NumberOfThreads; thread++)
            {
                tasks[thread] = Task.Run(() =>
                {
                    Console.WriteLine("Spinning up thread: " + thread.ToString());
                    LogFileSet.Add((int)Task.CurrentId, (LogFileBaseName + Task.CurrentId.ToString() + LogFileExtension));
                    do
                    {
                        if (SeqDataFilesQueue.TryDequeue(out int WorkId))
                        {
                            int remainingItems = SeqDataFilesQueue.Count();
                            float percentComplete = 1 - (((float)remainingItems + 1) / NumberOfFilesToUpload);
                            string pc = String.Format("  Percent complete: {0:P0}", percentComplete);
                            Console.WriteLine($"Starting item {WorkId} on thread {Task.CurrentId}; Remaining items:{remainingItems}; {pc}");
                            UploadSequenceData(WorkId, remainingItems);
                        }
                    } while (!SeqDataFilesQueue.IsEmpty);

                    Thread.Sleep(250);
                    // Console.WriteLine("Exiting thread " + Task.CurrentId.ToString());
                });
                Thread.Sleep(1000);  // wait just a bit between thread spinups
            }

            Task.WaitAll(tasks);
        }


        public static bool UploadSequenceData(int workId, int remainingItems)
        {
            SeqFileInfo SeqDataFile = new SeqFileInfo();
            StringBuilder LogMessage = new StringBuilder();

            if (!LogFileSet.TryGetValue((int)Task.CurrentId, out string logFile))
            {
                File.AppendAllText(logFile, ("Unable to get logfile name from LogFileSet " + workId.ToString()) + Environment.NewLine);
                return false;
            }

            if (!SeqDataFiles.TryGetValue(workId, out SeqDataFile))
            {
                File.AppendAllText(logFile, ("Unable to SeqFileInfo object out of SeqDataFiles " + workId.ToString()) + Environment.NewLine);
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

            sb.Append(DateTime.Now.ToString("g") + "  uploading " + SeqDataFile.Id);
            sb.Append(" on thread " + Task.CurrentId.ToString());
            sb.Append(" with " + remainingItems.ToString() + " work items remaining.");

            string logRecord = sb.ToString();
            sb.Clear();

            sb.Append("Begin: " + SeqDataFile.Id + "\t" + SeqDataFile.Submitter_id + "\t" + startTime);
            sb.Append(logRecord);
            sb.Append(Environment.NewLine + "cmd = " + DataTransferTool + " " + cmdLineArgs);
            File.AppendAllText(logFile, sb.ToString());
            sb.Clear();

            string stdOut = "";
            string stdErr = "";

            using (var proc = new Process())
            {
                ProcessStartInfo procStartInfo = new ProcessStartInfo();
                procStartInfo.FileName = DataTransferTool; 
                procStartInfo.Arguments = cmdLineArgs;
                procStartInfo.WorkingDirectory = SeqDataFile.DataFileLocation;   // gdc-client requires this
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

            string knownErrorMessage1 = "File in validated state, initiate_multipart not allowed";
            string knownErrorMessage2 = "File with id " + SeqDataFile.Id + " not found";
            string endTime = DateTime.Now.ToString("g");

            int uploadSuccess = stdOut.IndexOf("Multipart upload finished for file " + SeqDataFile.Id);
            sb.Clear();
            bool keepWorking = true;
            string logDateTime = DateTime.Now.ToString("yyyyMMddHHmmss");

            if (uploadSuccess == -1)  // upload was not successful
            {
                sb.Append(Environment.NewLine);
                string failBaseText = "---" + "\t" + logDateTime + "\t" + "File-NOT-UPLOADED:" + "\t" + SeqDataFile.Id + "\t" + SeqDataFile.Submitter_id + "\t";

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
                    // need to remove item from dictionay SeqDataFiles, then add it back to the dictionary with updated UploadAttempts
                    SeqDataFiles.Remove(workId);
                    SeqDataFile.UploadAttempts++;
                    SeqDataFiles.Add(workId, SeqDataFile);
                    SeqDataFilesQueue.Enqueue(workId);
                    Thread.Sleep(250);

                    string tempTxt = "Re-queued: " + SeqDataFile.UploadAttempts.ToString() + " of " + NumRetries.ToString();
                    sb.Append("---" + "\t" + logDateTime + "\t" + "Re-queuing" + "\t" + SeqDataFile.Id + "\t" + SeqDataFile.Submitter_id + "\t" + tempTxt);
                    sb.Append(Environment.NewLine + "stdErr = " + stdErr);
                }

            }

            sb.Append(Environment.NewLine + stdOut);
            sb.Append("End: " + endTime + "; " + SeqDataFile.Submitter_id + Environment.NewLine + Environment.NewLine);
            File.AppendAllText(logFile, sb.ToString());

            return true;
        }


        public static bool ProcessGDCUploadReport(string fileName)
        {
            if (!File.Exists(fileName))
            {
                Console.WriteLine("File not found, Upload Report file from GDC: " + fileName);
                return false;
            }

            int counter = 0;
            string line;

            try
            {
                using (StreamReader file = new StreamReader(fileName))
                {
                    while ((line = file.ReadLine()) != null)
                    {
                        string[] parts = line.Split('\t');
                        if (parts.Length > 1)
                        {
                            if (parts[2] == "submitted_unaligned_reads")
                            {
                                counter++;
                                SeqFileInfo temp = new SeqFileInfo
                                {
                                    Id = parts[0],
                                    Related_case = parts[1],
                                    EType = parts[2],
                                    Submitter_id = parts[4]
                                };
                                SeqDataFiles.Add(counter, temp);
                            }
                        }
                    }
                    file.Close();
                }
            }
            catch
            {
                Console.WriteLine("Exception while processing upload report from the gdc: " + fileName);
                Console.WriteLine("Counter = " + counter.ToString());
                return false;
            }

            return true;
        }



    }

    class Options
    {
        // https://www.nuget.org/packages/CommandLineParser/

        [Option(
            Default = false,
            HelpText = "Prints all messages to standard output.")]
        public bool Verbose { get; set; }

        [Option("ur",
            Default = "not set",
            HelpText = "Path to file that is the Upload Report from GDC.")]
        public string URFile { get; set; }

        [Option("threads",
            Default = 10,
            HelpText = "Number of simultaneous file uploads.")]
        public int NumThreads { get; set; }

        [Option("token",
            Default = "token.txt",
            HelpText = "Path to GDC token file for API calls.")]
        public string TokenFile { get; set; }

        [Option("retries",
            Default = 3,
            HelpText = "Path to GDC token file for API calls.")]
        public int Retries { get; set; }

        [Option("sim",
            Default = true,
            HelpText = "Use gdcsim.exe instead of the gdc data transfer tool?")]
        public bool UseSimulator { get; set; }

    }

}
