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
    //  requires that the data files are accessible via a file path, which is set via commandline argument
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
        public long DataFileSize { get; set; }
        public int UploadAttempts { get; set; }
    }

    class Program
    {
        private static int NumberOfThreads; // number of simultaneously executing uploads; not really threads, but calling them threads anyway

        // The dictionary contains all necessary info for each sequence data file to be uploaded
        // Id in SeqDataFilesQueue is the key to that item in the dictionary; the work queue 
        // only holds the key for each element in the dictionary
        private static Dictionary<int, SeqFileInfo> SeqDataFiles = new Dictionary<int, SeqFileInfo>();
        private static ConcurrentQueue<int> SeqDataFilesQueue = new ConcurrentQueue<int>();

        // Each thread gets its own log file - prevents file contention between threads
        // using a dictionary to manage the set of log files
        private static Dictionary<int, string> LogFileSet = new Dictionary<int, string>();
        private static readonly string LogFileBaseName = "logfile-";
        private static readonly string LogFileExtension = ".log";

        // configuration stuff - need to figure out how to pass a json file with these config values
        private static string UploadReportFileName; // this file comes from the GDC after successful metadata upload via the portal
        private static string GDCMetaDataFile;      // this is the json file with gdcmetadata used to create RG and SUR objectsin the submission portal
        private static string DataTransferTool = "gdcsim.exe";
        private static string GDCTokenFile;
        private static int NumRetries;
        private static bool UseSimulator;
        private static readonly bool TestMode = true;
        private static string DataFilesBaseLocation;

        private static int NumberOfFilesToUpload;

        static void Main(string[] args)
        {
            Parser.Default.ParseArguments<Options>(args)
                .WithParsed<Options>(o =>
                {
                    UploadReportFileName = o.URFile;
                    NumberOfThreads = o.NumThreads;
                    UseSimulator = o.UseSimulator;
                    NumRetries = o.Retries;
                    GDCTokenFile = o.TokenFile;
                    DataFilesBaseLocation = o.FilesBaseLocation;
                    GDCMetaDataFile = o.GDCMetadataFile;
                });


            if (!ProcessGDCMetaDataFile(GDCMetaDataFile))
            {
                Console.WriteLine("Error processing GDC metadata file.");
                return;
            }

            if (!ProcessGDCUploadReport(UploadReportFileName))
            {
                Console.WriteLine("Error processing Upload Report from the GDC.");
                return;
            }


            // to do: go find the files and update each dictionary object with path to the data file
            //        report how many files could not be found, if > 0, offer option to stop or continue


            // Load the work queue with the dictionary key of each data file in the dictionary
            foreach (KeyValuePair<int, SeqFileInfo> entry in SeqDataFiles)
            {
                SeqDataFilesQueue.Enqueue(entry.Key);
            }

            NumberOfFilesToUpload = SeqDataFilesQueue.Count();
            Console.WriteLine("            Number of work items: " + SeqDataFilesQueue.Count().ToString());
            Console.WriteLine(" Number of work items per thread: " + (SeqDataFilesQueue.Count() / NumberOfThreads).ToString());

            //  todo: show known state to user, allow to continue, cancel, or change NumberOfThreads

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
                });
                Thread.Sleep(500);  // wait just a bit between thread spinups
            }

            Task.WaitAll(tasks);

            // todo: process log files, provide number of files successfully and unsuccessfully uploaded
            //       expired time, bytes transferred

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
                stdOut = "Multipart upload finished for file " + SeqDataFile.Id + Environment.NewLine;
            }
            else
            {
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
                    // need to remove item from dictionay SeqDataFiles, 
                    // then add it back to the dictionary with updated value for UploadAttempts
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

            sb.Append(Environment.NewLine);
            sb.Append(stdOut);
            sb.Append("End: " + endTime + Environment.NewLine + Environment.NewLine);
            File.AppendAllText(logFile, sb.ToString());

            return true;
        }

        
        public static bool ProcessGDCMetaDataFile(string fileName)
        {
            if (!File.Exists(fileName))
            {
                Console.WriteLine("File not found, GDC Metadata File: " + fileName);
                return false;
            }

            string jsonstring = "";

            try
            {
                jsonstring = File.ReadAllText(fileName);
            }
            catch
            {
                Console.WriteLine("Exception reading GDC Metadata File: " + fileName);
                return false;
            }

            if (!GDCmetadata.LoadGDCJsonObjects(jsonstring))
            {
                Console.WriteLine("Error loading GDC Metadata File: " + fileName);
                return false;
            }

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
                                SeqFileInfo newDataFile = new SeqFileInfo
                                {
                                    Id = parts[0],
                                    Related_case = parts[1],
                                    EType = parts[2],
                                    Submitter_id = parts[4]
                                };

                                var tempSUR = new SUR();
                                if (GDCmetadata.SURdictionary.TryGetValue(parts[4], out tempSUR))
                                {
                                    newDataFile.DataFileName = tempSUR.file_name;
                                    newDataFile.DataFileSize = tempSUR.file_size;
                                }

                                SeqDataFiles.Add(counter, newDataFile);
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

}
