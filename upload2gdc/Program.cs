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
    //  This Win/Linux/Mac console application is a wrapper for the GDC Data Transfer Tool (gdc-client). 
    //  It manages uploads of genomic sequence data files to the National Cancer Institute.
    //  It requires that the data files are accessible via a file path from the OS upon which it runs.
    //  It is known to work on rc-dm2.its.unc.edu, an ITS-RC datamover node with the .net core sdk installed.
    //  https://gdc.cancer.gov/access-data/gdc-data-transfer-tool
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

        private static int NumberOfThreads; // number of simultaneously executing uploads
                                            // these are actually multithreaded processes but calling them threads anyway

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
        public static bool OnlyCheck4DataFiles;
        private static string DataFilesBaseLocation;

        private static string GenerateGDCMetadata4ThisFile;
        private static bool GenerateGDCMetadataDevServer;
        private static string GenerateGDCMetadataExperimentType;
        private static string GenerateGDCMetadataSkipList;
        private static string MultiPartMethod;


        public static string SkipFileUUIDs;
        public static List<string> SkipUUIDs = new List<string>();

        private static int NumberOfFilesToUpload;
        public static readonly bool TestMode = false;

        private static DateTime ProgramStartTime;


        static void Main(string[] args)
        {
            ProgramStartTime = DateTime.Now;
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
                    OnlyCheck4DataFiles = o.OnlyCheck4DataFiles;
                    SkipFileUUIDs = o.SkipFile;
                    GenerateGDCMetadata4ThisFile = o.MDGen;
                    GenerateGDCMetadataDevServer = o.MDGenDevServer;
                    GenerateGDCMetadataExperimentType = o.MDGenType;
                    GenerateGDCMetadataSkipList = o.MDGenSkipList;
                    MultiPartMethod = o.MultiPartUploads;
                });

            if (!OnlyCheck4DataFiles) // no log files to be written when only checking for data files
                LogFileLocation = Util.SetLocation4LogFiles(LogFileLocationFromConfig);

            if (OnlyScanLogFiles)
            {
                Console.WriteLine($"Examining *.log files in this location: {LogFileLocation}");
                Util.CheckLogFiles(LogFileLocationFromConfig);
                return;     // end program
            }

            if (GenerateGDCMetadata4ThisFile != "")
            {
                GenerateMetadata.GenGDCMD(GenerateGDCMetadata4ThisFile, GenerateGDCMetadataDevServer, GenerateGDCMetadataExperimentType, GenerateGDCMetadataSkipList);
                return;     // end program, nothing else to do when generating GDC metadata
            }


            if (!Util.ProcessGDCMetaDataFile(GDCMetaDataFile))
                return;     // end program

            if (OnlyCheck4DataFiles)
            {
                Util.ReportOnFilesReady(DataFilesBaseLocation);
                return;     // end program
            }

            if (!Util.ProcessGDCUploadReport(UploadReportFileName))
                return;     // end program

            int numFilesNotFound = Util.GoFindDataFiles(DataFilesBaseLocation);

            Util.WriteResultsOfFileScanToScreen(numFilesNotFound);

            if (numFilesNotFound == SeqDataFiles.Count() && !TestMode)
            {
                Console.WriteLine($"None of the {SeqDataFiles.Count()} files to be uploaded were found in the staging location {DataFilesBaseLocation}");
                return;     // end program
            }

            Console.WriteLine($"Log files will be written here: {LogFileLocation}");

            if (SkipFileUUIDs != "")
            {
                if (File.Exists(SkipFileUUIDs))
                {
                    Console.WriteLine($"Using SkipFile: {SkipFileUUIDs}");
                    SkipUUIDs = File.ReadAllLines(SkipFileUUIDs).ToList();
                }
            }

            // validate multipartmethod commandline parameter
            if (MultiPartMethod != "yes" && MultiPartMethod != "no" && MultiPartMethod != "program")
            {
                Console.WriteLine($"  Invalid option: \"--multipart {MultiPartMethod}\"{Environment.NewLine}  Must be one of: yes, no, program");
                return;
            }

            // Load the work queue with the dictionary key of each data file in the 
            // dictionary where we have successfully located the file on disk
            foreach (KeyValuePair<int, SeqFileInfo> entry in SeqDataFiles)
                if (entry.Value.ReadyForUpload)
                    if (SkipUUIDs.Contains(entry.Value.Id))
                        Console.WriteLine($"skipping {entry.Value.Id}");
                    else
                        SeqDataFilesQueue.Enqueue(entry.Key);

            NumberOfFilesToUpload = SeqDataFilesQueue.Count();
            Console.WriteLine($" Number of items in Upload Report: {SeqDataFiles.Count()}");
            Console.WriteLine($"             Number of work items: {SeqDataFilesQueue.Count()}" + Environment.NewLine);
            Console.WriteLine($"  Number of work items per thread: {(SeqDataFilesQueue.Count() / NumberOfThreads)}");
            Console.WriteLine($"                       Start Time: {DateTime.Now.ToString("g")}" + Environment.NewLine);


            //  todo: allow to continue, cancel, or perhaps change NumberOfThreads


            Task[] tasks = new Task[NumberOfThreads];
            for (int thread = 0; thread < NumberOfThreads; thread++)
            {
                tasks[thread] = Task.Run(() =>
                {
                    if (TestMode)
                        Console.WriteLine("Spinning up thread: " + thread.ToString());

                    string threadSpecificLogFile = Path.Combine(LogFileLocation, (LogFileBaseName + Task.CurrentId.ToString() + LogFileExtension));
                    LogFileSet.Add((int)Task.CurrentId, threadSpecificLogFile);
                    do
                    {
                        if (SeqDataFilesQueue.TryDequeue(out int WorkId))
                        {
                            int remainingItems = SeqDataFilesQueue.Count();
                            Console.WriteLine($"Starting item {WorkId} on thread {Task.CurrentId}; Remaining items:{remainingItems}");
                            UploadSequenceData(WorkId, remainingItems);
                        }
                    } while (!SeqDataFilesQueue.IsEmpty);
                    Thread.Sleep(500);
                });
                Thread.Sleep(1500);  // wait just a bit between thread spinups
            }

            Task.WaitAll(tasks);

            Util.CheckLogFiles(LogFileLocation);

            TimeSpan elapsed = (DateTime.Now).Subtract(ProgramStartTime);
            Console.WriteLine(Environment.NewLine + "Elapsed time: {0:c}", elapsed.TotalMinutes);
        }


        public static bool UploadSequenceData(int workId, int remainingItems)
        {
            SeqFileInfo SeqDataFile = new SeqFileInfo();
            StringBuilder LogMessage = new StringBuilder();

            if (!LogFileSet.TryGetValue((int)Task.CurrentId, out string logFile))
            {
                Console.WriteLine($"Unable to get logfile name from LogFileSet on TaskId {workId}");
                return false;
            }

            if (!SeqDataFiles.TryGetValue(workId, out SeqDataFile))
            {
                File.AppendAllText(logFile, ($"Unable to get SeqFileInfo object out of SeqDataFiles {workId}" + Environment.NewLine));
                return false;
            }

            string cmdLineArgs;
            string startTime = DateTime.Now.ToString("g");
            StringBuilder sb = new StringBuilder();

            //if (UseSimulator)
            //{
            //    cmdLineArgs = SeqDataFile.Submitter_id + " " + "fast";
            //    DataTransferTool = "gdcsim.exe";
            //}
            //else
            //  cmdLineArgs = ("upload -t " + GDCTokenFile + " " + SeqDataFile.Id);



            // v1.4 version of gdc-client uses single chunck upload when the file size is less than 1GB, however it does not exit cleanly in this mode. So force all xfers to be multi-part by setting 
            // if using v1.4 of gdc-client, user should set "--multipart yes" (default command line switch)
            // if using v1.5 of gdc-client, user should set "--multipart no"
            // by using "--multipart program", it does not force either outcome, and allows gdc-client to decide
            
            string uploadPartSize = ""; 
            long defaultPartSize = 1000000000; // this is the default for v1.4 of gdc-client; unsure about v1.5, going to assume it's the same

            if (MultiPartMethod == "yes")
            {
                if (SeqDataFile.DataFileSize < defaultPartSize)
                {
                    long newPartSize = (long)(SeqDataFile.DataFileSize * 0.8);  // force multipart upload
                    uploadPartSize = $"--upload-part-size {newPartSize}";
                }
            }
            else if (MultiPartMethod == "no")
            {
                long newPartSize = (long)(SeqDataFile.DataFileSize * 1.2);      // force single chunk upload
                uploadPartSize = $"--upload-part-size {newPartSize}";
            }


            // cmdLineArgs = ("upload -t " + GDCTokenFile + " " + SeqDataFile.Id + uploadPartSize);
            cmdLineArgs = $"upload -t {GDCTokenFile} {SeqDataFile.Id} {uploadPartSize}";

            sb.Append("Begin:" + "\t");
            sb.Append(startTime + "\t");
            sb.Append(SeqDataFile.Id + "\t");
            sb.Append(SeqDataFile.Submitter_id);
            sb.Append(Environment.NewLine);

            sb.Append("uploading ");
            sb.Append(SeqDataFile.Id);
            sb.Append(" on thread ");
            sb.Append(Task.CurrentId.ToString());
            sb.Append(" with ");
            sb.Append(remainingItems.ToString());
            sb.Append(" work items remaining.");
            sb.Append(Environment.NewLine);
            sb.Append("WorkingDirectory = ");
            sb.Append(SeqDataFile.DataFileLocation);

            sb.Append(Environment.NewLine);
            sb.Append(SeqDataFile.DataFileName);
            sb.Append("\t");
            sb.Append(SeqDataFile.DataFileSize);
            sb.Append("\t");
            sb.Append("partsize: " + uploadPartSize);
            sb.Append(Environment.NewLine);
            sb.Append("cmd = " + DataTransferTool + " " + cmdLineArgs);

            File.AppendAllText(logFile, sb.ToString());
            sb.Clear();

            string stdOut = "";
            string stdErr = "";

            if (TestMode)
            {
                Console.WriteLine(DataTransferTool + " " + cmdLineArgs + "; filename: " + SeqDataFile.DataFileName);
                // fake the output of a gdc-client run indicating upload finished successfully
                stdOut = "Multipart upload finished for file " + SeqDataFile.Id + Environment.NewLine;  
            }
            else
            {
                using (var proc = new Process())
                {
                    ProcessStartInfo procStartInfo = new ProcessStartInfo();
                    procStartInfo.FileName = DataTransferTool;
                    procStartInfo.Arguments = cmdLineArgs;

                    // the gdc-client DTT requires that it be executed from within the directory where the data file resides
                    // actually, with newer 1.4 version you can set --path path and it seems to work
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

            // two common error messages to look for:
            string knownErrorMessage1 = "File in validated state, initiate_multipart not allowed";  // file already exists at GDC
            string knownErrorMessage2 = "File with id " + SeqDataFile.Id + " not found";            // local file not found, gdc xfer tool likely not executed from within directory that contains the file

            int uploadSuccess = stdOut.IndexOf("Multipart upload finished for file " + SeqDataFile.Id);

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
                    SeqDataFile.UploadAttempts++;
                    SeqDataFiles[workId] = SeqDataFile;

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
                    Console.WriteLine($"Re-queued item {workId}");
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
