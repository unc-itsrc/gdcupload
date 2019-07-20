﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace upload2gdc
{
    class Util
    {
        public static int GoFindDataFiles(string basePath)
        {
            // since we cannot modify a Dictionary item while iterating over the dictionary,
            // copy the keys to a List and iterate over that instead
            List<int> ListOfKeys = new List<int>();
            foreach (KeyValuePair<int, SeqFileInfo> dataFile in Program.SeqDataFiles)
                ListOfKeys.Add(dataFile.Key);

            int numFilesNotFound = 0;

            foreach (int key in ListOfKeys)
            {
                string TracSeqDeliveryFolderName = "";

                SeqFileInfo newDataFile = Program.SeqDataFiles[key];
                string runId = newDataFile.Submitter_id.Substring(0, 35);  // first 35 chars of the submitter_id is our run_id

                string fileLocation = "";

                if (newDataFile.DataFileName.IndexOf("bam") != -1)
                {
                    TracSeqDeliveryFolderName = "uBam";
                    fileLocation = Path.Combine(basePath, TracSeqDeliveryFolderName, runId);
                }

                else if (newDataFile.DataFileName.IndexOf("fastq") != -1)
                {
                    TracSeqDeliveryFolderName = "fastq";
                    fileLocation = Path.Combine(basePath, TracSeqDeliveryFolderName); // runId is not currently in the path
                }


                if (File.Exists(Path.Combine(fileLocation, newDataFile.DataFileName)))
                {
                    newDataFile.DataFileLocation = fileLocation;
                    newDataFile.ReadyForUpload = true;
                    Program.SeqDataFiles[key] = newDataFile;
                    //Console.WriteLine($"Found: {Path.Combine(fileLocation, newDataFile.DataFileName)}");
                }
                else
                {
                    numFilesNotFound++;
                    //Console.WriteLine($"Not found: {Path.Combine(fileLocation, newDataFile.DataFileName)}");
                }
            }

            return numFilesNotFound;
        }

        // this needs to be separate from GoFindDataFiles which requires an upload_report. 
        // we need to be able to report on files without an upload report, which means no Program.SeqDataFiles
        // so we report on SURdictionary instead which comes from the .json metadata file
        public static void ReportOnFilesReady(string basePath)
        {
            int numFilesFound = 0;
            int numFilesNotFound = 0;

            StringBuilder filesFound = new StringBuilder();
            StringBuilder filesNotFound = new StringBuilder();

            foreach (var item in GDCmetadata.SURdictionary)
            {
                string TracSeqDeliveryFolderName = "";

                string fileName = item.Value.file_name;

                string runId = fileName.Substring(0, 35);  // first 35 chars is our run_id

                string fileLocation = "";
                if (fileName.IndexOf("bam") != -1)
                {
                    TracSeqDeliveryFolderName = "uBam";
                    fileLocation = Path.Combine(basePath, TracSeqDeliveryFolderName, runId);
                }

                else if (fileName.IndexOf("fastq") != -1)
                {
                    TracSeqDeliveryFolderName = "fastq";
                    fileLocation = Path.Combine(basePath, TracSeqDeliveryFolderName);  // runId is not currently in the path
                }


                if (File.Exists(Path.Combine(fileLocation, fileName)))
                {
                    filesFound.Append(item.Value.file_name + Environment.NewLine);
                    numFilesFound++;
                    //Console.WriteLine($"Found: {Path.Combine(fileLocation, fileName)}");
                }
                else
                {
                    filesNotFound.Append(item.Value.file_name + Environment.NewLine);
                    numFilesNotFound++;
                    //Console.WriteLine($"Not found: {Path.Combine(fileLocation, fileName)}");
                }
            }

            Console.Write(Environment.NewLine);
            Console.WriteLine($"Out of {numFilesFound + numFilesNotFound} files in the json md file, {numFilesFound} were found, and {numFilesNotFound} were Not found.");
            Console.Write(Environment.NewLine);

            Console.WriteLine("Press any key within 4 seconds to show list of file names");
            bool writeDetails = Task.Factory.StartNew(() => Console.ReadKey()).Wait(TimeSpan.FromSeconds(4.0));

            if (writeDetails)
            {
                Console.WriteLine(Environment.NewLine);
                Console.WriteLine("The following files *were found*:");
                Console.WriteLine(filesFound.ToString());
                Console.WriteLine("");
                if (numFilesNotFound > 0)
                    Console.WriteLine(filesNotFound.ToString());
                else
                    Console.WriteLine("There were NO files that were not found.");
            }

        }

        public static void WriteResultsOfFileScanToScreen(int numFilesNotFound)
        {
            Console.Write(Environment.NewLine);
            if (numFilesNotFound == 0)
            {
                Console.WriteLine($"All {Program.SeqDataFiles.Count()} of the files to be uploaded were found" + Environment.NewLine);

                Console.WriteLine("Press any key within 3 seconds to show list of file names");
                bool writeDetails = Task.Factory.StartNew(() => Console.ReadKey()).Wait(TimeSpan.FromSeconds(3.0));

                if (writeDetails)
                {
                    Console.WriteLine(Environment.NewLine);
                    Console.WriteLine("The following files *were found*:");
                    foreach (var item in Program.SeqDataFiles)
                        Console.WriteLine(item.Value.DataFileName);
                }
                return;
            }
            else
            {
                Console.WriteLine($"*** {numFilesNotFound} files not found out of an expected {Program.SeqDataFiles.Count()} files.");
                Console.Write(Environment.NewLine);

                Console.WriteLine("The following files were NOT found: ");

                foreach (var item in Program.SeqDataFiles)
                    if (!item.Value.ReadyForUpload)
                        Console.WriteLine(item.Value.DataFileName);

                Console.WriteLine(Environment.NewLine);
                Console.WriteLine("Press any key within 3 seconds to show files that *were found*");
                bool writeDetails = Task.Factory.StartNew(() => Console.ReadKey()).Wait(TimeSpan.FromSeconds(3.0));

                if (writeDetails)
                {
                    Console.WriteLine(Environment.NewLine);
                    Console.WriteLine("The following files *were found*:");
                    foreach (var item in Program.SeqDataFiles)
                        if (item.Value.ReadyForUpload)
                            Console.WriteLine(item.Value.DataFileName);
                }
            }
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
                                    Submitter_id = parts[4],
                                    ReadyForUpload = false
                                };

                                var tempSUR = new SUR();
                                if (GDCmetadata.SURdictionary.TryGetValue(parts[4], out tempSUR))
                                {
                                    newDataFile.DataFileName = tempSUR.file_name;
                                    newDataFile.DataFileSize = tempSUR.file_size;
                                }

                                Program.SeqDataFiles.Add(counter, newDataFile);
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


        public static void CheckLogFiles(string dirLocation)
        {
            string logfiledirmask = "*.log";

            string[] files = Directory.GetFiles(dirLocation, logfiledirmask, SearchOption.TopDirectoryOnly);

            List<string> CompletedUUIDs = new List<string>();
            List<string> FailedUUIDs = new List<string>();
            int TotalRequeues = 0;

            if (files.Length > 0)
            {
                string line = "";
                foreach (string filename in files)
                {
                    using (StreamReader file = new StreamReader(filename))
                    {
                        while ((line = file.ReadLine()) != null)
                        {
                            if (line.Contains("Multipart upload finished for file"))
                            {
                                string[] parts = line.Split();
                                CompletedUUIDs.Add(parts[5]);
                            }
                            else if (line.Contains("File-NOT-UPLOADED:"))
                            {
                                string[] parts = line.Split();
                                FailedUUIDs.Add(parts[5]);
                            }
                            else if (line.Contains("Re-queued:"))
                            {
                                TotalRequeues++;
                            }
                        }
                    }
                }
            }

            StringBuilder sb = new StringBuilder();
            StringBuilder header4ConsoleAndLogFile = new StringBuilder();
            string atLeastOneFailure = "";
            if (FailedUUIDs.Count > 0)
                atLeastOneFailure = " *****";

            header4ConsoleAndLogFile.Append($"{DateTime.Now.ToString("g")}: Results of scanning {files.Length} log files in directory: {dirLocation}");
            header4ConsoleAndLogFile.Append(Environment.NewLine);
            header4ConsoleAndLogFile.Append(Environment.NewLine);
            header4ConsoleAndLogFile.Append($" Total number of requeues: {TotalRequeues}" + Environment.NewLine);
            header4ConsoleAndLogFile.Append($"       Successful uploads: {CompletedUUIDs.Count()} " + Environment.NewLine);
            header4ConsoleAndLogFile.Append($"           Failed uploads: {FailedUUIDs.Count()} {atLeastOneFailure}");
            header4ConsoleAndLogFile.Append(Environment.NewLine + Environment.NewLine);

            sb.Append(header4ConsoleAndLogFile.ToString());

            if (CompletedUUIDs.Count > 0)
            {
                sb.Append("--- Success:");
                sb.Append(Environment.NewLine);
                foreach (string item in CompletedUUIDs)
                {
                    sb.Append(item);
                    sb.Append(Environment.NewLine);
                }
            }

            if (FailedUUIDs.Count > 0)
            {
                sb.Append(Environment.NewLine + Environment.NewLine);
                sb.Append("*** Failed:");
                sb.Append(Environment.NewLine);
                foreach (string item in FailedUUIDs)
                {
                    sb.Append(item);
                    sb.Append(Environment.NewLine);
                }
            }
            else
            {
                sb.Append(Environment.NewLine);
                sb.Append(Environment.NewLine);
                sb.Append("--- There were no failures");

            }

            string resultsFileName = "logScan-" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".log";

            try
            {
                File.WriteAllText(Path.Combine(dirLocation, resultsFileName), sb.ToString());
            }
            catch {
                Console.WriteLine("Exception writing results from log file scan.");
            }

            Console.WriteLine(Environment.NewLine);
            Console.Write(header4ConsoleAndLogFile.ToString());
        }


        public static string SetLocation4LogFiles(string fileLocation)
        {
            if (Directory.Exists(fileLocation))  // best case, operator specified the location and it exists
            {
                string newLogFileDir = Path.Combine(fileLocation, ("gdc-" + DateTime.Now.ToString("yyyyMMddHHmmss")));
                Directory.CreateDirectory(newLogFileDir);
                return newLogFileDir; 
            }
            else
            {
                if (fileLocation.Length > 0)
                    Console.WriteLine($"Specified Log file location not found: {fileLocation}");
            }

            string homeDir = Environment.GetFolderPath(Environment.SpecialFolder.Personal);

            // if we can't find a home directory, just use the current directory
            if (!Directory.Exists(homeDir))
                return Directory.GetCurrentDirectory();


            // create a new logfile directory within homedir that is specific to each run
            string homeDirLogs = Path.Combine(homeDir, "upload2gdc-logs");

            if (!Directory.Exists(homeDirLogs))
                Directory.CreateDirectory(homeDirLogs);

            string runSpecificLogDir = Path.Combine(homeDirLogs, ("gdc-" + DateTime.Now.ToString("yyyyMMdd-HHmmss")));
            Directory.CreateDirectory(runSpecificLogDir);

            return runSpecificLogDir;
        }


    }
}

