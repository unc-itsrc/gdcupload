using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace upload2gdc
{
    class Options
    {
        // https://www.nuget.org/packages/CommandLineParser/

        [Option(
            Default = false,
            HelpText = "Prints all messages to standard output.")]
        public bool Verbose { get; set; }

        [Option("ur",
            Default = "",
            Required = false,
            HelpText = "Path to file that is the Upload Report from GDC.")]
        public string URFile { get; set; }

        [Option("md",
            Default = "",
            Required = false,
            HelpText = "Path to file that is the GDC json metadata associated with the upload report from GDC.")]
        public string GDCMetadataFile { get; set; }

        [Option("files",
            Default = "L:\\tracseq\\delivery",   // on datamover node, set this to:  /proj/seq/tracseq/delivery
            Required = false,
            HelpText = "Path to base location of sequence data files.")]
        public string FilesBaseLocation { get; set; }

        [Option("threads",
            Default = 10,
            Required = false,
            HelpText = "Number of simultaneous file uploads.")]
        public int NumThreads { get; set; }

        [Option("token",
            Default = "token.txt",
            Required = false,
            HelpText = "Path to GDC token file for API calls.")]
        public string TokenFile { get; set; }

        [Option("retries",
            Default = 3,
            Required = false,
            HelpText = "Max number of times to try upload before failing.")]
        public int Retries { get; set; }

        [Option("log",
            Default = "",
            Required = false,
            HelpText = "Path to store and read log files.")]
        public string LogFileLocation { get; set; }

        [Option("logsonly",
            Default = false,
            Required = false,
            HelpText = "Set this option to true to only scan a set of logfiles.")]
        public bool OnlyScanLogFiles { get; set; }

        [Option("filesonly",
            Default = false,
            Required = false,
            HelpText = "Set this option to true to only look for and report on data file availability.")]
        public bool OnlyCheck4DataFiles { get; set; }

        [Option("dtt",
            Default = "gdc-client",   // this is the setting for rc-dm2.its.unc.edu
            Required = false,
            HelpText = "Path to store the GDC data transfer tool executable.")]
        public string DataTransferTool { get; set; }

        [Option("sim",
            Default = true,
            Required = false,
            HelpText = "Use gdcsim.exe instead of the gdc data transfer tool?")]
        public bool UseSimulator { get; set; }


    }
}
