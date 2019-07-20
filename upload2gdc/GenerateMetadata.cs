using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace upload2gdc
{

    class ItemCleared4Upload
    {
        public string SampleIdentifier { get; set; }
        public string HtsfSampleName { get; set; }
        public string HtsfExternalCode { get; set; }
    }

    class GenerateMetadata
    {
        private static string TracSeqEndpointProdRNASeq = "https://tracseq.genomics.unc.edu/internal/gdc/readgroup";
        private static string TracSeqEndpointDevRNASeq = "https://mps-mssql.its.unc.edu/tracseq/internal/gdc/readgroup";

        private static string TracSeqEndpointProdSmallRNA = "https://tracseq.genomics.unc.edu/internal/gdc/readgroupsmallrna";
        private static string TracSeqEndpointDevSmallRNA = "https://mps-mssql.its.unc.edu/tracseq/internal/gdc/readgroupsmallrna";

        public static List<ItemCleared4Upload> ItemsCleared4Upload = new List<ItemCleared4Upload>();

        public static void GenGDCMD(string inputFile, bool UseDevServer, string experimentType, string skipFile)
        {
            if (!File.Exists(inputFile))
            {
                Console.WriteLine($"Input file: {inputFile} not found - this is the tab delimited file containing items cleared for upload.");
                return;
            }

            string activeEndpoint = "";
            //Console.WriteLine($"Use dev server: {UseDevServer}");

            if (experimentType.Equals("rnaseq", StringComparison.InvariantCultureIgnoreCase))
            {
                if (UseDevServer)
                    activeEndpoint = TracSeqEndpointDevRNASeq;
                else
                    activeEndpoint = TracSeqEndpointProdRNASeq;
            }
            else if (experimentType.Equals("smallrna", StringComparison.InvariantCultureIgnoreCase))
            {
                if (UseDevServer)
                    activeEndpoint = TracSeqEndpointDevSmallRNA;
                else
                    activeEndpoint = TracSeqEndpointProdSmallRNA;
            }
            else
            {
                Console.WriteLine("Invalid command line option for GDC metadata generation. ");
                Console.WriteLine("mdgentype must be one of: rnaseq, smallrna");
                return;
            }

            if (!ReadInputFile(inputFile, skipFile))
            {
                Console.WriteLine($"Processing Error with input file ({inputFile}) and/or skiplist ({skipFile})");
                return;
            }

            Console.WriteLine($"{Environment.NewLine}Calling TracSeq API for metadata on ** {ItemsCleared4Upload.Count} ** samples.");
            Console.WriteLine($"Using this API endpoint: {activeEndpoint}");

            // now we have our tracseq API endpoint and filtered list of items cleared for upload, ready to get metadata

            bool insertComma = false;
            int numFailures = 0;
            string wsOutput;
            StringBuilder sb_JSON = new StringBuilder();
            sb_JSON.Append("[" + Environment.NewLine);
            List<string> ErrorList = new List<string>();
            string projectName = "";

            foreach (var item in ItemsCleared4Upload)
            {
                string URI = activeEndpoint + "/" + item.SampleIdentifier;
                wsOutput = "";
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(URI);
                request.Timeout = 200000;

                try
                {
                    using (var response = (HttpWebResponse)request.GetResponse())
                    {
                        using (Stream responseStream = response.GetResponseStream())
                        {
                            StreamReader reader = new StreamReader(responseStream, Encoding.UTF8);
                            wsOutput = reader.ReadToEnd();
                        }
                        if (wsOutput.Length > 500)  // if output is less than 500 chars, we know there is a problem
                        {
                            if (insertComma)        // do not insert a comma before the first item, but yes before all others
                                sb_JSON.Append(",");
                            else
                            {
                                insertComma = true;
                                // all records must be from the same project, just need this value once for the json file name
                                // the project name is the set of chars before the underscore in the HTSFSampleName (parts[1])
                                int itemp = item.HtsfSampleName.IndexOf("_");
                                projectName = item.HtsfSampleName.Substring(0, itemp);
                            }

                            sb_JSON.Append(wsOutput.Substring(1, (wsOutput.Length - 2)));  // use substring to remove [ ] from json since we are aggregating components into a larger json array
                            sb_JSON.Append(Environment.NewLine);
                        }
                        else
                        {
                            ErrorList.Add(item.SampleIdentifier);
                            Console.WriteLine("Invalid response from webservice for: " + item.SampleIdentifier);
                            numFailures++;
                        }
                    }
                }

                catch
                {
                    Console.WriteLine("Exception caught for sample: " + item.SampleIdentifier);
                    numFailures++;
                    ErrorList.Add(item.SampleIdentifier);
                }

            }

            if (numFailures > 0)
                Console.WriteLine($"**** Number of samples failed to get metadata: {numFailures}");
            else
                Console.WriteLine("No failures from API calls.");

            if (sb_JSON.Length > 10) // if at least one valid response from json generating web service call
            {
                sb_JSON.Append("]");

                DateTime temp = DateTime.Now;
                string insertValue = "." + temp.ToString("yyyy") +
                    temp.ToString("MM")
                    + temp.ToString("dd")
                    + "-" + temp.ToString("HH")
                    + temp.ToString("mm")
                    + temp.ToString("ss");

                string jsonFile = projectName + "-" + inputFile + insertValue + ".json";
                Console.WriteLine($"Writing JSON to file: {jsonFile}");
                File.AppendAllText(jsonFile, sb_JSON.ToString());
            }

            sb_JSON.Clear();


        }


        private static bool ReadInputFile(string fileName, string skipFile)
        {
            List<string> SkipSampleList = new List<string>();
            bool usingSkipFile = false;

            if (skipFile != "")
            {
                if (File.Exists(skipFile))
                {
                    usingSkipFile = true;
                    using (StreamReader sr = new StreamReader(skipFile))
                    {
                        string currentLine = "";
                        do
                        {
                            currentLine = sr.ReadLine();
                            SkipSampleList.Add(currentLine);
                        } while (!sr.EndOfStream);
                    }
                    Console.WriteLine($"SkipFile found, {SkipSampleList.Count} lines read from skip file.");
                }
                else
                {
                    Console.WriteLine($"A Skip file was specified ({skipFile}) however a file of that name was not found.");
                    return false;
                }
            }

            int skipped = 0;
            string line = "";
            try
            {
                using (StreamReader sr = new StreamReader(fileName))
                {
                    while ((line = sr.ReadLine()) != null)
                    {
                        int tabSearch = line.IndexOf('\t');

                        if (tabSearch != -1)  // ignore lines with no tab character
                        {
                            string[] parts = line.Split('\t');

                            if (SkipSampleList.Any(s => s.Contains(parts[0])))
                                skipped++;
                            else
                            {
                                if (parts.Length > 2) // ignore lines with less than three tab separated values
                                {
                                    ItemsCleared4Upload.Add(new ItemCleared4Upload()
                                    {
                                        SampleIdentifier = parts[0],
                                        HtsfSampleName = parts[1],
                                        HtsfExternalCode = parts[2]
                                    });
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                StringBuilder errMsg = new StringBuilder();
                errMsg.Append(Environment.NewLine + Environment.NewLine);
                errMsg.Append("Error processing Upload List input file.");
                errMsg.Append(Environment.NewLine + ex.Message);
                errMsg.Append(Environment.NewLine + Environment.NewLine);
                errMsg.Append("Expected format is tab delimited: sampleIdentifier htsfSampleName htsfExternalCode");
                errMsg.Append(Environment.NewLine + Environment.NewLine);
                errMsg.Append("Offending line:");
                errMsg.Append(Environment.NewLine + Environment.NewLine);
                errMsg.Append(line);
                return false;
            }

            if (usingSkipFile)
                Console.WriteLine(Environment.NewLine +
                    $"Skip File was specified ({skipFile}), found and processed." + Environment.NewLine +
                    $"Number of input records actually skipped = {skipped}" + Environment.NewLine);

            return true;
        }


    }
}
