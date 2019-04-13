using System;
using System.Threading;

namespace gdc_client_simulator
{
    // this program simulates the GDC Data Transfer tool to facilitate development of sequence data upload managers
    // simulator returns more file upload "failures" than in reality for dev/test purposes
    // https://gdc.cancer.gov/access-data/gdc-data-transfer-tool

    class Program
    {
        public static bool uploadSuccessful;
        public static string programOutput;

        static void Main(string[] args)
        {
            if (args == null)
            {
                Console.WriteLine("No UUID was supplied");
                return;
            }

            int lowerBound4Sleep;
            int upperBound4Sleep;

            if (args.Length > 1)
            {
                switch (args[1])
                {
                    case "fast":
                        lowerBound4Sleep = 5000;
                        upperBound4Sleep = 10000;
                        break;
                    case "normal":
                        lowerBound4Sleep = 15000;
                        upperBound4Sleep = 30000;
                        break;
                    case "slow": // most similar to actual GDCTool activity
                        lowerBound4Sleep = 200000;
                        upperBound4Sleep = 600000;
                        break;
                    default:
                        lowerBound4Sleep = 3000;
                        upperBound4Sleep = 10000;
                        break;
                }
            }
            else
            {
                lowerBound4Sleep = 3000;
                upperBound4Sleep = 10000;
            }

            string UUID = args[0];

            programOutput = uploadResult(UUID);

            if (uploadSuccessful)
            {
                Random rnd = new Random();
                int sleepyTime = rnd.Next(lowerBound4Sleep, upperBound4Sleep);
                Thread.Sleep(sleepyTime);
            }
            else
            {
                Thread.Sleep(6000); // most failures happen quickly (file not found, file already exists, etc. 
            }
            Console.WriteLine(programOutput);

        }

        public static string uploadResult(string fileID)
        {
            string retval = "";
            Random rnd1 = new Random();
            int result = rnd1.Next(1, 9);

            switch (result)
            {
                case 1:
                    retval = "File in validated state, initiate_multipart not allowed";
                    uploadSuccessful = false;
                    break;
                case 2:
                    retval = "2: this text simulates an unknown type of transfer failure, this UUID should be re-queued: " + fileID;
                    uploadSuccessful = false;
                    break;
                case 3:
                    retval = "Multipart upload finished for file " + fileID;
                    uploadSuccessful = true;
                    break;
                case 4:
                    retval = "Multipart upload finished for file " + fileID;
                    uploadSuccessful = true;
                    break;
                //case 5:
                //    retval = "File with id " + fileID + " not found";
                //    uploadSuccessful = false;
                //    break;
                case 5:
                    retval = "Multipart upload finished for file " + fileID;
                    uploadSuccessful = true;
                    break;
                case 6:
                    retval = "Multipart upload finished for file " + fileID;
                    uploadSuccessful = true;
                    break;
                case 7:
                    retval = "7: this text simulates an unknown type of transfer failure, this UUID should be re-queued: " + fileID;
                    uploadSuccessful = true; // upload is not successful, but saying yes here causes longer wait, simulating a failure further along during upload
                    break;
                case 8:
                    retval = "8: this text simulates an unknown type of transfer failure, this UUID should be re-queued: " + fileID;
                    uploadSuccessful = true;
                    break;
                case 9:
                    retval = "9: this text simulates an unknown type of transfer failure, this UUID should be re-queued: " + fileID;
                    uploadSuccessful = false;
                    break;
                //case 10:
                //    retval = "10: this text simulates an unknown type of transfer failure, this UUID should be re-queued: " + fileID;
                //    uploadSuccessful = true; // upload is not successful, but saying yes here causes longer wait, simulating a failure further along during upload
                //    break;
                default:
                    retval = "Default: this text simulates an unknown type of transfer failure, this UUID should be re-queued: " + fileID;
                    uploadSuccessful = false;
                    break;
            }

            return retval;
        }
    }
}
