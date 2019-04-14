<p>Manage uploads of sesquence data to the NIH Genomic Data Commons using the gdc data transfer tool.</p>

This Win/Linux/Mac console application is a wrapper for the GDC Data Transfer Tool (gdc-client). <br>
It manages uploads of genomic sequence data files to the National Cancer Institute.<br>
It requires that the data files are accessible via a file path from the OS upon which it runs.<br>
It is known to work on rc-dm2.its.unc.edu, an ITS-RC datamover node with the .net core sdk installed.<br>
https://gdc.cancer.gov/access-data/gdc-data-transfer-tool

USAGE: <br>
dotnet uoload2gdc.dll --help<br>
dotnet upload2gdc.dll --ur ~/gdc-upload-report.tsv --md ~/gdc-metadata-file.json --files /proj/seq/tracseq/delivery --token ~/token.txt<br>


<p>To see if sequence data files are in place based on the json file generated via the TracSeq API:<br>
dotnet upload2gdc.dll --md 1test-md.json --filesonly</p>