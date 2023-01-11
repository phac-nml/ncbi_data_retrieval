## Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)
- [Legal](#legal)
- [Contact](#contact)

## Introduction

NCBI performs assembly of specific pathogens on upload as part of their Pathogen Detection portal.  The assemblies are not always deposited into GenBank which makes retrieval of them harder than standard submissions. However, it is possible to retrieve the Skesa assemblies using the tool below. NCBI will change url's from time to time so it is best practice to identify the URL for the accession of interest using their SDL service
with commas separating the accession id's 

https://locate.ncbi.nlm.nih.gov/sdl/2/retrieve?accept-alternate-locations=yes&filetype=sraalign&acc=SRR498276,SRR498277,SRR498278

The service has a limit to the number of accessions which can be queried at one time. In testing, it was found that 100 accessions did not result in any issues regarding rejection of the request.

Once the URL location has been found for a given accession, the assembly can be retrieved using the NCBI vdb-dump tool using the URL identified from the previous step

vdb-dump -T REFERENCE -f fasta2 https://sra-download-nfs.be-md.ncbi.nlm.nih.gov/traces/sra48/SRZ/000498/SRR498276/SRR498276.realign

If there are issues with vdb-dump, try disabling 'enable remote access' on the first tab of their configuration utility.  The tool is finicky with proxy servers, so you may need to contact SRA
support regarding any specific issues you might be having with their vdb-dump tool. In general, update to the latest version as the first step in any troubleshooting.

Using this process, I have created a utility script that can download large numbers of NCBI assemblies using multiple download threads and control over the number of assemblies in each sub folder. It writes out a manifest file with information retrieved from SDL and the path to the fasta file in the download folder.

Hopefully this is helpful for those interested in downloading SKESA assemblies from NCBI Pathogen Detection.

## Installation

Python dependencies (defined in the [requirements](https://github.com/phac-nml/cladeomatic/blob/main/requirements.txt) file, should be automatically installed when using conda or pip)

In addition to the python dependencies, download the latest sra-toolkit https://github.com/ncbi/sra-tools




## Usage
    usage: ncbi_assembly_downloader.py [-h] --acs ACS --outdir OUTDIR [--batch_size BATCH_SIZE] [--folder_size FOLDER_SIZE] [--folder_prefix FOLDER_PREFIX] [--sdl_url SDL_URL] [--force] [--num_threads NUM_THREADS]
    
    Download SKESA Assemblies from NCBI SRA v. 1.0.0
    
    options:
      -h, --help            show this help message and exit
      --acs ACS             Single column list of SRA accessions
      --outdir OUTDIR       output directory
      --batch_size BATCH_SIZE
                            Number of accessions to return
      --folder_size FOLDER_SIZE
                            Maximum number of filers per directory
      --folder_prefix FOLDER_PREFIX
                            Maximum number of files per directory
      --sdl_url SDL_URL     Accesion lookup url
      --force               Force overwite of existing results directory
      --num_threads NUM_THREADS
                            Number of threads to use



**Outputs:**

```
OutputFolderName
├── {prefix}_{0}
    └── {acs.fasta}
    .
    .
    .
├── {prefix}_{n}
    └── {acs.fasta}
└── manifest.txt
```

## Legal

Copyright Government of Canada 2023

Written by: National Microbiology Laboratory, Public Health Agency of Canada

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this work except in compliance with the License. You may obtain a copy of the
License at:

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.


## Contact

**James Robertson**: james.robertson@phac-aspc.gc.ca
