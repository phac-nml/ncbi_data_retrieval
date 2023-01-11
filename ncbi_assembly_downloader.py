import pandas as pd
from argparse import (ArgumentParser, ArgumentDefaultsHelpFormatter, RawDescriptionHelpFormatter)
import pycurl
import certifi
from io import BytesIO
import json
import os
import ray
import psutil
import logging
import sys
import collections
from subprocess import Popen, PIPE
from datetime import datetime
import random
import time

SDL_BASE_URL = 'https://locate.ncbi.nlm.nih.gov/sdl/2/retrieve?accept-alternate-locations=yes&filetype=sraalign&acc='
VERSION = '1.0.0'

def parse_args():
    class CustomFormatter(ArgumentDefaultsHelpFormatter, RawDescriptionHelpFormatter):
        pass
    parser = ArgumentParser(
        description="Download SKESA Assemblies from NCBI SRA v. {}".format(VERSION,
        formatter_class=CustomFormatter))

    parser.add_argument('--acs', type=str, required=True, help='Single column list of SRA accessions')
    parser.add_argument('--outdir', type=str, required=True, help='output directory')
    parser.add_argument('--batch_size', type=int, required=False,default=100, help='Number of accessions to return')
    parser.add_argument('--folder_size', type=int, required=False, default=5000, help='Maximum number of filers per directory')
    parser.add_argument('--folder_prefix', type=str, required=False, default='X',
                        help='Maximum number of files per directory')
    parser.add_argument('--sdl_url', type=str, required=False, help='Accesion lookup url')
    parser.add_argument('--force', required=False, help='Force overwite of existing results directory',
                        action='store_true')
    parser.add_argument('--num_threads', type=int, required=False, default=1, help='Number of threads to use')
    return parser.parse_args()

@ray.remote
def query_acs(url):
    #Add a buffer to url requests
    time.sleep(random.randrange(1,10))
    b_obj = BytesIO()
    crl = pycurl.Curl()

    # Set URL value
    crl.setopt(crl.URL, url)

    # Write bytes that are utf-8 encoded
    crl.setopt(crl.WRITEDATA, b_obj)

    # Perform a file transfer
    try:
        crl.perform()
    except:
        return {'response': None, 'body': None}
    response_code = crl.RESPONSE_CODE
    # End curl session
    crl.close()

    # Get the content stored in the BytesIO object (in byte characters)
    get_body = b_obj.getvalue()

    # Decode the bytes stored in get_body to HTML and print the result
    return {'response':response_code,'body':get_body.decode('utf8')}

def read_acs(file):
    if validate_file(file):
        df = pd.read_csv(file, sep="\t", header=0)
        df = df.astype(str)
        return list(df.iloc[:, 0])
    else:
        return []

def validate_file(file):
    '''
    Parameters
    ----------
    file
    Returns
    -------
    '''
    if os.path.isfile(file) and os.path.getsize(file) > 9:
        return True
    else:
        return False

def create_manifest(acs):
    manifest = {}
    for id in acs:
        manifest[id] = {
            'accession':None,
            'size':0,
            'md5':None,
            'modification_date':None,
            'download_date':None,
            'links':{},
            'folder':'',
            'filename': '',
            'is_ok':False
        }
    return manifest

def divide_acs(l, n):
    out = []
    for i in range(0, len(l), n):
        out.append(l[i:i + n])
    return out

def run_command(command):
    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    stdout = stdout.decode('utf-8')
    stderr = stderr.decode('utf-8')
    return stdout, stderr

def parse_json(json_string):
    if json_string is None or len(json_string) == 0:
        return {}
    try:
        json_obj = json.loads(json_string)
    except:
        return {}
    data = {}

    if not 'result' in json_obj:
        print("Error malformed json:{}".format(json_obj))
        return data
    for bundle in json_obj['result']:
        if not 'files' in bundle:
            print("Error malformed json:{}".format(json_obj))
            return data

        for file in bundle['files']:
            acs = file['accession']
            size = file['size']
            md5 = file['md5']
            modification_date = file['modificationDate']
            links = []
            for location in file['locations']:
                if 'link' in location:
                    print("Error malformed json:{}".format(json_obj))
                    links.append(location['link'])
            data[acs] = {
                'accession': acs,
                'size': size,
                'md5':md5,
                'links':links,
                'modification_date':modification_date
            }
    return data

def download_assembly(link,filename):
    cmd = "vdb-dump -T REFERENCE -f fasta2 {}".format(link)
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    stdout = stdout.decode('utf-8')
    stderr = stderr.decode('utf-8')
    fh = open(filename,'w')
    fh.write(stdout)
    fh.close()
    return validate_file(filename)

@ray.remote
def grab_assembly(manifest,acs_ids):
    for acs in acs_ids:
        folder = manifest[acs]['folder']
        links = manifest[acs]['links']
        if len(links) == 0:
            continue
        filename = os.path.join(folder,"{}.fasta".format(acs))
        manifest[acs]['filename'] = filename
        for link in links:
            status = download_assembly(link, filename)
            if status:
                break
        manifest[acs]['is_ok'] = status
        time.sleep(random.randrange(1, 10))


def main():
    args = parse_args()
    acs_file = args.acs
    outdir = args.outdir
    batch_size = args.batch_size
    folder_size = args.folder_size
    folder_prefix = args.folder_prefix
    sdl_url = args.sdl_url
    force = args.force
    num_threads = args.num_threads

    if sdl_url is None:
        sdl_url = SDL_BASE_URL

    # Initialize Ray components
    os.environ['RAY_worker_register_timeout_seconds'] = '60'
    num_cpus = psutil.cpu_count(logical=False)
    if num_threads > num_cpus:
        num_threads = num_cpus

    if not ray.is_initialized():
        ray.init(num_cpus=num_threads)

    #Read acs
    acs = read_acs(acs_file)
    num_acs = len(acs)
    logging.info(
        "Read {} acs from {}".format(
            num_acs,acs_file))
    if num_acs == 0:
        logging.error(
            "Error no accessions found in input file")
        sys.exit()

    manifest = create_manifest(acs)

    if len(manifest) != num_acs:
        logging.warning("Found duplicate accessions in input list: {}".format(
        [item for item, count in collections.Counter(acs).items() if count > 1]))

    num_acs = len(acs)

    # Initialize directories
    if not os.path.isdir(outdir):
        logging.info("Creating download directory {}".format(outdir))
        os.makedirs(outdir, 0o755)
    elif not force:
        logging.error("Error directory {} already exists, if you want to overwrite existing results then specify --force".format(outdir))
        sys.exit()

    num_folders = int(num_acs / folder_size)
    if num_acs % folder_size > 0:
        num_folders+=1


    folders = []
    for i in range(0,num_folders):
        folder = os.path.join(outdir,"{}_{}".format(folder_prefix,i))
        if not os.path.isdir(folder):
            os.makedirs(folder, 0o755)
        folders.append(folder)

    #Query NCBI for the location of the files
    num_queries = int(num_acs / batch_size)
    if num_queries < 1:
        num_queries = 1
    elif num_acs % batch_size > 0:
        num_queries+=1

    unique_acs = sorted(list(manifest.keys()))
    queries = divide_acs(unique_acs, batch_size)

    results = []
    tracker = 0
    for query in queries:
        url = "{}{}".format(sdl_url,",".join(query))
        print("{}\t{}".format(tracker,url))
        tracker += 1
        results.append(query_acs.remote(url))
    del(queries)
    results = ray.get(results)

    #Add results into manifest
    tracker = 0
    for result in results:
        print("Query batch {} response code: {}".format(tracker,result['response']))
        data = parse_json(result['body'])
        for acs in data:
            for k,v in data[acs].items():
                manifest[acs][k] = v
        tracker+=1
    del(results)
    batch_count = 0
    batch_index = 0

    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    for i in range(0,num_acs):
        acs = unique_acs[i]
        batch_count+=1
        manifest[acs]['folder'] = folders[batch_index]
        manifest[acs]['filename'] = os.path.join(folders[batch_index],"{}.fasta".format(acs))
        manifest[acs]['download_date'] = dt_string
        if batch_count == folder_size:
            batch_index+=1
            batch_count = 0

    results = []
    manifest = ray.put(manifest)
    dl_worker_size = int(num_acs / num_threads)
    dl_batches = divide_acs(unique_acs, dl_worker_size )
    for acs_ids in dl_batches:
        results.append(grab_assembly.remote(manifest, acs_ids))
    results = ray.get(results)
    manifest = ray.get(manifest)
    for acs in unique_acs:
        filename = manifest[acs]['filename']
        manifest[acs]['is_ok'] = validate_file(filename)
    pd.DataFrame.from_dict(manifest,orient='index').to_csv(os.path.join(outdir,"mainifest.txt"),header=True,sep="\t",index=False)


# call main function
if __name__ == '__main__':
    main()