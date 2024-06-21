import hashlib
import json
import re
from io import BytesIO

import boto3
import oracledb
import requests
import yaml
from oracledb import Connection, Cursor
from requests_ntlm import HttpNtlmAuth

oracledb.init_oracle_client()

with open(r'C:\W10DEV\workspace\mrap-utils\database.yml', 'r') as stream:
    credentials: dict = yaml.safe_load(stream)

login: dict = credentials['production']
auth: dict = credentials['windows']

connection: Connection = oracledb.connect(**login)

session = requests.Session()
session.auth = HttpNtlmAuth(**auth)
session.headers.update({
    'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/65.0.3325.181 Chrome/65.0.3325.181 Safari/537.36"
})


def object_exists(bucket_name, s3_key):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    return sum(1 for _ in bucket.objects.filter(Prefix=s3_key)) > 0


def process_file(data: dict, bucket: str):
    s3_client = boto3.client('s3')
    trim_ref = data['trim_ref']
    refid = data['res_document_no']

    api_id = get_file_id(trim_ref)
    if api_id is None:
        return None

    md5_hash = hashlib.md5()

    api_url = "http://rmweb/CMServiceAPI2/Record/{:d}/file/document"
    response = session.get(api_url.format(api_id), stream=True)
    cd = response.headers['Content-Disposition']
    filename = get_filename_from_cd(cd)

    file_stream = BytesIO()
    for chunk in response.iter_content(chunk_size=8192):
        file_stream.write(chunk)
        md5_hash.update(chunk)

    md5_digest = md5_hash.hexdigest()

    # Reset the stream position to the beginning
    file_stream.seek(0)

    s3_key = md5_digest + "/" + filename
    # Upload the file to S3
    if not object_exists(bucket, md5_digest):
        print("Loading file {} to s3 bucket".format(filename))
        s3_client.upload_fileobj(file_stream, bucket, s3_key)

    json_data = json.dumps(data)

    # Create a BytesIO stream with the JSON data
    file_stream = BytesIO(json_data.encode('utf-8'))

    json_key = md5_digest + "/" + "{}.json".format(refid)
    # Upload the JSON file to S3
    s3_client.upload_fileobj(file_stream, bucket, json_key)

def get_file_id(trim_ref):
    try:
        api = 'http://rmweb/CMServiceAPI2/Record?q=number:{}&format=json'
        response = session.get(api.format(trim_ref))
        json_data = json.loads(response.text)
        if not json_data["Results"]:
            return None
        return json_data["Results"][0]["Uri"]
    except:
        print(json_data)


def get_filename_from_cd(cd):
    if not cd:
        return None
    fname = re.findall('filename=\"(.+)\"', cd)
    if len(fname) == 0:
        return None
    return fname[0]


s3_bucket = "ga-aws-trim-references"

sql = """
        select ed.eno as deposit_eno, ed.entityid as deposit_name,
        ez.eno as zone_eno, ez.entityid as zone_name,
        r.resource_no, to_char(r.record_date, 'YYYY-MM-DD') as record_date, rd.res_document_no, rd.title,
        trim_ref from a.entities ed
        join a.entities ez on ez.parent = ed.eno 
        join ozmin.resources r on r.eno = ez.eno
        join ozmin.resource_documents rd on rd.resource_no =r.resource_no
        where trim_ref is not null 
    """

cursor: Cursor = connection.cursor()

cursor.execute(sql)

records = cursor.fetchall()

data = [dict(zip([a[0].lower() for a in cursor.description], record)) for record in records]

for d in data:
    process_file(d, s3_bucket)
