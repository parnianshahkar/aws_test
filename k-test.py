import pandas as pd
import os
import numpy as np
import boto3
import multiprocessing as mp # can use this if you need to parallelise
from warcio.archiveiterator import ArchiveIterator
from selectolax.parser import HTMLParser

df = pd.read_csv('test_concatenated.csv')


# df should have these columns: ['warc_filename, warc_record_offset, warc_record_end']
s3 = boto3.client('s3', 'us-east-1',
                aws_access_key_id=  'AKIAWGCH75G4N4ZK57SE',
                aws_secret_access_key= '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')

list_of_responses = df.apply(lambda row: s3.get_object(Bucket='commoncrawl', Key=row.warc_filename, Range='bytes=%s-%s' % (row.warc_record_offset, row.warc_record_end)))
print(list_of_responses[0])