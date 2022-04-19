import pandas as pd
import os
import numpy as np
import boto3
import multiprocessing as mp # can use this if you need to parallelise
from warcio.archiveiterator import ArchiveIterator
from selectolax.parser import HTMLParser
import swifter
import timeit
import time


def get_text_selectolax(html):
    tree = HTMLParser(html)

    if tree.body is None:
        return None

    for tag in tree.css('script'):
        tag.decompose()
    for tag in tree.css('style'):
        tag.decompose()

    text = tree.body.text(separator='\n')
    return text

def read_doc(record, parser=get_text_selectolax):

    text = None

    html = record.strip()

    if len(html) > 0:
       text = parser(html)

    return text

df = pd.read_csv('top10urls.csv', nrows = 100)


root_key = pd.read_csv('rootkey_p.csv')
# print(root_key)
# df should have these columns: ['warc_filename, warc_record_offset, warc_record_end']
s3 = boto3.client('s3', 'us-east-1',
                aws_access_key_id=  root_key['AWSAccessKeyId'].iloc[0],
                aws_secret_access_key= root_key['AWSSecretKey'].iloc[0])

##################################################################################

def func(row):
    obj = s3.get_object(Bucket='commoncrawl', Key=row['warc_filename'], Range='bytes=%s-%s' % (row['warc_record_offset'], row['warc_record_end']))['Body']
    out_lst = []
    for record in ArchiveIterator(obj):  # once we have the relevant part of the warc file, we need to go though it and parse it
        if record.rec_type == 'response':
            # lst[i] = read_doc(record)
            a = record.content_stream().read()
            # print("parser output")
            out_lst.append(read_doc(a))

    string_format = '\n'.join(out_lst)
    paragraphs = string_format.split("\n")
    nonempty_paragraphs = [paragraph for paragraph in paragraphs if len(paragraph) > 20]
    covid_paragraphs = [paragraph for paragraph in nonempty_paragraphs if "covid" in paragraph]

    return covid_paragraphs

# starttime = timeit.default_timer() #### Time in microsecond
starttime = time.time()
df['text'] = df.apply(lambda row: func(row), axis = 1)
print("Number of subpages crawled:", len(df), "The time difference is :", time.time() - starttime)


df = df[['url', 'url_host_name', 'crawl', 'text']]
df.to_csv('subpages_test.csv.gzip', header=False, compression='gzip')  # saving compressed file or we can overload memory
print(df['text'][5:90])
# my_df = df.groupby(['url_host_name'])['text'].apply(';'.join).reset_index()  # pivoting it so we have all text by website
# my_df.to_csv('hostname_test.csv.gz', header=False, compression='gzip')  # saving compressed file or we can overload memory
# print('finished one!')
# print(my_df)
del df
# del my_df

##################################################################################

# paragraphs = text.split("\n")
# covid_paragraphs = [paragraph for paragraph in paragraphs if paragraph.contains("covid")]

# nonempty_paragraphs = [paragraph for paragraph in paragraphs if len(paragraph) >20]