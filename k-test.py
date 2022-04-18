import pandas as pd
import os
import numpy as np
import boto3
import multiprocessing as mp # can use this if you need to parallelise
from warcio.archiveiterator import ArchiveIterator
from selectolax.parser import HTMLParser
import swifter


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

df = pd.read_csv('test_concatenated.csv')


root_key = pd.read_csv('rootkey_p.csv')
print(root_key)
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

    return out_lst

df['text'] = df.apply(lambda row: func(row), axis = 1)

print(df['text'])
print(df.columns)

##################################################################################



# list_of_responses = df.apply(lambda row: s3.get_object(Bucket='commoncrawl', Key=row['warc_filename'], Range='bytes=%s-%s' % (row['warc_record_offset'], row['warc_record_end']))['Body'], axis = 1)
#
# # list_of_responses = df.apply(lambda row: , axis = 1)
#
# lst = [None] * (len(list_of_responses))  # empty list to store results
# for i in range(0, len(list_of_responses)):
#     out_lst = []
#     for record in ArchiveIterator(list_of_responses[i]):  # once we have the relevant part of the warc file, we need to go though it and parse it
#         if record.rec_type == 'response':
#             # lst[i] = read_doc(record)
#             a = record.content_stream().read()
#             # print("parser output")
#             out_lst.append(read_doc(a))
#     lst[i] = ';'.join(out_lst)  # now adding stuff to our list
#
# print(lst)
#
#
#
# my_df = pd.DataFrame(list(zip(df['url'], df['url_host_name'], df['crawl'], lst)),
#                      columns=['url', 'url_host_name', 'crawl', 'text'])
# # my_df.to_csv('subpages_separately.csv.gzip', header=False, compression='gzip')  # saving compressed file or we can overload memory
# del lst
# del df
# my_df_2 = my_df.groupby(['url_host_name'])['text'].apply(
#     ';'.join).reset_index()  # pivoting it so we have all text by website
# my_df_2.to_csv('commoncrawl_test.csv.gz', header=False, compression='gzip')  # saving compressed file or we can overload memory
# print('finished one!')
# del my_df
# del my_df_2
#
#
#
