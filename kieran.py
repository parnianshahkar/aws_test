import pandas as pd
import os
import numpy as np
import boto3
import multiprocessing as mp # can use this if you need to parallelise
from warcio.archiveiterator import ArchiveIterator
from selectolax.parser import HTMLParser

df = pd.read_csv('concatenated.csv')


# df should have these columns: ['warc_filename, warc_record_offset, warc_record_end']


list_of_responses = df.apply(lambda row: s3.get_object(Bucket='commoncrawl', Key=row.warc_filename, Range='bytes=%s-%s' % (row.warc_record_offset, row.warc_record_end))


warc_filename, warc_record_offset, warc_record_end = download_input[i].split(";") # putting these in correct format
warc_record_offset, warc_record_end = int(warc_record_offset), int(warc_record_end)
resp = s3.get_object(Bucket='commoncrawl', Key=warc_filename, Range='bytes=%s-%s' % (warc_record_offset, warc_record_end))

print(resp[0])

# turn html to clear text
import html2text
html = open("foobar.html").read()
print html2text.html2text(html)

########################################################################################################################
# Extraction functions
########################################################################################################################

def http_get_2(download_input:str): # function to download from s3 based on position in warc file
    lst = [None] * (len(download_input)) # empty list to store results

    for i in range(0, len(download_input)):
        warc_filename, warc_record_offset, warc_record_end = download_input[i].split(";") # putting these in correct format
        warc_record_offset, warc_record_end = int(warc_record_offset), int(warc_record_end)
        resp = s3.get_object(Bucket='commoncrawl', Key=warc_filename, Range='bytes=%s-%s' % (warc_record_offset, warc_record_end))['Body'] # downloading
        out_lst = []
        for record in ArchiveIterator(resp): # once we have the relevant part of the warc file, we need to go though it and parse it
            if record.rec_type == 'response':
                #lst[i] = read_doc(record)
                a = record.content_stream().read()
                #print("parser output")
                out_lst.append(read_doc(a))
        lst[i] = ';'.join(out_lst) # now adding stuff to our list
    #print(lst)

    return(lst)


# code to save each chunk of the dataframe with a specific name
def save_chunk(df):

    nm = 'commoncrawl_' + str(min(df.index))+ '_' + str(max(df.index)) + '.csv.gz'
    # applyinng the download function just to the strings for efficiency
    # lets turn this just into a vector of strings
    download_input = df["warc_filename"] + ';' + df['warc_record_offset'].apply(str) + ";" + df['warc_record_end'].apply(str)
    warc_list = http_get_2(download_input) # getting the files
    my_df = pd.DataFrame(list(zip(df['url'], df['url_host_name'], df['crawl'], warc_list)), columns = ['url', 'url_host_name', 'crawl', 'text'])
    # now creating something we can save and output
    my_df_2 = my_df.groupby(['url_host_name'])['text'].apply(';'.join).reset_index() # pivoting it so we have all text by website
    my_df_2.to_csv(nm, header=False, compression='gzip') # saving compressed file or we can overload memory
    print('finished one!')
    del my_df
    del warc_list
    return(nm)


########################################################################################################################
# Parser functions
########################################################################################################################

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

########################################################################################################################
# Running code
########################################################################################################################

df = pd.read_csv('swiss_1_lighter.csv')
#df = pd.read_csv('swiss_1_2.zip', warn_bad_lines=True,error_bad_lines=False,\ # if you need to pass a gz file, do this
#compression='zip', header=0, index_col=False)

s3 = boto3.client('s3', 'us-east-1',
                aws_access_key_id=  'YOUR ID',
                aws_secret_access_key= 'YOUR KEY')

save_chunk(df) # applying function to our df



###### My older version:
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