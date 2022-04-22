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

# covid_synonyms = ["corona pandemic ", "corona", "covid 19", "coronavirus", "coronapandemie "           	OR  "coronakrise  "   OR  "SARS CoV 2  "      	OR  "Wuhan virus "     	OR  "pandemie "         	OR  "pandemic  "         	OR  "2019 nCoV " 	OR  "pandémie  "        	OR  "pandemia"  OR "Koronapandemie"           	OR  "Korona"	OR  "covid 19"     	OR  "Coronavirus"      	OR  "Coronapandemie" OR  "Coronakrise"      	OR  "SARS CoV 2"        	OR  "Wuhan-Virus"	OR  "Pandemie"          	OR  "Pandemie"          	OR  "2019 nCoV"         	OR  "Pandémie"          	OR  "Pandemie" "pandémie corona" OR  "corona" 	OR  "Covid 19"  OR  "coronavirus"       	OR  "coronapandemie" OR  "coronakrise"	OR  "SARS CoV 2"        	OR  "Virus de Wuhan"   OR  "pandémie"          	OR  "pandémie"          	OR  "NCoV 2019"  	OR  "pandémie"          	OR  "pandémie" "pandemia de corona" OR  "corona" 	OR  "covid 19"  OR  "coronavirus"	OR  "coronapandemia" OR  "coronakrise"       	OR  "SARS CoV 2"        	OR  "Virus de Wuhan"   OR  "pandemia"    	OR  "pandemia"          	OR  "2019 nCoV"         	OR  "pandémie"          	OR  "pandemia" "pandemia de corona"      	OR  "corona" 	OR  "cobiçado 19"       	OR  "coronavírus"       	OR  "coronapandemie" OR  "coronakrise"	OR  "SARS CoV 2"        	OR  "Vírus Wuhan"     	OR  "pandemie"          	OR  "pandemia"          	OR  "2019 nCoV"  	OR  "pandémie"          	OR  "pandemia" "电晕大流行" OR  "电晕"     	OR  "covid 19"  OR  "冠状病毒"     	OR  "冠状流感"           	,  "日冕"     	,  "SARS CoV 2"        	,  "武汉病毒"           	,  "大流行"	,  "大流行"           	,  "2019年nCoV"   	,  "Pandémie"          	,  "大流行" "कोरोना महामारी" ,  "कोरोना" 	,  "कोविद १ ९"    	,  "कोरोना" 	,  "ओ और ndemie"	,  "ओ nakrise"          	,  "SARS CoV 2"        	,  "वुहान वायरस" ,  "ndemie"   ,  "महामारी"   ,  "2019 nCoV"         	,  "pandémie"          	,  "pandémie" , "コロナパンデミック" ,  "コロナ"	,  "covid 19"  ,  "コロナウイルス" ,  "コロナパンデミ" ,  "コロナクリス"            	,  "SARS CoV 2"        	,  "武漢ウイルス" 	,  "パンデミー"      	,  "パンデミック" 	,  "2019 nCoV"   ,  "パンデミー"      	,  "パンデミック" , "جائحة الاكليل"   ,  "الاكليل"    	,  "مطمع 19"   ,  "الفيروس التاجي"	,  "التاج"       	,  "التاج"       	,  "السارس CoV 2"      	,  "فيروس ووهان"        	,  "بانديمي" , "جائحة"            	,  "2019 nCoV"         	,  "بانديمي"  	,  "جائحة" ,"пандемия короны" ,"корона" ,"Ковид 19" ,"коронавирус" ,"ш и ndemie" ,"о в nakrise" ,"SARS CoV 2" ,"Уханьский вирус", "ndemie" ,"пандемия" ,"2019 нКоВ" ,"pandémie" ,"ndemia"]

covid_synonyms = ["corona pandemic " , "corona" 	, "covid 19"  , "coronavirus"       	, "coronapandemie "           	, "coronakrise  "   , "SARS CoV 2  "      	, "Wuhan virus "     	, "pandemie "         	, "pandemic  "         	, "2019 nCoV " 	, "pandémie  "        	, "pandemia"  ,"Koronapandemie"           	, "Korona"	, "covid 19"     	, "Coronavirus"      	, "Coronapandemie" , "Coronakrise"      	, "SARS CoV 2"        	, "Wuhan-Virus"	, "Pandemie"          	, "Pandemie"          	, "2019 nCoV"         	, "Pandémie"          	, "Pandemie" "pandémie corona" , "corona" 	, "Covid 19"  , "coronavirus"       	, "coronapandemie" , "coronakrise"	, "SARS CoV 2"        	, "Virus de Wuhan"   , "pandémie"          	, "pandémie"          	, "NCoV 2019"  	, "pandémie"          	, "pandémie" "pandemia de corona" , "corona" 	, "covid 19"  , "coronavirus"	, "coronapandemia" , "coronakrise"       	, "SARS CoV 2"        	, "Virus de Wuhan"   , "pandemia"    	, "pandemia"          	, "2019 nCoV"         	, "pandémie"          	, "pandemia" "pandemia de corona"      	, "corona" 	, "cobiçado 19"       	, "coronavírus"       	, "coronapandemie" , "coronakrise"	, "SARS CoV 2"        	, "Vírus Wuhan"     	, "pandemie"          	, "pandemia"          	, "2019 nCoV"  	, "pandémie"          	, "pandemia" "电晕大流行" , "电晕"     	, "covid 19"  , "冠状病毒"     	, "冠状流感"           	, "日冕"     	, "SARS CoV 2"        	, "武汉病毒"           	, "大流行"	, "大流行"           	, "2019年nCoV"   	, "Pandémie"          	, "大流行" "कोरोना महामारी" , "कोरोना" 	, "कोविद १ ९"    	, "कोरोना" 	, "ओ और ndemie"	, "ओ nakrise"          	, "SARS CoV 2"        	, "वुहान वायरस" , "ndemie"   , "महामारी"   , "2019 nCoV"         	, "pandémie"          	, "pandémie" ,"コロナパンデミック" , "コロナ"	, "covid 19"  , "コロナウイルス" , "コロナパンデミ" , "コロナクリス"            	, "SARS CoV 2"        	, "武漢ウイルス" 	, "パンデミー"      	, "パンデミック" 	, "2019 nCoV"   , "パンデミー"      	, "パンデミック" ,"جائحة الاكليل"   , "الاكليل"    	, "مطمع 19"   , "الفيروس التاجي"	, "التاج"       	, "التاج"       	, "السارس CoV 2"      	, "فيروس ووهان"        	, "بانديمي"  	, "جائحة"            	, "2019 nCoV"         	, "بانديمي"  	, "جائحة" ,"пандемия короны" ,          	"корона" ,            	"Ковид 19" ,"коронавирус" ,     	"ш и ndemie" ,        	"о в nakrise" ,           	"SARS CoV 2" ,            	"Уханьский вирус" ,"ndemie" , 	"пандемия" ,           	"2019 нКоВ" ,          	"pandémie" ,            	"ndemia"]

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

df = pd.read_csv('covid_containing_urls.csv', nrows = 100)


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
    nonempty_paragraphs = [paragraph for paragraph in paragraphs if len(paragraph) > 2]
    covid_paragraphs = [paragraph for paragraph in nonempty_paragraphs if any(ext in paragraph for ext in covid_synonyms)]
    # string_format = '\n'.join(paragraphs)

    return covid_paragraphs

# starttime = timeit.default_timer() #### Time in microsecond
starttime = time.time()
df['text'] = df.apply(lambda row: func(row), axis = 1)
print("Number of subpages crawled:", len(df), "The time difference is :", time.time() - starttime)


df = df[['url', 'url_host_name', 'crawl', 'text']]
df.to_csv('subpages_test.csv.gzip', header=False, compression='gzip')  # saving compressed file or we can overload memory
df['covid_paragraphs'] = df.apply(lambda row: len(row['text']), axis = 1)
print(df[['text', 'covid_paragraphs']])
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