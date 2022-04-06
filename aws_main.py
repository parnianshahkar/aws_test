import pandas as pd


# m1 = urlrq.urlopen('https://www.dropbox.com/s/eaxj3paf2wabeeo/covid-containing-subpages_aws.csv?dl=1', context=ssl.create_default_context(cafile=certifi.where()))
# print(m1)
# df1 = pd.read_csv(m1)[:100]
# print(df1)
# df2 = pd.read_csv(urlrq.urlopen('https://www.dropbox.com/s/5y205ipwntazbsi/top10subpages_aws.csv?dl=1', context=ssl.create_default_context(cafile=certifi.where())))[:100]

# df1 = pd.read_csv('/Users/parnianshahkar/Documents/KOF/Task 15/covid-containing-subpages_aws.csv')
# df1 = pd.read_csv('https://www.dropbox.com/s/eaxj3paf2wabeeo/covid-containing-subpages_aws.csv?dl=1')
df1 = pd.read_csv('test.csv')
df2 = pd.read_csv('other_test.csv')
print('len(df1) + len(df2)', len(df1) + len(df2))
df = pd.concat([df1, df2])
del df1
del df2
df_new = df.drop_duplicates(subset=['url', 'url_host_name', 'url_host_registered_domain'], keep='first')
del df
df_new.to_csv('concatenated.csv')
# df2 = pd.read_csv('/Users/parnianshahkar/Documents/KOF/Task 15/top10subpages_aws.csv')
# df2 = pd.read_csv('https://www.dropbox.com/s/5y205ipwntazbsi/top10subpages_aws.csv?dl=1')[:100]


print('\n len(concatenated)', len(df_new))
