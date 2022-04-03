import pandas as pd
import numpy as np

# df1 = pd.read_csv('/Users/parnianshahkar/Documents/KOF/Task 15/covid-containing-subpages_aws.csv')
df1 = pd.read_csv('https://www.dropbox.com/s/eaxj3paf2wabeeo/covid-containing-subpages_aws.csv?dl=1')
# df2 = pd.read_csv('/Users/parnianshahkar/Documents/KOF/Task 15/top10subpages_aws.csv')
df2 = pd.read_csv('https://www.dropbox.com/s/5y205ipwntazbsi/top10subpages_aws.csv?dl=1')

df = pd.concat([df1, df2])
print('HELLOO', len(df))
