import pandas as pd
import numpy as np

df1 = pd.read_csv('/Users/parnianshahkar/Documents/KOF/Task 15/covid-containing-subpages_aws.csv')
df2 = pd.read_csv('/Users/parnianshahkar/Documents/KOF/Task 15/top10subpages_aws.csv')

df = pd.concat([df1, df2])
print(len(df))
