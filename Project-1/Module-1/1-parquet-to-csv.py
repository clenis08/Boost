#!/usr/bin/env python
# coding: utf-8

# In[9]:


import pandas as pd


# In[4]:


df = pd.read_parquet('./data/yellow_tripdata_2024-01.parquet')
df.head(10)


# In[8]:


df.to_csv('nyt.csv', index = False)
print(pd.io.sql.get_schema(df, name = "yellow taxi data"))

