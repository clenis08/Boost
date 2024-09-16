#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


df = pd.read_csv('./data/nyt.csv',nrows = 100)
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
print(pd.io.sql.get_schema(df,name="yellow_taxi_data"))


# In[3]:


from sqlalchemy import create_engine


# In[4]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[5]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data',con=engine))


# In[6]:


df1=pd.read_csv('./data/nyt.csv')


# In[7]:


filas,columnas = df1.shape
print(f'El CSV tiene {filas} filas, {columnas} columnas') 


# In[8]:


df1.describe()


# In[9]:


df_describe = df1.describe().round(2)
print(df_describe)


# In[10]:


df_iter = pd.read_csv('./data/nyt.csv', iterator = True , chunksize = 100000)


# In[11]:


df = next(df_iter)
len(df)


# In[12]:


df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


# In[13]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists= 'replace')


# In[14]:


get_ipython().run_line_magic('time', "df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')")


# In[17]:


from time import time


# In[18]:


try:
    while True: 
        t_start = time()  # Captura el tiempo de inicio del bucle

        try:
            df = next(df_iter)  # Obtiene el siguiente chunk de datos del iterador df_iter
        except StopIteration:
            print("No more data to process.")
            break

        # Convierte las columnas de fechas a tipos datetime si es necesario
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        try:
            # Inserta el chunk actual del DataFrame en la tabla 'yellow_taxi_data' en la base de datos usando SQLAlchemy
            df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
        except Exception as e:
            print(f"Error inserting chunk into database: {e}")
            continue

        t_end = time()  # Captura el tiempo al finalizar la inserción del chunk

        # Imprime el tiempo que tomó insertar el chunk actual
        print(f"Inserted another chunk, took {t_end - t_start:.3f} seconds")

except Exception as e:
    print(f"An error occurred during processing: {e}")

