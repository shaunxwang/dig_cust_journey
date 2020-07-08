### Import packages


```python
import pandas as pd
import numpy as np
```

### Fetch data from Snowflake into a file on disk in **feather** format
Click [here](https://www.shaunwang.me/post/post_001_snowpy/) for a quick guide on how to pull data from snowflake by using Python

The following cell are commented out because I only needed to fun it once and I have done that


```python
# from snowpy import run_SQL_to_feather
# run_SQL_to_feather('./SQL','data_pages.sql','pages.feather')
# run_SQL_to_feather('./SQL','data_clicks.sql','clicks.feather')
# run_SQL_to_feather('./SQL','data_v_contact.sql','v_contact.feather')
# print('Done!')
```


```python
pd.set_option('max_colwidth',None)
pd.set_option('min_rows', 12)
```


```python
# pd.reset_option("^display")
```

### Load data of page viewed from feather file into memory and print a summary


```python
pg0 = pd.read_feather('./pages.feather')
pg0.info(verbose=True, null_counts=True)
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 6880922 entries, 0 to 6880921
    Data columns (total 17 columns):
     #   Column             Non-Null Count    Dtype         
    ---  ------             --------------    -----         
     0   VISITID            6880922 non-null  object        
     1   PAGEID             6880922 non-null  object        
     2   D_SITE             6880922 non-null  category      
     3   PLATFORM           6880922 non-null  object        
     4   CONTACT_UUID_JF    5411591 non-null  object        
     5   CONTACT_UUID_SDK   6880922 non-null  object        
     6   D_DATE_HOUR_VISIT  6880922 non-null  datetime64[ns]
     7   D_DATE_HOUR_EVENT  6880922 non-null  datetime64[ns]
     8   MIGRATION_FLAG     6880922 non-null  category      
     9   PAGE_NAME          6880915 non-null  object        
     10  FIRST_PAGE_NAME    6880922 non-null  object        
     11  END_PAGE_NAME      6880901 non-null  object        
     12  VISIT_LOGGED       6880922 non-null  category      
     13  D_PAGE             6880922 non-null  object        
     14  D_PAGE_CHAP1       6880913 non-null  category      
     15  D_PAGE_CHAP2       6811048 non-null  object        
     16  D_PAGE_CHAP3       6765013 non-null  object        
    dtypes: category(4), datetime64[ns](2), object(11)
    memory usage: 708.7+ MB


### Data transformation
The following cell are commented out because I only needed to fun it once and I have done that


```python
# pg0['MIGRATION_FLAG'] = pg0['MIGRATION_FLAG'].astype('category')
# pg0['VISIT_LOGGED'] = pg0['VISIT_LOGGED'].astype('category')
# pg0['D_PAGE_CHAP1'] = pg0['D_PAGE_CHAP1'].astype('category')
# pg0.insert(pg0.columns.get_loc('D_SITE')+1 ,'PLATFORM' \
#     ,pg0['D_SITE'].replace(['.+Android.*', '.+[Ii]OS.*', '.+Web.+'] \
#         ,['Android', 'iOS', 'Web'] \
#         ,regex=True))
# pg0['D_SITE'] = pg0['D_SITE'].astype('category')
# pg0.to_feather('./pages.feather')
```

### Getting a smaller sample of pages viewed, slicing on date of visit, to be between 2019-Dec-1 and 2020-Jan-31
We don't need 7 million rows for this exercise


```python
pg1 = \
    pg0.pipe(lambda df: df[df['D_DATE_HOUR_VISIT'].between('2019-12-01','2020-01-31')]) \
    .reset_index(drop=True)
```

### Getting subset of data from the sample in which number of pages viewed per each visit is between 20 and 60


```python
pg1 = pg1.pipe(lambda df: df.loc[df['VISITID'] \
        .isin(df['VISITID'].value_counts().to_frame('PAGE_COUNT') \
            .query("index.str.contains('JF') & PAGE_COUNT.between(20,60)") \
            .index.to_list())]) \
        .reset_index(drop=True).copy()
```

### Printing a summary of the final sample which we will work on
We have about 120,000 rows


```python
pg1.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 124344 entries, 0 to 124343
    Data columns (total 17 columns):
     #   Column             Non-Null Count   Dtype         
    ---  ------             --------------   -----         
     0   VISITID            124344 non-null  object        
     1   PAGEID             124344 non-null  object        
     2   D_SITE             124344 non-null  category      
     3   PLATFORM           124344 non-null  object        
     4   CONTACT_UUID_JF    76891 non-null   object        
     5   CONTACT_UUID_SDK   124344 non-null  object        
     6   D_DATE_HOUR_VISIT  124344 non-null  datetime64[ns]
     7   D_DATE_HOUR_EVENT  124344 non-null  datetime64[ns]
     8   MIGRATION_FLAG     124344 non-null  category      
     9   PAGE_NAME          124342 non-null  object        
     10  FIRST_PAGE_NAME    124344 non-null  object        
     11  END_PAGE_NAME      124323 non-null  object        
     12  VISIT_LOGGED       124344 non-null  category      
     13  D_PAGE             124344 non-null  object        
     14  D_PAGE_CHAP1       124342 non-null  category      
     15  D_PAGE_CHAP2       108905 non-null  object        
     16  D_PAGE_CHAP3       95794 non-null   object        
    dtypes: category(4), datetime64[ns](2), object(11)
    memory usage: 12.8+ MB


#### Producing tables for post


```python
pg1 \
    .pipe(lambda df: df[df['PLATFORM']=='Web']) \
    .loc[:,'D_PAGE_CHAP1'] \
    .value_counts() \
    .to_frame('Num. of pages') \
    .head(12)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Num. of pages</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>email request</th>
      <td>13856</td>
    </tr>
    <tr>
      <th>login</th>
      <td>5631</td>
    </tr>
    <tr>
      <th>registration</th>
      <td>4819</td>
    </tr>
    <tr>
      <th>home</th>
      <td>4703</td>
    </tr>
    <tr>
      <th>error</th>
      <td>4345</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>2539</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>2451</td>
    </tr>
    <tr>
      <th>offer</th>
      <td>2359</td>
    </tr>
    <tr>
      <th>requests</th>
      <td>1927</td>
    </tr>
    <tr>
      <th>booknow</th>
      <td>1263</td>
    </tr>
    <tr>
      <th>concierge chat request</th>
      <td>975</td>
    </tr>
    <tr>
      <th>profile</th>
      <td>795</td>
    </tr>
  </tbody>
</table>
</div>




```python
pg1 \
    .pipe(lambda df: df[df['PLATFORM']=='Web']) \
    .pipe(lambda df: df[df['D_PAGE_CHAP1']=='benefits']) \
    .loc[:,'D_PAGE_CHAP2'] \
    .value_counts() \
    .to_frame('Num. of pages') \
    .head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Num. of pages</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>all</th>
      <td>1751</td>
    </tr>
    <tr>
      <th>hotel</th>
      <td>152</td>
    </tr>
    <tr>
      <th>dining</th>
      <td>139</td>
    </tr>
    <tr>
      <th>shopping</th>
      <td>88</td>
    </tr>
    <tr>
      <th>golf</th>
      <td>34</td>
    </tr>
  </tbody>
</table>
</div>




```python
pg1 \
    .pipe(lambda df: df[df['PLATFORM']=='Web']) \
    .pipe(lambda df: df[df['D_PAGE_CHAP1']=='benefits']) \
    .pipe(lambda df: df[df['D_PAGE_CHAP2']=='hotel']) \
    .loc[:,'D_PAGE_CHAP3'] \
    .value_counts() \
    .to_frame('Num. of pages') \
    .head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Num. of pages</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>tokyo</th>
      <td>4</td>
    </tr>
    <tr>
      <th>mumbai</th>
      <td>3</td>
    </tr>
    <tr>
      <th>london</th>
      <td>2</td>
    </tr>
    <tr>
      <th>taichung</th>
      <td>1</td>
    </tr>
    <tr>
      <th>bali</th>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



#### Creating a function to be pipelined(i.e. chained) with other functions, to move column to the desired loc


```python
def moving_col(df, col_name, col_name_insert_at, after=True):
    if isinstance(df, pd.DataFrame):
        loc = 0
        col = df.pop(col_name)
        if after:
            loc = df.columns.get_loc(col_name_insert_at) + 1
        else:
            loc = df.columns.get_loc(col_name_insert_at)
        df.insert(loc, col_name, col)
        return df
    else:
        return None
```

### Create customer journey

#### 1. Create page number in each visit sorted by event date/time of page viewed ascendingly


```python
pg2 = \
    pg1[['VISITID','PAGEID','D_PAGE_CHAP1']] \
        .sort_values(['VISITID','PAGEID']) \
        .reset_index(drop=True) \
        .assign(PAGE_NUM=lambda df: df.groupby('VISITID').cumcount()+1)
```


```python
def print_results(df):
    df_c = None
    if isinstance(df, pd.DataFrame):
        df_c = df.query('VISITID=="JF - Visa APAC Android - PROD_2019-12-02_000000000000001"') \
            .pipe(lambda df: df.loc[:,'D_PAGE_CHAP1':]).set_index('D_PAGE_CHAP1',drop=True).copy()
        return df_c
    else:
        return df_c
```


```python
print_results(pg2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>PAGE_NUM</th>
    </tr>
    <tr>
      <th>D_PAGE_CHAP1</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>home</th>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>2</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>3</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>4</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>5</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>6</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>7</td>
    </tr>
    <tr>
      <th>home</th>
      <td>8</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>9</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>10</td>
    </tr>
    <tr>
      <th>home</th>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>12</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>13</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>14</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>15</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>16</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>17</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>18</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>19</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>20</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>21</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>22</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>23</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>24</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>25</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>26</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>27</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>28</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>29</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>30</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>31</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>32</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>33</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>34</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>35</td>
    </tr>
    <tr>
      <th>offer</th>
      <td>36</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>37</td>
    </tr>
  </tbody>
</table>
</div>



#### 2. Getting continuous page boolean markers: the start row of each sequence is 1, subsequent row with the same D_PAGE_CHAP1 is 0


```python
pg2 = \
    pg2.assign(CONTINUOUS_PG_BOOL_MARKER=lambda df: \
        ((df['PAGE_NUM'] == 1) \
            | ((df['VISITID'] == df['VISITID'].shift(1)) \
                & (df['D_PAGE_CHAP1'] != df['D_PAGE_CHAP1'].shift(1)))) \
        .astype('int'))
```


```python
print_results(pg2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>PAGE_NUM</th>
      <th>CONTINUOUS_PG_BOOL_MARKER</th>
    </tr>
    <tr>
      <th>D_PAGE_CHAP1</th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>home</th>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>2</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>3</td>
      <td>0</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>4</td>
      <td>0</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>5</td>
      <td>0</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>6</td>
      <td>1</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>7</td>
      <td>1</td>
    </tr>
    <tr>
      <th>home</th>
      <td>8</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>9</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>10</td>
      <td>0</td>
    </tr>
    <tr>
      <th>home</th>
      <td>11</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>12</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>13</td>
      <td>0</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>14</td>
      <td>0</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>15</td>
      <td>1</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>16</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>17</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>18</td>
      <td>0</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>19</td>
      <td>0</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>20</td>
      <td>0</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>21</td>
      <td>0</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>22</td>
      <td>0</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>23</td>
      <td>0</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>24</td>
      <td>0</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>25</td>
      <td>0</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>26</td>
      <td>0</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>27</td>
      <td>0</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>28</td>
      <td>1</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>29</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>30</td>
      <td>1</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>31</td>
      <td>1</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>32</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>33</td>
      <td>1</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>34</td>
      <td>1</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>35</td>
      <td>0</td>
    </tr>
    <tr>
      <th>offer</th>
      <td>36</td>
      <td>1</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>37</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



#### 3. Then, cumsum within each visit so that each member row (including the start row) of a sequence would be assigned with the same number but different to the number assigned to subsequent sequences


```python
pg2 = pg2.assign(PG_SQUNC_GROUP_MARKER=lambda df: df['CONTINUOUS_PG_BOOL_MARKER'].cumsum())
```


```python
print_results(pg2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>PAGE_NUM</th>
      <th>CONTINUOUS_PG_BOOL_MARKER</th>
      <th>PG_SQUNC_GROUP_MARKER</th>
    </tr>
    <tr>
      <th>D_PAGE_CHAP1</th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>home</th>
      <td>1</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>2</td>
      <td>1</td>
      <td>2</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>3</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>4</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>5</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>6</td>
      <td>1</td>
      <td>3</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>7</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>home</th>
      <td>8</td>
      <td>1</td>
      <td>5</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>9</td>
      <td>1</td>
      <td>6</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>10</td>
      <td>0</td>
      <td>6</td>
    </tr>
    <tr>
      <th>home</th>
      <td>11</td>
      <td>1</td>
      <td>7</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>12</td>
      <td>1</td>
      <td>8</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>13</td>
      <td>0</td>
      <td>8</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>14</td>
      <td>0</td>
      <td>8</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>15</td>
      <td>1</td>
      <td>9</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>16</td>
      <td>1</td>
      <td>10</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>17</td>
      <td>1</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>18</td>
      <td>0</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>19</td>
      <td>0</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>20</td>
      <td>0</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>21</td>
      <td>0</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>22</td>
      <td>0</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>23</td>
      <td>0</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>24</td>
      <td>0</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>25</td>
      <td>0</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>26</td>
      <td>0</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>27</td>
      <td>0</td>
      <td>11</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>28</td>
      <td>1</td>
      <td>12</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>29</td>
      <td>1</td>
      <td>13</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>30</td>
      <td>1</td>
      <td>14</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>31</td>
      <td>1</td>
      <td>15</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>32</td>
      <td>1</td>
      <td>16</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>33</td>
      <td>1</td>
      <td>17</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>34</td>
      <td>1</td>
      <td>18</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>35</td>
      <td>0</td>
      <td>18</td>
    </tr>
    <tr>
      <th>offer</th>
      <td>36</td>
      <td>1</td>
      <td>19</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>37</td>
      <td>1</td>
      <td>20</td>
    </tr>
  </tbody>
</table>
</div>



#### 4. Create rank by row within each group as marked by PG_SQUNC_GROUP_MARKER


```python
pg2 = pg2.assign(PG_SQUNC_GROUP_RANK=lambda df: \
          df.groupby(['VISITID','PG_SQUNC_GROUP_MARKER'])['PG_SQUNC_GROUP_MARKER'] \
               .cumcount().astype('int')+1)
```


```python
print_results(pg2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>PAGE_NUM</th>
      <th>CONTINUOUS_PG_BOOL_MARKER</th>
      <th>PG_SQUNC_GROUP_MARKER</th>
      <th>PG_SQUNC_GROUP_RANK</th>
    </tr>
    <tr>
      <th>D_PAGE_CHAP1</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>home</th>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>2</td>
      <td>1</td>
      <td>2</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>3</td>
      <td>0</td>
      <td>2</td>
      <td>2</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>4</td>
      <td>0</td>
      <td>2</td>
      <td>3</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>5</td>
      <td>0</td>
      <td>2</td>
      <td>4</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>6</td>
      <td>1</td>
      <td>3</td>
      <td>1</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>7</td>
      <td>1</td>
      <td>4</td>
      <td>1</td>
    </tr>
    <tr>
      <th>home</th>
      <td>8</td>
      <td>1</td>
      <td>5</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>9</td>
      <td>1</td>
      <td>6</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>10</td>
      <td>0</td>
      <td>6</td>
      <td>2</td>
    </tr>
    <tr>
      <th>home</th>
      <td>11</td>
      <td>1</td>
      <td>7</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>12</td>
      <td>1</td>
      <td>8</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>13</td>
      <td>0</td>
      <td>8</td>
      <td>2</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>14</td>
      <td>0</td>
      <td>8</td>
      <td>3</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>15</td>
      <td>1</td>
      <td>9</td>
      <td>1</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>16</td>
      <td>1</td>
      <td>10</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>17</td>
      <td>1</td>
      <td>11</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>18</td>
      <td>0</td>
      <td>11</td>
      <td>2</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>19</td>
      <td>0</td>
      <td>11</td>
      <td>3</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>20</td>
      <td>0</td>
      <td>11</td>
      <td>4</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>21</td>
      <td>0</td>
      <td>11</td>
      <td>5</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>22</td>
      <td>0</td>
      <td>11</td>
      <td>6</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>23</td>
      <td>0</td>
      <td>11</td>
      <td>7</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>24</td>
      <td>0</td>
      <td>11</td>
      <td>8</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>25</td>
      <td>0</td>
      <td>11</td>
      <td>9</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>26</td>
      <td>0</td>
      <td>11</td>
      <td>10</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>27</td>
      <td>0</td>
      <td>11</td>
      <td>11</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>28</td>
      <td>1</td>
      <td>12</td>
      <td>1</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>29</td>
      <td>1</td>
      <td>13</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>30</td>
      <td>1</td>
      <td>14</td>
      <td>1</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>31</td>
      <td>1</td>
      <td>15</td>
      <td>1</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>32</td>
      <td>1</td>
      <td>16</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>33</td>
      <td>1</td>
      <td>17</td>
      <td>1</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>34</td>
      <td>1</td>
      <td>18</td>
      <td>1</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>35</td>
      <td>0</td>
      <td>18</td>
      <td>2</td>
    </tr>
    <tr>
      <th>offer</th>
      <td>36</td>
      <td>1</td>
      <td>19</td>
      <td>1</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>37</td>
      <td>1</td>
      <td>20</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



#### 5. Create group size of each sequence and assign the group size to all members of the sequence


```python
pg2 = pg2.assign(PG_SQUNC_GROUP_SIZE=lambda df: \
          df.groupby(['VISITID','PG_SQUNC_GROUP_MARKER'])['PG_SQUNC_GROUP_MARKER'] \
              .transform('size'))
```


```python
print_results(pg2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>PAGE_NUM</th>
      <th>CONTINUOUS_PG_BOOL_MARKER</th>
      <th>PG_SQUNC_GROUP_MARKER</th>
      <th>PG_SQUNC_GROUP_RANK</th>
      <th>PG_SQUNC_GROUP_SIZE</th>
    </tr>
    <tr>
      <th>D_PAGE_CHAP1</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>home</th>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>2</td>
      <td>1</td>
      <td>2</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>3</td>
      <td>0</td>
      <td>2</td>
      <td>2</td>
      <td>4</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>4</td>
      <td>0</td>
      <td>2</td>
      <td>3</td>
      <td>4</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>5</td>
      <td>0</td>
      <td>2</td>
      <td>4</td>
      <td>4</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>6</td>
      <td>1</td>
      <td>3</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>7</td>
      <td>1</td>
      <td>4</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>home</th>
      <td>8</td>
      <td>1</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>9</td>
      <td>1</td>
      <td>6</td>
      <td>1</td>
      <td>2</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>10</td>
      <td>0</td>
      <td>6</td>
      <td>2</td>
      <td>2</td>
    </tr>
    <tr>
      <th>home</th>
      <td>11</td>
      <td>1</td>
      <td>7</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>12</td>
      <td>1</td>
      <td>8</td>
      <td>1</td>
      <td>3</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>13</td>
      <td>0</td>
      <td>8</td>
      <td>2</td>
      <td>3</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>14</td>
      <td>0</td>
      <td>8</td>
      <td>3</td>
      <td>3</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>15</td>
      <td>1</td>
      <td>9</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>16</td>
      <td>1</td>
      <td>10</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>17</td>
      <td>1</td>
      <td>11</td>
      <td>1</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>18</td>
      <td>0</td>
      <td>11</td>
      <td>2</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>19</td>
      <td>0</td>
      <td>11</td>
      <td>3</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>20</td>
      <td>0</td>
      <td>11</td>
      <td>4</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>21</td>
      <td>0</td>
      <td>11</td>
      <td>5</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>22</td>
      <td>0</td>
      <td>11</td>
      <td>6</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>23</td>
      <td>0</td>
      <td>11</td>
      <td>7</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>24</td>
      <td>0</td>
      <td>11</td>
      <td>8</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>25</td>
      <td>0</td>
      <td>11</td>
      <td>9</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>26</td>
      <td>0</td>
      <td>11</td>
      <td>10</td>
      <td>11</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>27</td>
      <td>0</td>
      <td>11</td>
      <td>11</td>
      <td>11</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>28</td>
      <td>1</td>
      <td>12</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>29</td>
      <td>1</td>
      <td>13</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>30</td>
      <td>1</td>
      <td>14</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>make a request</th>
      <td>31</td>
      <td>1</td>
      <td>15</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>email request</th>
      <td>32</td>
      <td>1</td>
      <td>16</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>benefits</th>
      <td>33</td>
      <td>1</td>
      <td>17</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>34</td>
      <td>1</td>
      <td>18</td>
      <td>1</td>
      <td>2</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>35</td>
      <td>0</td>
      <td>18</td>
      <td>2</td>
      <td>2</td>
    </tr>
    <tr>
      <th>offer</th>
      <td>36</td>
      <td>1</td>
      <td>19</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>cities</th>
      <td>37</td>
      <td>1</td>
      <td>20</td>
      <td>1</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



#### 6. Filtering rows to the ones which only have PG_SQUNC_GROUP_RANK as 1, PG_SQUNC_GROUP_RANK = 1 points to the first occurance of charpter 1 in a group of charpter 1's which are the same as the the first occurance; then create charpter 1 customer journey; and create step depth for each point in charpter 1 customer journey


```python
pg_ch1_journey = \
    pg2.pipe(lambda df: df[df['PG_SQUNC_GROUP_RANK']==1]) \
        .assign(PG_SQUNC_GROUP_FIRST_OCCUR_NUM=lambda df: \
            df.groupby('VISITID').cumcount()+1) \
        .assign(CAT_STRING=lambda df: \
            '[' + df['PG_SQUNC_GROUP_FIRST_OCCUR_NUM'].astype('str') + '] ') \
        .assign(TO_FORM_LIST=lambda df: \
            df['CAT_STRING'].str.cat(df['D_PAGE_CHAP1'])) \
        .pipe(lambda df: df.groupby('VISITID')['TO_FORM_LIST'].agg(list) \
            .to_frame('CHAP1_JOURNEY') \
            .join(df.groupby('VISITID')['PG_SQUNC_GROUP_SIZE'].agg(list) \
                .to_frame('CHAP1_JOURNEY_STEP_DEPTH')))
```


```python
pg_ch1_journey.reset_index(drop=True).head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CHAP1_JOURNEY</th>
      <th>CHAP1_JOURNEY_STEP_DEPTH</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>[[1] home, [2] benefits, [3] make a request, [4] email request, [5] home, [6] benefits, [7] home, [8] benefits, [9] make a request, [10] email request, [11] benefits, [12] make a request, [13] email request, [14] benefits, [15] make a request, [16] email request, [17] benefits, [18] cities, [19] offer, [20] cities]</td>
      <td>[1, 4, 1, 1, 1, 2, 1, 3, 1, 1, 11, 1, 1, 1, 1, 1, 1, 2, 1, 1]</td>
    </tr>
    <tr>
      <th>1</th>
      <td>[[1] benefits, [2] contents, [3] benefits, [4] offer, [5] error, [6] offer, [7] home, [8] benefits, [9] offer, [10] error, [11] offer, [12] home]</td>
      <td>[3, 1, 1, 3, 1, 2, 1, 1, 3, 1, 2, 1]</td>
    </tr>
    <tr>
      <th>2</th>
      <td>[[1] home, [2] login, [3] legal, [4] help, [5] registration, [6] help, [7] home, [8] help]</td>
      <td>[1, 4, 2, 1, 2, 1, 1, 13]</td>
    </tr>
    <tr>
      <th>3</th>
      <td>[[1] home, [2] login, [3] legal, [4] registration, [5] legal, [6] help, [7] home, [8] help, [9] login, [10] help, [11] registration, [12] help]</td>
      <td>[1, 3, 1, 2, 1, 1, 1, 5, 1, 1, 1, 3]</td>
    </tr>
    <tr>
      <th>4</th>
      <td>[[1] home, [2] login, [3] home, [4] benefits, [5] home, [6] highlights, [7] offer, [8] home, [9] cities, [10] offer, [11] cities, [12] offer, [13] home, [14] requests, [15] make a request, [16] concierge chat request, [17] requests, [18] make a request, [19] requests, [20] concierge chat request, [21] requests, [22] home, [23] login, [24] home, [25] make a request, [26] concierge chat request, [27] home]</td>
      <td>[1, 1, 2, 1, 1, 1, 1, 1, 3, 2, 3, 7, 1, 7, 1, 1, 1, 1, 1, 1, 1, 1, 3, 1, 1, 1, 1]</td>
    </tr>
  </tbody>
</table>
</div>


