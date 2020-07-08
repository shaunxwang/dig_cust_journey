## Introduction
We are increasingly replying on digital devices for our daily lives, the convenience we gain from using them has undoubtedly revolutionised our lifestyles but at the same time data collection has also evolved with it. 

Digital footprint at every step is tracked and data is collected for analysis. There are many types of data wrangling we can do with digital tracking data:

 - On the user level:
   - Number of visitors to a website
   - User behavior such as click rate and bounce rate
   - Conversions
   - Customer journeys
 - On the website level
   - Visibility
   - OnPage Factors
   - Backlink analysis and monitoring
 - Shopping and e-commerce level:
   - Conversion-Tracking
   - Tracking and monitoring in email campaigns
   - Cross-selling campaigns
 - Online-advertising:
   - Campaign tracking
   - Tracking affiliate links

## The Tools
What interests us here is **customer journey**, more specifically a visitor's journey in each visit to an app or website, derived from pages viewed during the visit.

The tracking/tagging platform used here is not **Google Analytics**, that's right, some competition in the field is always welcomed. Here, I will featured the new kid on the block, [AT Internet](https://developers.atinternet-solutions.com/home/).

AT Internet's tracker is  initialised done via **JavaScript** methods which is inserted into ```html``` like so:
```html
<html>
  <head lang="en">
    <meta charset="UTF-8">
    <title>My Page</title>
    <script src="smarttag.js"></script>
  </head>
  <body>
    ...
    <script type="text/javascript">      
      var tag = new ATInternet.Tracker.Tag();
    </script>
  </body>
</html>
```
And the following shows an example of a **complete tagging**:
``` javascript
var tag = new ATInternet.Tracker.Tag();
tag.page.send({
    name:'pageName',
    chapter1:'chap1',
    chapter2:'chap2',
    chapter3:'chap3',
    level2:'123',
    customObject: {
        param1: 'val1',
        param2: 'val2'
    },
    event: anEvent,
    callback: function(){}
});
```
## The Data
There are a lot of data collected from a tag like the above, but what interests us here are the following columns:
 - Visit ID of each visit
 - Page ID of each page viewed
 - [SDK ID](https://developers.atinternet-solutions.com/android-en/users-android-en/user-id-android-en/) of each visitor
 - Chapter 1 of each page viewed
 - Chapter 2 of each page viewed
 - Chapter 3 of each page viewed
 - D_Page a.k.a. "hit"
   - Usually in the format of: "chapter1::chapter2::chapter3::page_name"
 - Event date/time of a page being viewed
 - Start date/time of a visit

Yes, it's very confusing!

You may ask what **chapter 1**, **chapter 2** and **chapter 3** are?

A quick look at **chapter 1** for platform of **website**:
```python
pg1 \
    .pipe(lambda df: df[df['PLATFORM']=='Web']) \
    .loc[:,'D_PAGE_CHAP1'] \
    .value_counts() \
    .to_frame('Num. of pages') \
    .head(12)
```
yields:

|       ( Top 12 )       | Num. of pages |
|------------------------|---------------|
|      email request     | 13856         |
|          login         | 5631          |
|      registration      | 4819          |
|          home          | 4703          |
|          error         | 4345          |
|         cities         | 2539          |
|        benefits        | 2451          |
|          offer         | 2359          |
|        requests        | 1927          |
|         booknow        | 1263          |
| concierge chat request | 975           |
|         profile        | 795           |

I agree, no, it doesn't explain anything but at least we know there is a **chapter 1** called **benefits**.

Let's say that the marketing/partnership department wants to know how the **benefits** to our customers are doing:
```python
pg1 \
    .pipe(lambda df: df[df['PLATFORM']=='Web']) \
    .pipe(lambda df: df[df['D_PAGE_CHAP1']=='benefits']) \
    .loc[:,'D_PAGE_CHAP2'] \
    .value_counts() \
    .to_frame('Num. of pages')
```
yields:

| ( Top 5 ) | Num. of pages |
|-----------|---------------|
|    all    | 1751          |
|   hotel   | 152           |
|   dining  | 139           |
|  shopping | 88            |
|    golf   | 34            |

We can see that **chapter 2** is **benefit category**.

Then, the marketing/partnership department wants to know how **benefits** in **hotel** are doing:
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
yields:

|   ( Top 5 ) | Num. of pages |
|-------------|---------------|
|    tokyo    | 4             |
|    mumbai   | 3             |
|    london   | 2             |
|     bali    | 1             |
| los angeles | 1             |

We can see that **chapter 3** is the location of **benefits**.

But the the marketing/partnership department wants to know more, here is where **customer journey** comes in handy.

We can study where **customer journey** "forks" to after customer has browsed the **benefits for the desired category and for the right location/city**. Ideally, to turn revenue, we want to see customer either go to **chapter 1** of **booknow**, **email request** or **concierge chat request** i.e. the **channels** of servicing. In other words, we want customers to book a request through different channels via CRM after they have browsed **benefits**.

We can then combine it with what is stored in CMS (a.k.a. content management system) about **benefits** to provide more granular views of **customer journey** within the **benefits** funnel.

At the same time, we can formulate a KPI:

$KPI=\frac{\text{Number of visit in which customer has forked from benefits to channels}}{\text{Total number of visit in which customer has been to benefits}}$

To recap, **chapter 1** is of higher order to **chapter 2** and **chapter 3**. What is tracked and stored in **chapter 2** and **chapter 3** largely depends on your **digital tracking template** and **digital strategy**.

[Here's](https://www.kaushik.net/avinash/) a very good source of digital tracking and digital strategy.

## The Challenge
Naturally, we want our customers to spend some time in the **benefits** funnel, i.e. browsing through a few benefits and maybe a few different cities (because there may be a positive correlation between amount of money spent and number of cities browsed, maybe). This is sometimes called **depth** of visit, i.e. how deep the customer has gone in each visit.

So the challenge for creating a customer journey based on **chapter 1** is that there will be a lot of repeated values. Imaging we have a web session (part of a visit) as follows:

{{< diagram >}}
graph TD;
  A[Benefit A] --> B[City A];
  B[City A] --> C[Benefit B];
  C[Benefit B] --> D[City A];
  D[City A] --> E[Benefit ALL];
  E[Benefit ALL] --> F[City A];
  F[City A] --> G[...]
{{< /diagram >}}

As you can see that all of pages viewed by customer in session above have **chapter 1** as **benefits** which will be repeated many times in the **customer journey by chapter 1**.

One may say: "easy fix mate, just take unique values of **chapter 1** in each visit."

It would not work because what if the customer in session above went to **offer** and spent some time there but then went back to **benefits** to browse furthermore. The second or subsequent "fork" to **benefits** would have been lost if we simply take the unique values of **chapter 1** for each visit.

An example of such visit is shown below:

|  D_PAGE_CHAP1  | PAGE_NUM |
|----------------|----------|
|      home      | 1        |
|    benefits    | 2        |
|    benefits    | 3        |
|    benefits    | 4        |
|    benefits    | 5        |
| make a request | 6        |
|  email request | 7        |
|      home      | 8        |
|    benefits    | 9        |
|    benefits    | 10       |
|      home      | 11       |
|    benefits    | 12       |
|    benefits    | 13       |
|    benefits    | 14       |
| make a request | 15       |
|  email request | 16       |
|    benefits    | 17       |
|    benefits    | 18       |
|    benefits    | 19       |
|    benefits    | 20       |
|    benefits    | 21       |
|    benefits    | 22       |
|    benefits    | 23       |
|    benefits    | 24       |
|    benefits    | 25       |
|    benefits    | 26       |
|    benefits    | 27       |
| make a request | 28       |
|  email request | 29       |
|    benefits    | 30       |
| make a request | 31       |
|  email request | 32       |
|    benefits    | 33       |
|     cities     | 34       |
|     cities     | 35       |
|      offer     | 36       |
|     cities     | 37       |

Customer in above visit browsed **benefits** six times seperately.

## A Python solution to the challenge
> :warning: <br>
Start of a Jupyter notebook

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

> :warning: <br>
End of a Jupyter notebook

## The Customer Journey
Let's take the following **customer journey** of a visist:
> [[1] benefits, [2] contents, [3] benefits, [4] offer, [5] error, [6] offer, [7] home, [8] benefits, [9] offer, [10] error, [11] offer, [12] home]

with **step depth** of the following:
> [3, 1, 1, 3, 1, 2, 1, 1, 3, 1, 2, 1]

We can quickly draw some insights:
1. Customer journey did not start with **home**, indicating that prior to browsing **benefit** customer was inactive for more than 30 minutes so a new visit was tracked when customer was active again.
   - Action: check if 30 minutes threshold is appropriate. Check the proportion of those visits which were timed out. If proportion is large, deep-dive.
<br></br>
2. Customer experienced **error** after browsed **offer** and this happened two times.
   - Action: check **chapter 2** and **chapter 3** for more details of the error. Check the proportion of forking to **error** after **offer** pages; and **error** happend after which **chapter 3** and its proportion
<br></br>
3. Look for the customer journey of next visit by the customer to draw more insights

4. Look for the customer journey of previous visit by the customer to add that to this customer journey to form a more complete journey

## Path Map
We can draw greater insights by creating a path map based on large dataset of visits and viewed-pages:
![Path Map](img/path_map.jpg "Customer journey path map")**Customer Journey Path Map**