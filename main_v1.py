# %%
import time
start = time.time() 

# %%
from datetime import datetime

now = datetime.now()
year = now.year
month = now.month - 2

id_periodo = int(str(year) + str(month).zfill(2))
print(id_periodo)

# %%
id_periodo = 202408
countries = ('CO','PE','BO','EC','CL','AR','GT','SV','HN','DO','PR','PA','CR')

# %%
#!python -m venv matriz_opp

# %%
import pandas as pd
import pyodbc
import time
import numpy as np
import os
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go


# %% [markdown]
# # 1. Sell Out

# %%
import time
def timeit(function):
   def inner(*args, **kwargs):
       tic = time.time()
       value = function(*args, **kwargs)
       toc = time.time()
       print("Time elapsed (min):", round((toc-tic)/60, 2))
       return value
   return inner

import os
import pyodbc
import pandas as pd
class DataHandler:
   def __init__(self, server, database, username):
       self.server = server
       self.database = database
       self.username = username
       self.connection = None
       pd.set_option("display.max_columns", 1000)
       pd.set_option("display.max_rows", 50)
       pd.set_option("display.max_colwidth", 10000)
   def connect(self):
       conn_str = (
           f"DRIVER={{ODBC Driver 18 for SQL Server}};"
           f"SERVER={self.server};"
           f"DATABASE={self.database};"
           f"UID={self.username};"
           f"Authentication=ActiveDirectoryInteractive"
       )
       self.connection = pyodbc.connect(conn_str)
   @timeit
   def load_data(self, query):
       self.connect()
       self.data = pd.read_sql(query, self.connection)
       self.connection.close()
   def get_data(self):
       return self.data.copy()

# %%
server = "sql-kcpmialao-prod-scus.database.windows.net"
database = "db-kcpmialao-prod-scus-1"
username = "juan.s.quiroga@kcc.com"
data_handler = DataHandler(server, database, username)
query = f"""
   DECLARE @id_periodo INT;
   DECLARE @nu_anio INT;
   DECLARE @nu_mes INT;
   DECLARE @id_periodo_12 INT;
   SET @id_periodo = {id_periodo};
   SET @nu_anio = ROUND(@id_periodo/100,0);
   SET @nu_mes = CAST(RIGHT(CAST(@id_periodo AS VARCHAR(6)), 2) AS INT);
   SET @id_periodo_12 = YEAR(DATEADD(month, -11, CAST(CONCAT(CAST(@nu_anio AS VARCHAR(8)), '-', CAST(@nu_mes AS VARCHAR(8)), '-01') AS DATE)))*100 + MONTH(DATEADD(month, -11, CAST(CONCAT(CAST(@nu_anio AS VARCHAR(8)), '-', CAST(@nu_mes AS VARCHAR(8)), '-01') AS DATE)));
   WITH SISO AS (
       SELECT
           b.cod_country AS id_pais,
           a.id_country,
           a.id_subsegmento,
           tax AS tax_number,
           id_sk_tax AS id_cliente,
           id_material AS id_producto,
           imp_lc_total,
           imp_usd_total,
           num_cajas,
           gsu,
           SISO,
           TRY_CAST(YEAR(fec_datos) AS INT)*100 + MONTH(fec_datos) AS id_periodo
       FROM prod.view_macd_siso a
       LEFT JOIN prod.dim_geografia b ON a.id_country = b.id_country
       WHERE a.id_country <> 4033
       AND TRY_CAST(YEAR(fec_datos) AS INT)*100 + MONTH(fec_datos) BETWEEN @id_periodo_12 AND @id_periodo
       AND imp_usd_total > 0
       AND b.cod_country IN {countries}
   )
   SELECT a.*,
       CASE WHEN b.desc_business_category = 'Other KCP' THEN 'Others' ELSE b.desc_business_category END AS desc_business_category,
       b.desc_eph3,
       b.desc_eph4,
       b.desc_eph5,
       CASE WHEN b.split_category LIKE '%Sanit%' THEN 'Sanitizer' ELSE b.split_category END AS split_category,
       c.desc_end_customer AS desc_cliente,
       d.desc_segmento_ing AS desc_segmento_eng,
       d.desc_business_ing AS business_segment,
       cl.RFM AS desc_rfm,
       cl.Holding,
       cl.sfdc_account_id
   FROM SISO a
   INNER JOIN prod.dim_producto b ON a.id_producto = b.id_material AND b.desc_business_category NOT IN ('Consumo','Safety','Not EPH') AND b.desc_eph5 NOT LIKE '%Dispenser%'
   INNER JOIN prod.dim_macd_end_customer c ON a.id_cliente = c.id_sk_tax
   INNER JOIN prod.dim_segmento d ON c.id_subsegmento = d.id_subsegmento AND d.desc_segmento_ing NOT IN ('No End Customers','Government')
   LEFT JOIN prod.view_dim_end_customer cl ON cl.id_sk_tax = c.id_sk_tax
"""
# Load the data
data_handler.load_data(query)
# Get the data
df = data_handler.get_data()

# %%
#data = data_full.get_data()
data = df.copy()
print(data.shape,data['id_cliente'].nunique())
data['category'] = np.where(data['desc_business_category']!='Skincare', data['desc_business_category'],data['split_category'])

# %%
key = ['id_pais','desc_rfm','desc_segmento_eng','business_segment']
categories = ['Bath Tissue','Towels','Soaps','Sanitizer','Wipers','Others']

data_agg = data.groupby(['id_cliente'] + key + ['category'],as_index=False).agg(
    annual_value = ('imp_usd_total','sum'),
    monthly_value = ('imp_usd_total',lambda x: x.sum()/12)
)

print(data_agg.shape,data_agg['id_cliente'].nunique())

# %%
tmp = data[['id_cliente'] + key].drop_duplicates().merge(pd.DataFrame({'category':categories}), how = 'cross')
data_agg = tmp.merge(data_agg.drop(key, axis=1), on = ['id_cliente','category'], how = 'left').fillna(0)
print(data_agg.shape,data_agg['id_cliente'].nunique())

# %% [markdown]
# # 2. EMIS Preprocessing

# %% [markdown]
# This section should be moved to another single script

# %%
country_dic = {'Colombia':'CO','Dominican Republic':'DO','Peru':'PE','Chile':'CL','Costa Rica':'CR','El Salvador':'SV'
               ,'Guatemala':'GT','Paraguay':'PY','Jamaica':'DO','Bolivia':'BO','Trinidad and Tobago':'DO','Bahamas':'DO'
               ,'Saint Lucia':'DO','Barbados':'DO','Puerto Rico':'PR', 'Ecuador': 'EC', 'Panama':'PA','Argentina':'AR', 'Honduras':'HN','Bolivia':'BO'}

# %%
base_emis = pd.DataFrame()
for i in countries:
    elem = list(country_dic.values()).index(i)
    country = list(country_dic.keys())[elem]

    base_emis_single = pd.read_excel("Input\EMIS\Base_EMIS.xlsb"
                            ,engine = 'pyxlsb'
                            ,sheet_name=i
                            ,dtype = {'Número de Identificación Fiscal' : 'object' } )
    print(f"{country}: ",base_emis_single.shape[0], ' ' ,base_emis_single[base_emis_single['Número de Identificación Fiscal'].notnull()].shape[0])
    base_emis_single['id_pais'] = i
    base_emis = pd.concat([base_emis,base_emis_single], axis = 0)

print('TOTAL EMIS: ' , base_emis.shape[0], ' ' ,base_emis[base_emis['Número de Identificación Fiscal'].notnull()].shape[0])

base_emis = base_emis[['PAIS','id_pais','Número de Identificación Fiscal','Empleados','Nombre Comercial']]
base_emis.columns = ['pais','id_pais','tax_number','empleados','desc_cliente']
base_emis['tax_number'] = base_emis['tax_number'].str.strip()


# %%
cond = (base_emis['empleados']==0)|(base_emis['empleados'].isnull())
base_emis_1 = base_emis[cond].copy()
base_emis_2 = base_emis[~cond].copy()

#### Treatment on employees field
base_emis_2['empleados'] = base_emis_2['empleados'].astype(str)
base_emis_2['empleados'] = base_emis_2['empleados'].apply(lambda x: x[:-7].strip() if '(' in str(x) else x).str.replace(',','').replace('Above 1000',1000).replace('0xf',np.nan)
base_emis_2['empleados'] = base_emis_2['empleados'].astype(str).apply(lambda x: (int(x.split(' - ')[0])+int(x.split(' - ')[1]))/2 if '-' in x else x)

base_emis_clean = pd.concat([base_emis_1,base_emis_2],axis = 0)
base_emis_clean['empleados'] = base_emis_clean['empleados'].astype(float)

#####################
print("Without tax number: \n",base_emis_clean[(base_emis_clean['tax_number'].isnull())|(base_emis_clean['tax_number']=='')]['pais'].value_counts())
print("% with employess datum", round(len(base_emis_clean[base_emis_clean['empleados']>0])*100/len(base_emis_clean),1))
print("% with employess and tax number datum", round(len(base_emis_clean[(base_emis_clean['empleados']>0)&(base_emis_clean['tax_number'].notnull())&(base_emis_clean['tax_number']!='')])*100/len(base_emis_clean)))
#####################

# %%
base_emis_clean['tax_number'] = base_emis_clean['tax_number'].astype(str).apply(lambda x: x.split(', '))
base_emis_clean = base_emis_clean.explode('tax_number')

base_emis_clean['id_cliente'] = np.where(base_emis_clean['id_pais'].isin(['HN','DO','PR']), base_emis_clean['tax_number'], base_emis_clean['id_pais']+ '-' + base_emis_clean['tax_number'].astype(str))
base_emis_clean['empleados'] = np.where(base_emis_clean['empleados']==0, np.nan, base_emis_clean['empleados'])

# %%
#TO REMOVE DUPLICATES
base_emis_clean = base_emis_clean.groupby(['pais','id_pais','id_cliente','tax_number'])['empleados'].max().reset_index()

# %% [markdown]
# # 3. Join EMIS

# %%
data_merge = data_agg.merge(base_emis_clean, on = ['id_cliente','id_pais'],how = 'left').drop(['pais','tax_number'],axis = 1)
data_merge['fl_empleados'] = np.where(data_merge['empleados'].isna(),'Nulo','EMIS') 

# %%
rate = round(data_merge[data_merge['fl_empleados']=='EMIS']['id_cliente'].nunique()*100/data_merge['id_cliente'].nunique(),1)
print(f"Total customers:  {data_merge['id_cliente'].nunique()} ({rate}% with employees number datum) \n")

for i in ['Champion','Promising','Re-Engage','Low Value','Lost','New Customer','N/A']:
    #print(f"Customer with employees number datum in RFM {i}: ")
    condic = (data_merge['desc_rfm'] == i) #####&(data_merge['id_pais'].isin(['CO']))
    if condic.sum() > 0:
        rate = round(data_merge[(data_merge['empleados']>0)&condic]['id_cliente'].nunique()*100 / data_merge[condic]['id_cliente'].nunique(),1)
        print(f"Total customers in RFM {i} = {data_merge[condic]['id_cliente'].nunique()}. ({rate}% with employees number datum)")

# %%
##### ANALISIS DE CASOS NULOS  DE EMPLEADOS POR PAIS, SEGMENTO Y RFM Y SU RESPECTIVA IMPUTACIÓN
### tabla de error por pais segmento, mayor incertidumbre 
#key = 'desc_segmento_eng'
#tmp2 = data_merge.groupby(['id_pais',key],as_index=False).agg(count=('id_cliente','count'),count_sin_na=('empleados','count'),mean=('empleados','mean')).assign(count_nulos = lambda x : x['count']-x['count_sin_na'], porc_nulos = lambda x : np.round(100*(1- (x['count_sin_na']/x['count'])),0))
#tmp2['etiqueta']=tmp2['count_nulos'].astype(str)+'/'+tmp2['count'].astype(str)  #,count_sin_na=('empleados', lambda x : len(list(filter(lambda y: y > 0, x)))  ))
#tmp2 = tmp2[['id_pais',key,'porc_nulos','etiqueta']]
#tmp2.columns = ['x','y','z','t']
#tmp2.z=round(tmp2.z,0)
#fig = go.Figure(data=go.Heatmap(
#                   z=tmp2['z'],
#                   x=tmp2['y'],
#                   y=tmp2['x'],
#                   hoverongaps = False,
#                    text=tmp2['t'],
#                    texttemplate="%{text}",
#                    textfont={"size":10}
#                    ))
#fig.update_layout(title_text=f"Nulos por pais y {key}" , width =1200, height=600)
#fig.show()

# %%
def missing_imputation(data_merge, lvl = ['id_pais']):
    tmp = data_merge[data_merge['empleados']>0].groupby(lvl, as_index = False).agg(empleados_median = ('empleados','median'))
    
    data_merge = data_merge.merge(tmp, on = lvl, how = 'left')
    print("Nulos: ",data_merge['empleados'].isna().sum())
    data_merge['empleados'] = data_merge['empleados'].fillna(data_merge['empleados_median'])
    data_merge.drop('empleados_median',axis = 1, inplace = True)
    
    return data_merge

# %%
data_merge = missing_imputation(data_merge, lvl = ['id_pais','desc_segmento_eng','desc_rfm'])
data_merge = missing_imputation(data_merge, lvl = ['id_pais','desc_segmento_eng'])
data_merge = missing_imputation(data_merge, lvl = ['desc_segmento_eng'])
data_merge = missing_imputation(data_merge, lvl = ['id_pais'])

# %% [markdown]
# # 4. Opp Value

# %% [markdown]
# ### Homologación segmentos

# %%
###########################################################
##### Homologacion segmentos con listas de consumos #######
###########################################################

### HHT
data_merge['segment_hht'] = np.where(data_merge['business_segment'] == 'Others', data_merge['desc_segmento_eng'], data_merge['business_segment'])
data_merge['segment_hht'] = np.where(data_merge['segment_hht'].isin(['Contract Cleaners','Transportation Hubs']), 'Other H&W', data_merge['segment_hht'])

### WYPALL
data_merge['segment_wypall'] = np.where(data_merge['business_segment'] == 'Manufacturing', data_merge['desc_segmento_eng'], data_merge['business_segment'])
data_merge['segment_wypall'] = np.where(data_merge['segment_wypall'].isin(['Others','Education']),'Other H&W',data_merge['segment_wypall'])
data_merge['segment_wypall'] = np.where(data_merge['segment_wypall']=='Aerospace','Other Industrial',data_merge['segment_wypall'])
data_merge['segment_wypall']= np.where( (data_merge['segment_wypall']=='Lodging')&(data_merge['id_pais'].isin(['CL', 'CO', 'EC', 'PE','AR','BO']) ),'Other H&W',data_merge['segment_wypall'])

print("HHT segments: ",data_merge['segment_hht'].unique())
print("Wypall segments: ",data_merge['segment_wypall'].unique())

# %% [markdown]
# ### Tamaño clientes grandes vs chicos hht, wypall y champion

# %%
###################################################
######################## HHT ######################
###################################################

### Venta SO
data_merge['total_annual']=data_merge.groupby(['id_cliente','id_pais','segment_hht'])['annual_value'].transform(lambda x: np.sum(x))
data_merge['mean_annual_hht']=data_merge[data_merge.total_annual>0].groupby(['id_pais','segment_hht'])['total_annual'].transform(lambda x: np.mean(x))
data_merge['size_clients_hht']=np.where(data_merge.total_annual>data_merge.mean_annual_hht,'Grande','Chica')

### #empleados
data_merge['mean_empleados_hht']=data_merge.groupby(['id_pais','segment_hht'])['empleados'].transform(lambda x: np.mean(x))
data_merge['size_clients_hht_empleados']=np.where(data_merge.empleados>data_merge.mean_empleados_hht,'Grande','Chica')

### Consolidado
data_merge['size_hht']=np.where((data_merge['size_clients_hht_empleados']=='Grande')|(data_merge['size_clients_hht']=='Grande'),'Grande','Chica')

# %%
###################################################
###################### WYPALL #####################
###################################################
data_merge['mean_annual_wypall']=data_merge[data_merge.total_annual>0].groupby(['id_pais','segment_wypall'])['total_annual'].transform(lambda x: np.mean(x))
data_merge['size_wypall']=np.where(data_merge.total_annual>data_merge.mean_annual_wypall,'Grande','Chica')

# %%
###################################################
##################### CHAMPION ####################
###################################################

print("Before:")
print(data_merge[data_merge.desc_rfm=='Champion']['size_hht'].value_counts()/5)
print(data_merge[data_merge.desc_rfm=='Champion']['size_wypall'].value_counts()/5)

data_merge['size_hht']=np.where(data_merge['desc_rfm']=='Champion','Grande',data_merge['size_hht'])
data_merge['size_wypall']=np.where(data_merge['desc_rfm']=='Champion','Grande',data_merge['size_wypall'])

print("After:")
print(data_merge[data_merge.desc_rfm=='Champion']['size_hht'].value_counts()/5)
print(data_merge[data_merge.desc_rfm=='Champion']['size_wypall'].value_counts()/5)

# %%
data_merge['segment_unified']=np.where(data_merge.category=='Wipers',data_merge.segment_wypall,data_merge.segment_hht)
data_merge['size_unified']=np.where(data_merge.category=='Wipers',data_merge.size_wypall,data_merge.size_hht)

# %% [markdown]
# ### Consolidación lista de consumo

# %%
###################################################
######################## HHT ######################
###################################################

df_hht = pd.read_excel("Input\Base_hhts_list.xlsx")
df_hht['id_pais'] = df_hht['Pais'].replace(country_dic)

segment_dict= {'Food Processing': 'Food processing', 'Educación':'Education', 'Manufactura' : 'Manufacturing',
 'Leisure & Entertainment':'Leisure & Entertainm', 'Others H&W':'Other H&W'}
df_hht['SEGMENT']=df_hht['SEGMENT'].replace(segment_dict)

df_hht['CATEGORY'].replace('Soap','Soaps',inplace=True)
#df_hht['CATEGORY']= np.where(df_hht['CATEGORY'].isin(['Soap','Sanitizer']), 'Skincare',df_hht['CATEGORY'])
df_hht=df_hht.groupby(['id_pais','Tipo de Empresa','SEGMENT','CATEGORY','GTN Pais'],as_index=False).agg(NSV_USD_Total_Por_Empleado_sum=('NSV USD Total Por Empleado','sum'))
df_hht.columns=['id_pais','size_unified','segment_unified','category','GTN Pais','Value_category']


###################################################
###################### WYPALL #####################
###################################################

df_wypall = pd.read_excel("Input\Base_wypall_list.xlsx")
df_wypall['id_pais'] = df_wypall['Pais'].replace(country_dic)

segment_dict_wyp= {'Food Processing': 'Food processing', 'Educación':'Education', 'Mining':'Minning',
                   'Oil & Gas':'Oil and Gas', 'Others H&W':'Other H&W'}
df_wypall['Segmento']=df_wypall['Segmento'].replace(segment_dict_wyp)

df_wypall['Value_category']= df_wypall['Consumo Cajas Mensuales Por Tarea']*df_wypall['Net Sales USD Per Cs']

df_wypall=df_wypall[['id_pais', 'Tipo de Empresa','Segmento', 'Categoria','GTN Pais', 'Value_category']]
df_wypall.columns=['id_pais','size_unified','segment_unified','category','GTN Pais','Value_category']


list_consolidada=pd.concat([df_hht,df_wypall], axis=0)


# %% [markdown]
# ### Oportunidad ampliación & penetración

# %%
######################################################################
#### Opp Value with the expected consumption of companies depending on their employees number (HHT) or activity type (Wypall)
######################################################################
data_opp=data_merge.merge(list_consolidada, on=['id_pais','size_unified','segment_unified','category'], how='left', indicator=True)

data_opp['GTN Pais'] = data_opp.groupby(['id_pais'])['GTN Pais'].transform('min')

data_opp['full_potential']=np.where(data_opp['category'] !='Wipers', data_opp['empleados'] * data_opp['Value_category'], data_opp['Value_category'])
data_opp['monthly_value_NSV']=data_opp['monthly_value']*(1-data_opp['GTN Pais'])
data_opp['opportunity_value']=data_opp['full_potential']-data_opp['monthly_value_NSV']

data_opp['opportunity_value']=np.where(data_opp['opportunity_value']<0,0,data_opp['opportunity_value'])

# %%
######################################################################
#### Opp Value with the monthly average of the population
#### Also to assume some opp value for the category "Others"
######################################################################

tmp = data_opp[data_opp['monthly_value_NSV']>0].groupby(['id_pais','segment_unified','size_unified','category'],as_index=False).agg(monthly_value_NSV_avg = ('monthly_value_NSV','mean'))
data_opp = data_opp.merge(tmp, on = ['id_pais','segment_unified','size_unified','category'], how = 'left')

tmp = data_opp[data_opp['monthly_value_NSV']>0].groupby(['segment_unified','category'],as_index=False).agg(
    monthly_value_NSV_avg2 = ('monthly_value_NSV','mean'))
data_opp = data_opp.merge(tmp, on = ['segment_unified','category'], how = 'left')
data_opp['monthly_value_NSV_avg'] = data_opp['monthly_value_NSV_avg'].fillna(data_opp['monthly_value_NSV_avg2'])
data_opp.drop('monthly_value_NSV_avg2',axis = 1, inplace = True)


data_opp['opportunity_value2']=data_opp['monthly_value_NSV_avg']-data_opp['monthly_value_NSV']
data_opp['opportunity_value2']=np.where(data_opp['opportunity_value2']<0,0,data_opp['opportunity_value2'])

# %%
HHT = ['Bath Tissue','Towels','Sanitizer','Soaps']

### Option 1: 
## HHT with theoretica formula for customers with EMIS employees number datum
## HHT based on SO average for customers without EMIS employess number datum
## Wipers theoretical consumption formula
## Others based on SO average
cond = ((data_opp['fl_empleados']=='EMIS') & (data_opp['category'].isin(HHT))) | (data_opp['category']=='Wipers')
data_opp['opportunity_value_final'] = (data_opp['opportunity_value']).where(cond,data_opp['opportunity_value2'])

### Option 2: 
## HHT with theoretica formula for customers with EMIS employees number datum
## HHT based on SO average for customers without EMIS employess number datum
## Wipers based on SO average
## Others based on SO average
#cond = ((data_opp['fl_empleados']=='EMIS') & (data_opp['category'].isin(HHT)))
#data_opp['opportunity_value_final'] = (data_opp['opportunity_value']).where(cond,data_opp['opportunity_value2'])

### Option 3: Same as SOB 2024 
#data_opp['opportunity_value_final'] = data_opp[['opportunity_value','opportunity_value2']].max(axis=1)

# %%
data_opp.groupby(['category']).agg(opp_teorica = ('opportunity_value','sum'),
                                   opp_so = ('opportunity_value2','sum'),
                                   opp_final = ('opportunity_value_final','sum'),
                                   current_value = ('monthly_value_NSV','sum')).sum()

# %%
### Opp Type: Ampliación vs Penetración
data_opp['opp_type']=np.where(data_opp['monthly_value_NSV']==0,'Penetración',None)
data_opp['opp_type']=np.where((data_opp['monthly_value_NSV']>0)&(data_opp['opportunity_value_final']>0),'Ampliación',data_opp['opp_type'])
data_opp['opp_type']=np.where((data_opp['monthly_value_NSV']>0)&(data_opp['opportunity_value_final']==0),'Sin Opp',data_opp['opp_type'])

# %%
data_opp['opp_type'].value_counts()

# %%
import nbformat
print(nbformat.__version__)

# %%
import nbformat

tipo = 'Penetración'
fig = px.box(data_opp[(data_opp['opp_type']==tipo)&(data_opp['opportunity_value']>0)], x='desc_rfm', y="opportunity_value_final", color='category')
fig.update_layout(title_text=f"Distribución de oportunidades de {tipo}")
fig.show()

# %% [markdown]
# # 5. Output

# %%
data_opp.columns

# %%
columns = ['id_cliente','id_pais','desc_rfm','desc_segmento_eng','business_segment','segment_unified','size_unified','empleados', 'fl_empleados','category','monthly_value_NSV','opportunity_value_final','opp_type']
df = data_opp[columns].copy()

tmp = data.groupby(['id_cliente']).agg({'desc_cliente':'max','Holding':'max','sfdc_account_id':'max','id_country':'max','id_subsegmento':'max'}).reset_index()
df = df.merge(tmp, on = 'id_cliente', how = 'left')
df_agg = df.groupby('id_cliente',as_index = False).agg(customer_opp_total = ('opportunity_value_final','sum'))
df_agg_2 = df[df['monthly_value_NSV']>0].groupby('id_cliente',as_index = False).agg(number_categories = ('category','nunique'), categories = ('category',lambda x: str(x.unique())[2:-2].replace("' '",", ")))
df = df.merge(df_agg, on = 'id_cliente', how = 'left')
df = df.merge(df_agg_2[['id_cliente','number_categories','categories']], on = 'id_cliente', how = 'left')
df['categories'] = df['categories'].fillna('None').astype('string')
df['number_categories'] = df['number_categories'].fillna(0).astype(int)

df['opportunity_value_final']=round(df['opportunity_value_final'],0)
df['customer_opp_total']=round(df['customer_opp_total'],0)
df['monthly_value_NSV']=round(df['monthly_value_NSV'],0)
df['id_periodo']=id_periodo

df.columns


# %%
##########################################################
###### Filter in customers with purchases within the last 6 months
##########################################################
print(len(df), df['id_cliente'].nunique())
last_purchase = data.groupby(['id_cliente'],as_index=False)['id_periodo'].max()
last_purchase['last_purchase'] = pd.to_datetime(last_purchase['id_periodo'].astype(str)+'01')
periodos_ant = [int((pd.to_datetime(str(id_periodo)+'01')+pd.DateOffset(months=-i)).strftime('%Y%m')) for i in range(0,6)]
clientes_activos_6m = last_purchase[last_purchase['id_periodo'].isin(periodos_ant)]['id_cliente'].unique()
df= df[df['id_cliente'].isin(clientes_activos_6m)]
print(len(df), df['id_cliente'].nunique())

# %%
cols_to_upload = ['id_periodo'] + columns + ['customer_opp_total','number_categories','categories','desc_cliente','Holding','sfdc_account_id','id_country','id_subsegmento']
df = df[cols_to_upload]

# %%
df.info()

# %%
#DROP TABLE prod_ds.Sellout_Penetration;
#CREATE TABLE prod_ds.Sellout_Penetration (
#id_periodo INT
#, id_cliente VARCHAR(100)--113
#, id_pais VARCHAR(2)--3
#, desc_rfm VARCHAR(50)--100
#, desc_segmento_eng VARCHAR(100)--50
#, business_segment VARCHAR(100)--50
#, segment_unified VARCHAR(100)--50
#, size_unified VARCHAR(20)
#, empleados FLOAT
#, fl_empleados VARCHAR(30)
#, category VARCHAR(20)--100
#, monthly_value_NSV FLOAT
#, opportunity_value_final FLOAT
#, opp_type VARCHAR(20)
#, customer_opp_total FLOAT
#, number_categories INT
#, categories VARCHAR(300)
#, desc_cliente VARCHAR(300) --max
#, Holding VARCHAR(200) --max
#, sfdc_account_id VARCHAR(100) 
#--, id_localidad_distribuidor VARCHAR(300)--113
#--, id_vendedor VARCHAR(300) --100
#, id_country INT
#, id_subsegmento INT
#)


# %%
from tqdm.notebook import tqdm
import ipywidgets

user = "juan.s.quiroga"
connstring = f"DRIVER={{ODBC Driver 18 for SQL Server}};DSN=kcplao;UID={user}@kcc.com"
conn = pyodbc.connect(connstring = connstring)

cursor = conn.cursor()
cursor.fast_executemany = True


cursor.execute(f"""
                DELETE FROM prod_ds.Sellout_Penetration
                WHERE id_periodo = {id_periodo} AND id_pais in {countries}
                """)
conn.commit()

#### INSERT new data
cols = str(tuple(df.columns)).replace("'","").replace(" ","")
args = str(tuple(['?']*len(df.columns))).replace("'","").replace(" ","")
insert_sql = f"INSERT INTO prod_ds.Sellout_Penetration {cols} VALUES {args};"

    
chunk_size = 500    
for row_count in tqdm(range(0, df.shape[0],chunk_size)):
    chunk = df.iloc[row_count:row_count + chunk_size,:].values.tolist()
    tuple_of_tuples = tuple(tuple(x) for x in chunk)
    cursor.executemany(insert_sql,tuple_of_tuples)
    
    
conn.commit()

# %%
df.groupby(['id_cliente'])['empleados'].count().sort_values(ascending=False)

# %%
end = time.time()
print(f"Duration = {round((end-start)/60,1)}m")

# %%



