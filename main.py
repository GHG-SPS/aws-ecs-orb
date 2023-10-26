import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
import snowflake.connector
import sys,re
import os
# import streamlit as st
warnings.filterwarnings('ignore')

class LoadingData:
    @staticmethod
    def LoadingData(PN):
        path = r'C:\Users\zhusj\python\Input_data\TFMC'
        PN = PN+'.xlsx'
        path = os.path.join(path,PN)
        data = pd.read_excel(path)
        EF_PR = EF_PR = pd.read_excel(r'C:\Users\zhusj\python\Input_data\TFMC/EF_vendor_region.xlsx')
        dfs = pd.read_excel(r'C:\Users\zhusj\python\Input_data\TFMC\Air_Ocea_Road.xlsx',sheet_name=None)
        road,sea,air,EF= dfs['Roaddistance'],dfs['Seadistance'],dfs['Airportdistance'],dfs['EF']
        EF_Trans ={'Road':road,'Sea':sea,'Air':air,'EF':EF}
        return data,EF_PR,EF_Trans
# data = pd.read_excel('data/P1000220410.xlsx')# read BOM data
# EF_PR = pd.read_excel('data/EF_vendor_region.xlsx')
# data, EFe= LoadingData.LoadingData('P7000073756')
class GetEF:
    @staticmethod
    def getEF(EF_PR):
        columns = EF_PR.columns
        EFe = {i:2 for i in columns}
        for i in columns:
            for j in EF_PR[i]:
                if not pd.isna(j):
                    EFe[i]=j
                    break
        return EFe

# EFe = GetEF.getEF(EFe)
####################################
# EF_vendor = pd.read_excel('data/Master file -Survey based.xlsx',sheet_name ='GHG Calculation')# read BOM data
# cols= ['Supplier Name','Supplier Country','Please choose reporting year:','Total EVG EF']
# EF_VD = EF_vendor[EF_vendor['Direct/Indirect']=='Direct'][cols]
# EF_VD.dropna(subset=['Total EVG EF'],inplace=True)
# EF_VD.where(EF_VD['Total EVG EF']>1e-5,inplace =True)
# EF_VD.dropna(subset=['Total EVG EF'],inplace=True)
# EF_VD1 = EF_VD.groupby(['Supplier Name','Supplier Country'])[['Total EVG EF']].mean().sort_values(by='Supplier Country')
# EF_VD1.to_excel('data/EF_vendor_region2.xlsx')
#####################################
# clear data 
class DataClear:
    @staticmethod
    def dataClear(data):
        col = data.columns
        col = [j.upper() for j in col]
        data.columns = col
        data0 = data.dropna(subset=['PART','LEVEL']) # drop rows without PART and LEVEL row
        data0= data0.reset_index(drop=True) # reset the index again
        col = data0.columns
        for i in col:
            if i!='QTY':
                if i =='LEVEL':
                    if data0[i].dtype=='float64':
                        data0[i]=data0[i].astype(int)
                        data0[i]=data0[i].astype(str)
                    elif data0[i].dtype=='O':
                        pass 
                else:
                    data0[i]=data0[i].astype(str)
        data0['PART'][1:]= pd.Series([i[(i.count('+')+1):] for i in data0['PART'][1:]])
        data0.replace(u'\xa0\xa0\xa0\xa0','',inplace=True)
        data0.replace(u'\xa0','',inplace=True)
        
        return data0
# data0 =DataClear.dataClear(data)
# query data from snowfake
def queryData(data,PN=None):
    PN = PN,PN
    PN_string = tuple(set(data['PART'])) # get the PN list to be query as tuple
    my_query = f"""
    select distinct
    ltrim(h.material,'0') as PART,h.description,h.components,
    h.PROD_FAMILY,h.ELASTOMER_SPECS,h.COATING_SPECS,h.prod_line,
    h.PC1,h.PC2,h.OEM_VENDOR
    from core.material as h
    where PART in {PN_string}
    """  # query material info
    my_query1 = f"""
    select distinct
    ltrim(a.matnr,'0') as PART,a.aedat as purchase_date,
    c.lifnr as vendor_ID,c.name1 as vendor_nanme,c.land1 as vendor_country, c.ORT01 as vendor_city
    from rpl_sap.ekpo as a 
    left join rpl_sap.ekko as b on a.ebeln = b.ebeln
    left join rpl_sap.lfa1 as c on b.lifnr = c.lifnr
    where PART in {PN_string}
    and a.aedat > '20210101'
    """  # query purchased infor
  
    my_query2 =f"""
    select 
    distinct
    ltrim(f.material,'0') as PN0,
    f.description,
    b.GLTRI as actual_finish_date,
    a.WERKS as plant,
    f.MAIN_PRODUCTION_PLANT,
    d.country_code,
    d.city,
    a.bukrs  as company_code
    from rpl_sap_attunity.aufk as a
    inner join rpl_sap_attunity.afko as b on b.aufnr = a.aufnr
    inner join rpl_sap_attunity.afpo as c on c.aufnr = a.aufnr
    right join core.material as f on f.material = b.plnbez
    left join sap_reporting.plants as d on d.plant = f.MAIN_PRODUCTION_PLANT
    where ltrim(f.material,'0') in {PN}
    """# query top LEVEL PN production site
    
    
    with snowflake.connector.connect( 
    user='ethan.zhu@technipfmc.com', # Required. Replace with your email 
    authenticator="externalbrowser", # Required. 
    account='technipfmc-data', # Required. 
    database="idsprod", # Optional 
    schema="rpl_sap.ekko", # Optional. Replace with the schema you will be working on 
    role="reporting", # Optional. Replace with the role you will be working with 
    warehouse="reporting_wh", # Optional. Replace with the warehouse you will be working with 
    client_store_temporary_credential=True, # Only if installing secure-local-storage to avoid reopening tabs
    ) as conn: 
        cursor = conn.cursor()
        cursor.execute(my_query)
        cursor1 = conn.cursor()
        cursor1.execute(my_query1)
        cursor2 = conn.cursor()
        cursor2.execute(my_query2)
        # res = cursor.fetchall() # To return a list of tuples 
        df_toplevelpn = cursor.fetch_pandas_all() # To return a dataframe
        df_purchased = cursor1.fetch_pandas_all()
        df_production = cursor2.fetch_pandas_all()
    return df_toplevelpn ,df_purchased,df_production

# print(df_toplevelpn.head(2))
# len(df_toplevelpn.PART.unique())
class DFClear:
    @staticmethod
    def dfClear(df):
        col = df.columns
        for i in col:
            df[i]=df[i].astype(str)
        col = [j.upper() for j in col]
        return df
# df_toplevelpn =DFClear.dfClear(df_toplevelpn)
# df_externalcost =DFClear.dfClear(df_externalcost)


# organize one dataframe purchased history with different regions
class PNCountry:
    @staticmethod
    def pnCountry(df):
        df0 = df[['PART','VENDOR_COUNTRY','CITY']]
        df0.drop_duplicates(keep ='first',inplace=True)
        PN = list(df0['PART'].unique())
        out= pd.DataFrame()
        for _, j in enumerate(PN):
            if len(df0[df0['PART']==j])==0:
                cty_list = []
                cty_list.append(j)
            else:
                cty_list = list(df0[df0['PART']==j]['VENDOR_COUNTRY'])
            # cty_list.(j)
            lth = len(cty_list)
            cty_col = ['CTY'+str(k+1) for k in range(lth-1)]
            cty_col.append('PART')
            df1 = pd.DataFrame(data=[cty_list],columns=cty_col)
            out = pd.concat([out,df1])
        out.replace('nan','',inplace=True)
        out.sort_values(by=cty_col,na_position='last',inplace=True)
        PART = out.pop('PART')
        out.insert(loc=0,column=('PART'),value=PART)
        # out.sort_index(axis=1,ascending=False,inplace=True)
        return out
# df_pncty = PNCountry.pnCountry(df_externalcost)
# df_pncty.head(2)
# df_pncty.shape

# general class for concatenate two DF
class Twodfconcat:
    @staticmethod
    def combine(source,target,index=False,start=1):
        if index:
            if 'PART' in target.columns:
                target.index = target['PART']
            elif 'PN' in target.columns:
                target.index = target['PN']
        cols= target.columns
        slice_col = cols[start:]
        lth = source.shape[1]
        # for i,j in enumerate(slice_col):
        #     source.insert(lth+i,column=j,value=None)
        if 'PART' in source.columns:
            for k,l in enumerate(source.PART):
                source.loc[k,slice_col]=target.loc[l,slice_col]
        else:
            pass 
        return source
# concatenate vendor region information
# df_0 = Twodfconcat.combine(df_toplevelpn,df_pncty,index=True)
# df_0 = df_toplevelpn
# define the class to prepare and clear the data 
class DataPrepare:
    # def __init__(self):
    #     self.dataWeight = dataWeight
    #     self.subTotal = subTotal
    #     self.singleAssy = singleAssy
    #     self.rawLevel = rawLevel
    def dataWeight(self,source_data):
        '''insert a new column with weight_kg '''
        cl_data = source_data[['WEIGHT LBS(KGS)']]
        cl_data['Weight_kg']=0
        for j,i in enumerate(cl_data['WEIGHT LBS(KGS)']):
            if i == '':
                cl_data.loc[i,'Weight_kg']=0.01
            elif '(' in set(i):
                a1=i.find('(')
                cl_data.loc[j,'Weight_kg']= max(float(i[a1+1:-1]),0.01)
            else:
                cl_data.loc[i,'Weight_kg'] = max(round(float(i)/2.2,2),0.01)
        source_data['WEIGHT LBS(KGS)']=cl_data['Weight_kg']
        source_data.rename(columns={'WEIGHT LBS(KGS)':'Weight_kg'},inplace=True)
        # return self.source_data
    def subTotal(self,source_data):
        '''function amount subtotal QTY per line'''
        df0= source_data[['LEVEL','QTY']]
        df0['Sub_total_qty']=source_data['QTY']
        for i,j in enumerate(df0['LEVEL']):
            if j =='1' or j =='2':
                pass
            elif 'A' in str(j):
                df0.loc[i,'Sub_total_qty']=0
            else:
                upper_level = str(int(j)-1)# get upper LEVEL string
                df1 = df0.iloc[0:i,]
                upper_df= df1[df1['LEVEL']==upper_level]['Sub_total_qty'] # get upper string QTY series
                upper_index = list(upper_df.index) # convert index to list
                upper_above_close = upper_index[-1]
                upper_level_qty = upper_df[upper_above_close]
                df0.loc[i,'Sub_total_qty']=df0.loc[i,'QTY']*upper_level_qty
            # this for loop setup alternative parts itself and below parts substotal is 0
        for i,j in enumerate(df0['LEVEL']):
            if 'A' in j:
                level_A = j[0]
                df1 = df0.iloc[i:,:]
                lower_level = str(int(level_A)+1)
                level_list = list(df1[df1['LEVEL']==level_A].index)
                up_level = str(int(level_A)-1)
                if len(level_list)==0:
                    up_list = list(df1[df1['LEVEL']==up_level].index)
                    lower_list = list(df1[df1['LEVEL']==lower_level].index)
                    if len(up_list)==0:
                        if len(lower_list) >=1:
                            df0.loc[i:,'Sub_total_qty']=0 
                    elif len(up_list)>=1:
                        if len(lower_list) >=1:
                            df0.loc[i:up_list[0],'Sub_total_qty']=0 
                elif len(level_list)>=1:
                    df2 = df0.iloc[i:level_list[0],:]
                    up_list = list(df2[df2['LEVEL']==up_level].index)
                    lower_list = list(df2[df2['LEVEL']==lower_level].index)
                    if len(up_list)==0:
                        if len(lower_list) >=1:
                            df0.loc[i:level_list[0]-1,'Sub_total_qty']=0 
                    elif len(up_list)>=1:
                        if len(lower_list) >=1:
                            df0.loc[i:up_list[0],'Sub_total_qty']=0 
        source_data.insert(loc=22,column='Sub_total_qty',value=df0['Sub_total_qty'])
        # return self.source_data
    def singleAssy(self,source_data):
        '''add up one column as label distinguish is single or assy PART
        identify each of line is single PART or assy PART '''
        lst =[]
        for i,j in enumerate(source_data['LEVEL'].astype(str)):
            if j=='1':
                lst.append('TopAssy')
            elif 'A' in set(j):
                lst.append(' ')
            else:
                df1 = source_data.iloc[i:,:]
                level_list = list(df1[df1['LEVEL']==j]['LEVEL'].index)
                lower_level = str(int(j)+1)
                up_level = str(int(j)-1)
                level_list = list(df1[df1['LEVEL']==j].index)
                # slice below this PN all items
                if len(level_list)==1:
                    df1 = source_data.iloc[i:,:]
                elif len(level_list)>=2:
                    df1 =source_data.iloc[i:level_list[1],:]
                up_list = list(df1[df1['LEVEL']==up_level].index)
                if len(up_list)==0:
                    pass
                elif len(up_list)>=1:
                    df1=source_data.iloc[i:up_list[0],:]
                lower_list = list(df1[df1['LEVEL']==lower_level].index)
                if len(lower_list)<=1:
                    lst.append('Single')
                else:
                    # second_j = level_list[1]
                    # lower_df = source_data.iloc[i:second_j,:]
                    # lower_level = str(int(j)+1)
                    # lower_list = lower_df[lower_df['LEVEL']==lower_level]
                    # if len(lower_list) <=1:
                    #     lst.append('Single')
                    # else:
                    lst.append('Assy')
        source_data.insert(loc =source_data.shape[1],column='Single_ASSY',value=lst)
        # return self.source_data
    def rawLevel(self,source_data,target_data):
        '''insert couple of columns into data0 '''
        if len(target_data)<1:
            pass
        else:
            cols =['COMPONENTS','PROD_FAMILY','ELASTOMER_SPECS',
                   'COATING_SPECS','PC1','PC2','OEM_VENDOR']
            for l,k in enumerate(cols):
                source_data.insert(loc=2+l,column=k,value=None)
            for i,j in enumerate(source_data['PART']):
                source_data.iloc[i,2:len(cols)+2]=target_data[target_data['PART']==j].iloc[0,:][cols]
            # df_3col = pd.DataFrame()
            # for i,j in enumerate(source_data['PART']):
            #     df_ = target_data[target_data['PART']==j][cols]
            #     df_3col = df_3col.append(df_,ignore_index=True)
            # for k,l in enumerate(df_3col.columns):
                
            #     source_data.insert(loc=2+k,column=l,value =df_3col[l])
            # # return self.source_data
    def getData(self,source_data,target_data):
        self.dataWeight(source_data)
        self.subTotal(source_data)
        self.singleAssy(source_data)
        self.rawLevel(source_data,target_data)
        return source_data
# test_data = DataPrepare()
# data_rawlv=test_data.getData(data0,df_0)
# print( f'data_rawlv shape is {data_rawlv.shape}')
# data_rawlv.to_excel('data/P1000220410_edit5.xlsx',index =False)# write clear data to local file

# class for isolate text into each of word
class StrIsolate:
    # def __init__(self,string):
    #     self.string = string
    def split(self,string):
        strall = re.split(r'[,; /]',string)
        strset = set(strall)
        return strset

# class to recognize each of PN in which mfg process 
class ProcessIdentify:
    def __init__(self,data):
        self.word = StrIsolate()
        self.data =data
        self.lst = list(set(self.data['PART']))
        self.cat = ['ASSY','FORGING','MACHINING','OEM_METAL','OEM_NONMETAL','CASTING','CHEMICAL',
                    'CLADDING','FABRICATION','COATING','RAW_METAL','RAW_NONMETAL']
        self.df  = pd.DataFrame(data=0,index=self.lst,columns=self.cat)
    def chemical(self,PN):
        if self.data[self.data['PART']==PN].iloc[0,:]['PROD_FAMILY']=='CHEMICAL':
            self.df.loc[PN,:]['CHEMICAL']=1
    def assy(self,PN):
        if self.data[self.data['PART']==PN].iloc[0,:]['Single_ASSY'] =='TopAssy':
            self.df.loc[PN,:]['ASSY']=1
        elif self.data[self.data['PART']==PN].iloc[0,:]['Single_ASSY'] =='Assy':
            self.df.loc[PN,:]['ASSY']=1
    def casting(self,PN):
        if self.data[self.data['PART']==PN].iloc[0,:]['Single_ASSY'] =='Single':
            if len(self.data[self.data['PART']==PN].iloc[0,:]['OEM_VENDOR'])<5:
                if len(self.data[self.data['PART']==PN].iloc[0,:]['COMPONENTS'])<5:
                    word1 = self.word.split(self.data[self.data['PART']==PN].iloc[0,:]['QUALITY SPECS'])
                    word2 = self.word.split(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])
                    if 'Q00328' in word1:
                        self.df.loc[PN,:]['CASTING']=1
                        self.df.loc[PN,:]['RAW_METAL']=1
                    elif len(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])>5:
                        if 'P60157' in word2:
                            self.df.loc[PN,:]['CASTING']=1
                            self.df.loc[PN,:]['RAW_METAL']=1
    def coating(self,PN):
        c_prefix =('C80','C81','C82')
        if len(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])>5:
            word = self.word.split(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])
            for i in word:
                for j in c_prefix:
                    if j in i:
                        self.df.loc[PN,:]['COATING']=1
    def OEM(self,PN):
        if self.data[self.data['PART']==PN].iloc[0,:]['Single_ASSY'] =='Single':
            if len(self.data[self.data['PART']==PN].iloc[0,:]['OEM_VENDOR'])>=5:
                if len(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])>5:
                    word0 = self.word.split(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])
                    for i in word0:
                        if 'E5' in i:
                            self.df.loc[PN,:]['OEM_NONMETAL']=1
                            self.df.loc[PN,:]['RAW_NONMETAL']=1
                        elif 'M1' in i:
                            self.df.loc[PN,:]['OEM_METAL']=1
                            self.df.loc[PN,:]['RAW_METAL']=1
                        elif 'M2' in i:
                            self.df.loc[PN,:]['OEM_METAL']=1
                            self.df.loc[PN,:]['RAW_METAL']=1
                        elif 'M3' in i:
                            self.df.loc[PN,:]['OEM_METAL']=1
                            self.df.loc[PN,:]['RAW_METAL']=1
                        elif 'M4' in i:
                            self.df.loc[PN,:]['OEM_METAL']=1
                            self.df.loc[PN,:]['RAW_METAL']=1
                elif len(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])<5:
                    if sum(self.df.loc[PN,:][self.cat])<1:
                        self.df.loc[PN,:]['OEM_METAL']=1
                        self.df.loc[PN,:]['RAW_METAL']=1
            elif len(self.data[self.data['PART']==PN].iloc[0,:]['OEM_VENDOR'])<5:
                if len(self.data[self.data['PART']==PN].iloc[0,:]['PROD_FAMILY'])>=5:
                    Nonmetal = ('O-RING','ELASTOMERS','INSULATION MATERIAL')
                    metal = ('WASHER','LABELS','KEY','CABLE','SPRING','HYDRAULIC TUBE/PIPE','PIPE-TUBE-FITTINGS',
                             'FASTERNS','HYDRAULIC COUPLER COMPONENTS','ANODE','SCREW','TUBULAR')
                    for i in Nonmetal:
                        if self.data[self.data['PART']==PN].iloc[0,:]['PROD_FAMILY']==i:
                            self.df.loc[PN,:]['OEM_NONMETAL']=1
                            self.df.loc[PN,:]['RAW_NONMETAL']=1
                    for j in metal:
                        if self.data[self.data['PART']==PN].iloc[0,:]['PROD_FAMILY']==j:
                            if sum(self.df.loc[PN,:][self.cat])<1:
                                self.df.loc[PN,:]['OEM_METAL']=1
                                self.df.loc[PN,:]['RAW_METAL']=1
                    word2 = self.word.split(self.data[self.data['PART']==PN].iloc[0,:]['PROD_FAMILY'])
                    terms = ('FASTENER','SCREW','SPRING')
                    for i in word2:
                        for j in terms:
                            if j in i:
                                self.df.loc[PN,:]['OEM_METAL']=1
                                self.df.loc[PN,:]['RAW_METAL']=1
                    if len(self.data[self.data['PART']==PN].iloc[0,:]['QUALITY SPECS'])>=5:
                        word3 = self.word.split(self.data[self.data['PART']==PN].iloc[0,:]['QUALITY SPECS'])
                        if 'Q00500' in word3:
                            if sum(self.df.loc[PN,:][self.cat])<1.0:
                                self.df.loc[PN,:]['OEM_METAL']=1
                                self.df.loc[PN,:]['RAW_METAL']=1
                    elif 'SEAL' in word2:
                        if self.data[self.data['PART']==PN].iloc[0,:]['MODEL']=='NO-DWG':
                            word4 = self.word.split(self.data[self.data['PART']==PN].iloc[0,:]['QUALITY SPECS'])
                            q_spec = ('Q03801','Q03802','Q03803')
                            for i in q_spec:
                                if i in word4:
                                    if sum(self.df.loc[PN,:][self.cat])<1.0:
                                        self.df.loc[PN,:]['OEM_NONMETAL']=1
                                        self.df.loc[PN,:]['RAW_NONMETAL']=1
                                else:
                                    if sum(self.df.loc[PN,:][self.cat])<1.0:                            
                                        self.df.loc[PN,:]['OEM_METAL']=1
                                        self.df.loc[PN,:]['RAW_METAL']=1
                    elif len(self.data[self.data['PART']==PN].iloc[0,:]['COMPONENTS'])<5:
                        if len(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])<5:
                            if self.data[self.data['PART']==PN].iloc[0,:]['Weight_kg']<1.0:
                                if sum(self.df.loc[PN,:][self.cat])<1.0:
                                    self.df.loc[PN,:]['OEM_NONMETAL']=1
                                    self.df.loc[PN,:]['RAW_NONMETAL']=1
                        elif len(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])>=5:
                            if self.data[self.data['PART']==PN].iloc[0,:]['MODEL']=='NO-DWG':
                                if self.data[self.data['PART']==PN].iloc[0,:]['Weight_kg']<1.0:
                                    if sum(self.df.loc[PN,:][self.cat])<1.0:
                                        self.df.loc[PN,:]['OEM_NONMETAL']=1
                                        self.df.loc[PN,:]['RAW_NONMETAL']=1
                                elif sum(self.df.loc[PN,:][self.cat])<1.0:
                                    self.df.loc[PN,:]['OEM_METAL']=1
                                    self.df.loc[PN,:]['RAW_METAL']=1
                    elif self.data[self.data['PART']==PN].iloc[0,:]['MODEL']=='NO-DWG':
                        if len(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])<5:
                            if sum(self.df.loc[PN,:][self.cat])<1.0:
                                print('OEM_METAL',PN)
                                self.df.loc[PN,:]['OEM_METAL']=1
                                self.df.loc[PN,:]['RAW_METAL']=1
    def forging(self,PN):
        if self.data[self.data['PART']==PN].iloc[0,:]['Single_ASSY'] =='Single':
            if self.data[self.data['PART']==PN].iloc[0,:]['PROD_FAMILY']=='BILLET':
                self.df.loc[PN,:]['FORGING']=1
                self.df.loc[PN,:]['RAW_METAL']=1
            elif self.data[self.data['PART']==PN].iloc[0,:]['PROD_FAMILY']=='FORGING':
                self.df.loc[PN,:]['FORGING']=1
                self.df.loc[PN,:]['RAW_METAL']=1
            elif self.data[self.data['PART']==PN].iloc[0,:]['PROD_FAMILY']=='BAR STOCK':
                self.df.loc[PN,:]['FORGING']=1
                self.df.loc[PN,:]['RAW_METAL']=1
            elif self.data[self.data['PART']==PN].iloc[0,:]['PROD_FAMILY']=='SEMI FINISHED':
                self.df.loc[PN,:]['FORGING']=1
                self.df.loc[PN,:]['RAW_METAL']=1
            elif len(self.data[self.data['PART']==PN].iloc[0,:]['OEM_VENDOR'])<5:
                if len(self.data[self.data['PART']==PN].iloc[0,:]['COMPONENTS'])<5:
                    if len(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])>5:
                        word = self.word.split(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])
                        spec = ('P60161','M2','M3','M40')
                        for i in word:
                            for j in spec:
                                if j in i and self.data[self.data['PART']==PN].iloc[0,:]['Weight_kg']>0.5:
                                    self.df.loc[PN,:]['FORGING']=1
                                    self.df.loc[PN,:]['RAW_METAL']=1
    def machining(self,PN):
        m_spec =('E55','E50','E47')
        eng_words = self.word.split(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])
        LEVEL = self.data[self.data['PART']==PN].iloc[0,:]['LEVEL']
        if 'A' in LEVEL:
            LEVEL = LEVEL[0]
        else:
            pass
        lower_level = str(int(LEVEL)+1)
        up_level = str(int(LEVEL)-1)
        idx =list(self.data[self.data['PART']==PN].index)
        level_df =self.data.iloc[idx[0]:,:]
        level_list = list(level_df[level_df['LEVEL']==LEVEL].index)
        # slice below this PN all items
        if len(level_list)==1:
            lower_df = self.data.iloc[level_list[0]:,:]
        elif len(level_list)>=2:
            lower_df =self.data.iloc[level_list[0]:level_list[1],:]
        up_list = list(lower_df[lower_df['LEVEL']==up_level].index)
        if len(up_list)==0:
            pass
        elif len(up_list)>=1:
            lower_df=self.data.iloc[idx[0]:up_list[0],:]
        lower_list = list(lower_df[lower_df['LEVEL']==lower_level].index)                                      
        weight_level = self.data[self.data['PART']==PN].iloc[0,:]['Weight_kg']
        if len(lower_list)==0:
            for i in m_spec:
                for j in eng_words:
                    if i in j:
                        if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                            self.df.loc[PN,:]['MACHINING']=1
                            self.df.loc[PN,:]['RAW_NONMETAL']=1
            if len(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])<5:
                if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                    self.df.loc[PN,:]['MACHINING']=1
                    self.df.loc[PN,:]['RAW_NONMETAL']=1
        elif len(lower_list)==1:
            weight_lower = self.data.iloc[lower_list[0],:]['Weight_kg']
            lower_qty = self.data.iloc[lower_list[0],:]['QTY']
            if weight_level <= weight_lower*lower_qty:
                self.df.loc[PN,:]['MACHINING']=1
        elif len(lower_list)==2:
            weight_lower0 = self.data.iloc[lower_list[0],:]['Weight_kg']
            weight_lower1 = self.data.iloc[lower_list[1],:]['Weight_kg']
            lower_qty0 = self.data.iloc[lower_list[0],:]['QTY']
            lower_qty1 = self.data.iloc[lower_list[1],:]['QTY']
            if weight_level < (weight_lower0*lower_qty0 + weight_lower1*lower_qty1):
                self.df.loc[PN,:]['MACHINING']=1
        elif len(lower_list)==3:
            weight_lower0 = self.data.iloc[lower_list[0],:]['Weight_kg']
            weight_lower1 = self.data.iloc[lower_list[1],:]['Weight_kg']
            weight_lower2 = self.data.iloc[lower_list[2],:]['Weight_kg']
            lower_qty0 = self.data.iloc[lower_list[0],:]['QTY']
            lower_qty1 = self.data.iloc[lower_list[1],:]['QTY']
            lower_qty2 = self.data.iloc[lower_list[2],:]['QTY']
            if weight_level < (weight_lower0*lower_qty0 + weight_lower1*lower_qty1+
                               weight_lower2*lower_qty2):
                self.df.loc[PN,:]['MACHINING']=1
        elif self.data[self.data['PART']==PN].iloc[0,:]['Single_ASSY'] =='Single':        
            if len(self.data[self.data['PART']==PN].iloc[0,:]['OEM_VENDOR'])<5:
                if len(self.data[self.data['PART']==PN].iloc[0,:]['COMPONENTS'])<5:
                    if len(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])<5:
                        if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                            self.df.loc[PN,:]['MACHINING']=1
                            self.df.loc[PN,:]['RAW_NONMETAL']=1
    def cladding(self,PN):
        term =('OVERLAY','INLAY','CLADDING')
        words = self.word.split(self.data[self.data['PART']==PN].iloc[0,:]['PART DESCRIPTION'])
        if len(self.data[self.data['PART']==PN].iloc[0,:]['OEM_VENDOR'])<5:
            if len(self.data[self.data['PART']==PN].iloc[0,:]['COMPONENTS'])>5:
                for j in term:
                    if j in words:
                        LEVEL = self.data[self.data['PART']==PN].iloc[0,:]['LEVEL']
                        if 'A' in LEVEL:
                            LEVEL = LEVEL[0]
                        else:
                            idx = list(self.data[self.data['PART']==PN].index)
                            i = idx[0]
                            level_weight = self.data.loc[i,'Weight_kg']
                            # LEVEL = self.data.loc[i,'LEVEL']
                            df1 = self.data.iloc[i:,:] #cutoff iBOM this PN below.
                            level_list = list(df1[df1['LEVEL']==LEVEL]['LEVEL'].index)
                            lower_level = str(int(LEVEL)+1)
                            up_level = str(int(LEVEL)-1)
                            up_level_list =list(df1[df1['LEVEL']==up_level].index)
                            if len(up_level_list)==0:
                                if len(level_list)==1:
                                    lower_df_list = list(df1[df1['LEVEL']==lower_level].index)
                                    if len(lower_df_list)==0:
                                        pass
                                    elif len(lower_df_list)>=1:
                                        lr_qty = self.data.loc[lower_df_list,'QTY']
                                        lr_weight = self.data.loc[lower_df_list,'Weight_kg']
                                        lr_mass_sum = sum(lr_qty*lr_weight)
                                        if level_weight >= lr_mass_sum:
                                            self.df.loc[PN,:]['CLADDING']=1
                                            self.df.loc[PN,:]['RAW_METAL']= 1
                                elif len(level_list)>=2:
                                    df2 = self.data.iloc[i:level_list[1],]
                                    lower_df_list = list(df1[df1['LEVEL']==lower_level].index)
                                    if len(lower_df_list)==0:
                                        pass
                                    elif len(lower_df_list)>=1:
                                        lr_qty = self.data.loc[lower_df_list,'QTY']
                                        lr_weight = self.data.loc[lower_df_list,'Weight_kg']
                                        lr_mass_sum = sum(lr_qty*lr_weight)
                                        if level_weight >= lr_mass_sum:
                                            self.df.loc[PN,:]['CLADDING']=1
                                            self.df.loc[PN,:]['RAW_METAL']= 1
                            elif len(up_level_list)>=1:
                                if len(level_list)==1:
                                    df3 = data.iloc[i:level_list[0],]
                                    lower_df_list = list(df3[df3['LEVEL']==lower_level].index)
                                    if len(lower_df_list)==0:
                                        pass
                                    elif len(lower_df_list)>=1:
                                        lr_qty = self.data.loc[lower_df_list,'QTY']
                                        lr_weight = self.data.loc[lower_df_list,'Weight_kg']
                                        lr_mass_sum = sum(lr_qty*lr_weight)
                                        if level_weight >= lr_mass_sum:
                                            self.df.loc[PN,:]['CLADDING']=1
                                            self.df.loc[PN,:]['RAW_METAL']= 1
                                elif len(level_list)>=2:
                                    if level_list[1]<up_level_list[0]:
                                        df2 = data.iloc[i:level_list[1],]
                                        lower_df_list =list(df2[df2['LEVEL']==lower_level].index)
                                        if len(lower_df_list)==0:
                                            pass
                                        elif len(lower_df_list)>=1:
                                            lr_qty = self.data.loc[lower_df_list,'QTY']
                                            lr_weight = self.data.loc[lower_df_list,'Weight_kg']
                                            lr_mass_sum = sum(lr_qty*lr_weight)
                                            if level_weight >= lr_mass_sum:
                                                self.df.loc[PN,:]['CLADDING']=1
                                                self.df.loc[PN,:]['RAW_METAL']=1
                                    elif level_list[1] >up_level_list[0]:
                                        df2 = data.iloc[i:up_level_list[0],]
                                        lower_df_list =list(df2[df2['LEVEL']==lower_level].index)
                                        if len(lower_df_list)==0:
                                            pass
                                        elif len(lower_df_list)>=1:
                                            lr_qty = self.data.loc[lower_df_list,'QTY']
                                            lr_weight = self.data.loc[lower_df_list,'Weight_kg']
                                            lr_mass_sum = sum(lr_qty*lr_weight)
                                            if level_weight >= lr_mass_sum:
                                                self.df.loc[PN,:]['CLADDING']=1
                                                self.df.loc[PN,:]['RAW_METAL']=1                                          
    def fabrication(self,PN):
        term= ('WELDMENT','FRAME','STRUCTURE','MUDMAT')
        Q_spec = ('Q00070','Q00075','Q00083','Q00825')
        w_spec =('W99000','W99101')
        m_spec =('M10','M11','M12')
        desc_words = self.word.split(self.data[self.data['PART']==PN].iloc[0,:]['PART DESCRIPTION'])
        eng_words = self.word.split(self.data[self.data['PART']==PN].iloc[0,:]['ENGINEERING SPECS'])
        Q_words = self.word.split(self.data[self.data['PART']==PN].iloc[0,:]['QUALITY SPECS'])
        LEVEL = self.data[self.data['PART']==PN].iloc[0,:]['LEVEL']
        if 'A' in LEVEL:
            LEVEL = LEVEL[0]
        else:
            pass
        lower_level = str(int(LEVEL)+1)
        up_level = str(int(LEVEL)-1)
        idx =list(self.data[self.data['PART']==PN].index)
        level_df =self.data.iloc[idx[0]:,:]
        level_list = list(level_df[level_df['LEVEL']==LEVEL].index)
        # slice below this PN all items
        if len(level_list)==1:
            lower_df = self.data.iloc[level_list[0]:,:]
        elif len(level_list)>=2:
            lower_df =self.data.iloc[level_list[0]:level_list[1],:]
        up_list = list(lower_df[lower_df['LEVEL']==up_level].index)
        if len(up_list)==0:
            pass
        elif len(up_list)>=1:
            lower_df=self.data.iloc[idx[0]:up_list[0],:]
        lower_list = list(lower_df[lower_df['LEVEL']==lower_level].index)
        # weight_level = self.data[self.data['PART']==PN].iloc[0,:]['Weight_kg']
        for i in term:
            if i in desc_words:
                if len(lower_list)<=1:
                    if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                        self.df.loc[PN,:]['FABRICATION']=1
                        self.df.loc[PN,:]['RAW_METAL']=1
                elif len(lower_list)>=2:
                    if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                        self.df.loc[PN,:]['FABRICATION']=1
                        self.df.loc[PN,:]['RAW_METAL']=1
        for j in  Q_spec:
            if j in Q_words:
                if len(lower_list)<=1:
                    if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                        self.df.loc[PN,:]['FABRICATION']=1
                        self.df.loc[PN,:]['RAW_METAL']=1
                elif len(lower_list)>=2:
                    if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                        self.df.loc[PN,:]['FABRICATION']=1
        for k in w_spec:
            if k in eng_words:
                if len(lower_list)<=1:
                    if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                        self.df.loc[PN,:]['FABRICATION']=1
                        self.df.loc[PN,:]['RAW_METAL']=1
                elif len(lower_list)>=2:
                    if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                        self.df.loc[PN,:]['FABRICATION']=1
        for l in m_spec:
            for m in eng_words:
                if l in m:
                    if len(lower_list)<=1:
                        if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                            self.df.loc[PN,:]['FABRICATION']=1
                            self.df.loc[PN,:]['RAW_METAL']=1
                    elif len(lower_list)>=2:
                        if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                            self.df.loc[PN,:]['FABRICATION']=1
    def proccesSet(self):
        for i in self.df.index:
            # self.Series= self.data[self.data['PART']==i].iloc[0,:]
            self.chemical(i)
            self.assy(i)
            self.casting(i)
            self.coating(i)
            self.forging(i)
            self.cladding(i)
            self.OEM(i)
            self.machining(i)
            self.fabrication(i)
        return self.df
# Prs = ProcessIdentify(data_rawlv)
# df_Prs =Prs.proccesSet()
# print(df_Prs.head(2),f'initial df shape is {df_Prs.shape}')
# df_Prs.to_excel('data/P4000083572W01_process.xlsx')# write clear data to local file
# df_pncty.to_excel('data/P1000220410_vendor.xlsx',index=False)# write clear data to local file

# df_concat = Twodfconcat.combine(df_Prs, df_0,index=True,start=10)

# df_concat.to_excel('data/P4000083572W01_pr&vr.xlsx')# write clear data to local file
# df_complete= Twodfconcat.combine(data_rawlv,df_Prs,start=0)
# df_complete.columns
# df_complete.to_excel('data/P1000220410_complete_test.xlsx',index=False)

class RawMaterial:
    def rawNetMass(self,data):
        data.insert(loc= 33,column='Raw_Net_Mass',value=0)
        for i,j in enumerate(data['PART']):
            if data.loc[i,'Single_ASSY']=='Single':
                if 'A' in data.loc[i,'LEVEL']:
                    pass 
                else:
                    level_weight = data.loc[i,'Weight_kg']
                    LEVEL = data.loc[i,'LEVEL']
                    df1 = data.iloc[i:,:]
                    level_list = list(df1[df1['LEVEL']==LEVEL]['LEVEL'].index)
                    lower_level = str(int(LEVEL)+1)
                    up_level = str(int(LEVEL)-1)
                    up_level_list =list(df1[df1['LEVEL']==up_level].index)
                    if len(up_level_list)==0:
                        if len(level_list)==1:
                            lower_df_list = list(df1[df1['LEVEL']==lower_level].index)
                            if len(lower_df_list)==0:
                                if data.loc[i,'RAW_METAL']==1:
                                    data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
                                elif data.loc[i,'RAW_NONMETAL']==1:
                                    data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
                            elif len(lower_df_list)>=1:
                                lr_qty = data.loc[lower_df_list,'QTY']
                                lr_weight = data.loc[lower_df_list,'Weight_kg']
                                lr_mass_sum = sum(lr_qty*lr_weight)
                                if level_weight<=lr_mass_sum:
                                    data.loc[i,'Raw_Net_Mass']= 0
                                elif level_weight > lr_mass_sum:
                                    if data.loc[i,'RAW_METAL']==1:
                                        data.loc[i,'Raw_Net_Mass']= level_weight - lr_mass_sum
                        elif len(level_list)>=2:
                            df2 = data.iloc[i:level_list[1],]
                            lower_df_list = list(df1[df1['LEVEL']==lower_level].index)
                            if len(lower_df_list)==0:
                                if data.loc[i,'RAW_METAL']==1:
                                    data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
                                elif data.loc[i,'RAW_NONMETAL']==1:
                                    data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
                            elif len(lower_df_list)>=1:
                                lr_qty = data.loc[lower_df_list,'QTY']
                                lr_weight = data.loc[lower_df_list,'Weight_kg']
                                lr_mass_sum = sum(lr_qty*lr_weight)
                                if level_weight<=lr_mass_sum:
                                    data.loc[i,'Raw_Net_Mass']= 0
                                elif level_weight > lr_mass_sum:
                                    if data.loc[i,'RAW_METAL']==1:
                                        data.loc[i,'Raw_Net_Mass']= level_weight - lr_mass_sum
                                    elif data.loc[i,'RAW_NONMETAL']==1:
                                        data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
                    elif len(up_level_list)>=1:
                        if len(level_list)==1:
                            df3 = data.iloc[i:level_list[0],]
                            lower_df_list = list(df3[df3['LEVEL']==lower_level].index)
                            if len(lower_df_list)==0:
                                if data.loc[i,'RAW_METAL']==1:
                                    data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
                                elif data.loc[i,'RAW_NONMETAL']==1:
                                    data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
                            elif len(lower_df_list)>=1:
                                lr_qty = data.loc[lower_df_list,'QTY']
                                lr_weight = data.loc[lower_df_list,'Weight_kg']
                                lr_mass_sum = sum(lr_qty*lr_weight)
                                if level_weight<=lr_mass_sum:
                                    data.loc[i,'Raw_Net_Mass']= 0
                                elif level_weight > lr_mass_sum:
                                    if data.loc[i,'RAW_METAL']==1:
                                        data.loc[i,'Raw_Net_Mass']= level_weight - lr_mass_sum
                                    elif data.loc[i,'RAW_NONMETAL']==1:
                                        data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
                        elif len(level_list)>=2:
                            if level_list[1]<up_level_list[0]:
                                df2 = data.iloc[i:level_list[1],]
                                lower_df_list =list(df2[df2['LEVEL']==lower_level].index)
                                if len(lower_df_list)==0:
                                    if data.loc[i,'RAW_METAL']==1:
                                        data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
                                    elif data.loc[i,'RAW_NONMETAL']==1:
                                        data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
                                elif len(lower_df_list)>=1:
                                    lr_qty = data.loc[lower_df_list,'QTY']
                                    lr_weight = data.loc[lower_df_list,'Weight_kg']
                                    lr_mass_sum = sum(lr_qty*lr_weight)
                                    if level_weight<=lr_mass_sum:
                                        data.loc[i,'Raw_Net_Mass']= 0
                                    elif level_weight > lr_mass_sum:
                                        if data.loc[i,'RAW_METAL']==1:
                                            data.loc[i,'Raw_Net_Mass']= level_weight - lr_mass_sum
                                        elif data.loc[i,'RAW_NONMETAL']==1:
                                            data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
                            elif level_list[1] >up_level_list[0]:
                                df2 = data.iloc[i:up_level_list[0],]
                                lower_df_list =list(df2[df2['LEVEL']==lower_level].index)
                                if len(lower_df_list)==0:
                                    if data.loc[i,'RAW_METAL']==1:
                                        data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
                                    elif data.loc[i,'RAW_NONMETAL']==1:
                                        data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
                                elif len(lower_df_list)>=1:
                                    lr_qty = data.loc[lower_df_list,'QTY']
                                    lr_weight = data.loc[lower_df_list,'Weight_kg']
                                    lr_mass_sum = sum(lr_qty*lr_weight)
                                    if level_weight<=lr_mass_sum:
                                        data.loc[i,'Raw_Net_Mass']= 0
                                    elif level_weight > lr_mass_sum:
                                        if data.loc[i,'RAW_METAL']==1:
                                            data.loc[i,'Raw_Net_Mass']= level_weight - lr_mass_sum
                                        elif data.loc[i,'RAW_NONMETAL']==1:
                                            data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
            elif data.loc[i,'Single_ASSY']=='TopAssy':
                top_weight = data.loc[i,'Weight_kg']
                lower_level = '2'
                level_list = list(data[data['LEVEL']==lower_level].index)
                lower_qty = data.loc[level_list,'QTY']
                lower_weight = data.loc[level_list,'Weight_kg']
                lower_total_wtg = sum(lower_qty *lower_weight)
                if top_weight <= lower_total_wtg:
                    data.loc[i,'Raw_Net_Mass']= 0
                elif top_weight > lower_total_wtg:
                    data.loc[i,'Raw_Net_Mass']= top_weight-lower_total_wtg
            elif data.loc[i,'Single_ASSY']=='Assy':
                if 'A' in data.loc[i,'LEVEL']:
                    pass 
                else:
                    level_weight = data.loc[i,'Weight_kg']
                    LEVEL = data.loc[i,'LEVEL']
                    df1 = data.iloc[i:,:]
                    level_list = list(df1[df1['LEVEL']==LEVEL]['LEVEL'].index)
                    lower_level = str(int(LEVEL)+1)
                    up_level = str(int(LEVEL)-1)
                    up_level_list =list(df1[df1['LEVEL']==up_level].index)
                    if len(up_level_list)==0:
                        if len(level_list)==1:
                            lower_df_list = list(df1[df1['LEVEL']==lower_level].index)
                            lr_qty = data.loc[lower_df_list,'QTY']
                            lr_weight = data.loc[lower_df_list,'Weight_kg']
                            lr_mass_sum = sum(lr_qty*lr_weight)
                            if level_weight<=lr_mass_sum:
                                data.loc[i,'Raw_Net_Mass']= 0
                            elif level_weight > lr_mass_sum:
                                data.loc[i,'Raw_Net_Mass']= level_weight - lr_mass_sum
                        elif len(level_list)>=2:
                            df2 = data.iloc[i:level_list[1],]
                            lr_level_list =list(df2[df2['LEVEL']==lower_level].index)
                            lr_qty = data.loc[lr_level_list,'QTY']
                            lr_weight = data.loc[lr_level_list,'Weight_kg']
                            lr_mass_sum = sum(lr_qty*lr_weight)
                            if level_weight<=lr_mass_sum:
                                data.loc[i,'Raw_Net_Mass']= 0
                            elif level_weight > lr_mass_sum:
                                data.loc[i,'Raw_Net_Mass']= level_weight - lr_mass_sum
                    elif len(up_level_list)>=1:
                        if len(level_list)==1:
                            df3 = data.iloc[i:level_list[0],]
                            lower_df_list = list(df3[df3['LEVEL']==lower_level].index)
                            lr_qty = data.loc[lower_df_list,'QTY']
                            lr_weight = data.loc[lower_df_list,'Weight_kg']
                            lr_mass_sum = sum(lr_qty*lr_weight)
                            if level_weight<=lr_mass_sum:
                                data.loc[i,'Raw_Net_Mass']= 0
                            elif level_weight > lr_mass_sum:
                                data.loc[i,'Raw_Net_Mass']= level_weight - lr_mass_sum
                        elif len(level_list)>=2:
                            if level_list[1]<up_level_list[0]:
                                df2 = data.iloc[i:level_list[1],]
                                lr_level_list =list(df2[df2['LEVEL']==lower_level].index)
                                lr_qty = data.loc[lr_level_list,'QTY']
                                lr_weight = data.loc[lr_level_list,'Weight_kg']
                                lr_mass_sum = sum(lr_qty*lr_weight)
                                if level_weight<=lr_mass_sum:
                                    data.loc[i,'Raw_Net_Mass']= 0
                                elif level_weight > lr_mass_sum:
                                    data.loc[i,'Raw_Net_Mass']= level_weight - lr_mass_sum
                            elif level_list[1] >up_level_list[0]:
                                df2 = data.iloc[i:up_level_list[0],]
                                lr_level_list =list(df2[df2['LEVEL']==lower_level].index)
                                lr_qty = data.loc[lr_level_list,'QTY']
                                lr_weight = data.loc[lr_level_list,'Weight_kg']
                                lr_mass_sum = sum(lr_qty*lr_weight)
                                if level_weight<=lr_mass_sum:
                                    data.loc[i,'Raw_Net_Mass']= 0
                                elif level_weight > lr_mass_sum:
                                    data.loc[i,'Raw_Net_Mass']= level_weight - lr_mass_sum
    def raW_Uti(self,data):
        data.insert(loc=34,column='Raw_Uti',value=1)
        for i,j in enumerate(data['PART']):
            if data.loc[i,'FORGING']==1:
                if data.loc[i,'MODEL']=='NO-DWG':
                    if data.loc[i,'Weight_kg'] * data.loc[i,'QTY']<100:
                        data.loc[i,'Raw_Uti']=0.7
                    elif data.loc[i,'Weight_kg'] * data.loc[i,'QTY'] >=100:
                        data.loc[i,'Raw_Uti']=0.65
                elif data.loc[i,'MODEL']!='NO-DWG':
                    if data.loc[i,'Weight_kg']<100:
                        data.loc[i,'Raw_Uti']=0.6
                    elif data.loc[i,'Weight_kg']>=100 and data.loc[i,'Weight_kg']<500:
                        data.loc[i,'Raw_Uti']=0.5
                    elif data.loc[i,'Weight_kg']>500:
                        data.loc[i,'Raw_Uti']=0.45
            elif data.loc[i,'FORGING']==0 and data.loc[i,'RAW_METAL']==1:
                data.loc[i,'Raw_Uti']=0.8
            elif data.loc[i,'FORGING']==0 and data.loc[i,'RAW_NONMETAL']==1:   
                data.loc[i,'Raw_Uti']=0.4
    def setUp(self,data):
        self.rawNetMass(data)
        self.raW_Uti(data)
        return data
    

# Raw = RawMaterial()
# df_final =Raw.setUp(df_complete)

# df_final.to_excel('data/P7000094258-26_final.xlsx',index=False)

# the class assign each of BOM item purchased vendor country and city '
# also topleve PN production site

class MfgLocation:
    @staticmethod
    def sourcingPN(data0:pd.DataFrame,data1:pd.DataFrame)->pd.DataFrame:
        location = list(set(data0['COUNTRY_CODE']))
        plant_coty = list(filter(None,location))
        toplevelpn= data0['PN0'][0]
        df = pd.DataFrame()
        PNs = data1['PART'].unique().tolist()
        for i,j in enumerate(PNs):
            country = set(data1[data1['PART']==j]['VENDOR_COUNTRY'].tolist())
            coty_uni = list(filter(None,country))
            coty_name = ['Coty'+str(i) for i in range(len(coty_uni))]
            df.loc[i,'PN']=j
            df.loc[i,'Vendor_num']=len(coty_uni)
            df.loc[i,coty_name]=coty_uni
        if len(df[df['PN']==toplevelpn])==0:
            lth = len(df)
            df.loc[lth,'PN']=toplevelpn
            df.loc[lth,'Vendor_num']=len(plant_coty)
            coty_name = ['Coty'+str(i) for i in range(len(plant_coty))]
            df.loc[lth,coty_name]=plant_coty
        return df
class ProcesEF:
    @staticmethod
    def procesEF(data,EFe):
        '''EFe is dictionary'''
        for i,j in EFe.items():
            if i =='Country':
                data[i]=''
            else:
                data[i+'_Mass']=0
        for h in data.index:
            for k,l in EFe.items():
                if data.loc[h,k]==1:
                    col = k+'_Mass'
                    data.loc[h,col]=l
        return data 

# Final_df = ProcesEF.procesEF(df_final, EFe)
# Final_df.to_excel('data/fi_df.xlsx',index =False)
class TransEmission:
    @staticmethod
    def transCal(source:pd.DataFrame,target:pd.DataFrame,EF_Trans,plant_ixd=0,vendor_ixd=0):
        target.fillna('',inplace=True)
        target.index = target['PN']
        for i,j in enumerate(source['PART']):
            if i ==0:
                coty = (target.loc[j,].tolist())[2:]
                num = target.loc[j,'Vendor_num']
                coty = list(filter(str.isalpha,coty))
                if plant_ixd <=(num-1):
                    source['TFMC_plant_country']=coty[plant_ixd]
                else:
                    source['TFMC_plant_country']=coty[0]
                source.loc[i,'Vendor_coty']=source['TFMC_plant_country'][0]
            elif i>=1:
                if j not in target.index:
                    source.loc[i,'Vendor_coty']=''
                elif j in target.index:
                    coty = (target.loc[j,].tolist())[2:]
                    num = target.loc[j,'Vendor_num']
                    coty = list(filter(None,coty))
                    if len(coty)==0:
                        source.loc[i,'Vendor_coty']=''
                    else:
                        if vendor_ixd<=(num-1):
                            source.loc[i,'Vendor_coty']=coty[vendor_ixd]
                        else:
                            source.loc[i,'Vendor_coty']=coty[0]
        df_road, df_sea,df_air,df_ef = EF_Trans['Road'],EF_Trans['Sea'],EF_Trans['Air'],EF_Trans['EF']
        df_sea.index = df_sea['Unnamed: 0']
        source['Road_EF']=df_ef['Road'][1]
        source['Air_EF']=df_ef['Air'][1]
        source['Sea_EF']=df_ef['Ocean'][1]
        for k,l in enumerate(source['PART']):
            vendor_region = source.loc[k,'Vendor_coty']
            plant_region = source.loc[k,'TFMC_plant_country']
            if k ==0:
                source.loc[k,'Sea_label']=0
                source.loc[k,'Air_label']=0
                source.loc[k,'Orin_road_airport']=0
                source.loc[k,'Orin_road_seaport']=0
                source.loc[k,'sea_road_desti']=0
                source.loc[k,'airport_road_desti']=0
                source.loc[k,'Sea_dist']=0
                source.loc[k,'Air_dist']=0
            elif k>=1:
                if source.loc[k,'Weight_kg']<10.0:
                    source.loc[k,'Air_label']=1
                    source.loc[k,'Sea_label']=0
                elif source.loc[k,'Weight_kg']>=10.0:
                    source.loc[k,'Air_label']=0
                    source.loc[k,'Sea_label']=1
                if vendor_region=='':
                    source.loc[k,'Orin_road_airport']=0
                    source.loc[k,'Orin_road_seaport']=0
                    source.loc[k,'sea_road_desti']=0
                    source.loc[k,'sea_road_desti']=0
                    source.loc[k,'Sea_dist']=0
                    source.loc[k,'Air_dist']=0
                elif vendor_region!='':
                    source.loc[k,'Orin_road_airport']=df_road[df_road['Country_code']==vendor_region]['Orin_to_airport'].iloc[0,]
                    source.loc[k,'Orin_road_seaport']=df_road[df_road['Country_code']==vendor_region]['Orin_to_seaport'].iloc[0,]
                    source.loc[k,'airport_road_desti']=df_road[df_road['Country_code']==plant_region]['Airport_to_destination'].iloc[0,]
                    source.loc[k,'sea_road_desti']=df_road[df_road['Country_code']==plant_region]['Seaport_to_destination'].iloc[0,]
                    source.loc[k,'Sea_dist']=df_sea.loc[vendor_region,plant_region]
                    if vendor_region==plant_region:
                        source.loc[k,'Air_dist']=0
                    else:
                        source.loc[k,'Air_dist']=df_air[(df_air['Country0']==vendor_region)&\
                            (df_air['Country1']==plant_region)]['Distance_km'].iloc[0,]
            source.loc[k,'Road_emission']=((source.loc[k,'Orin_road_airport']+source.loc[k,'airport_road_desti'])*\
                source.loc[k,'Air_label']+(source.loc[k,'Orin_road_seaport']+source.loc[k,'sea_road_desti'])*\
                    source.loc[k,'Sea_label'])*source.loc[k,'Road_EF']*source.loc[k,'Sub_total_qty']*source.loc[k,'Weight_kg']/1000*1.1
            source.loc[k,'Air_emission']=source.loc[k,'Air_dist']*source.loc[k,'Air_label']*source.loc[k,'Air_EF']*\
            source.loc[k,'Sub_total_qty']*source.loc[k,'Weight_kg']/1000*1.1
            source.loc[k,'Sea_emission']= source.loc[k,'Air_dist']*source.loc[k,'Air_label']*source.loc[k,'Air_EF']*\
            source.loc[k,'Sub_total_qty']*source.loc[k,'Weight_kg']/1000*1.1
        source['Trans_total']= source['Road_emission'].sum()+source['Air_emission'].sum()+source['Sea_emission'].sum()
        source['Trans_total'][1:]=''
        return source
def emissionSum(data):
    cols = data.columns[-12:]
    data['Pr_sum']=0
    data['Raw_sum_metal']= data['Raw_Net_Mass']/data['Raw_Uti']*data['RAW_METAL_Mass']*data['Sub_total_qty']
    data['Raw_sum_non'] = data['Raw_Net_Mass']/data['Raw_Uti']*data['RAW_NONMETAL_Mass']*data['Sub_total_qty']
    for i in cols[0:10]:
        data['Pr_sum']+=data[i]
    data['Pr_total'] = data['Pr_sum']*data['Weight_kg']*data['Sub_total_qty']
    data['Raw_Total']=data['Raw_sum_metal']+data['Raw_sum_non']
    data['CO2_total_Mass'] = data['Raw_Total'] + data['Pr_total']
    total =sum(data['CO2_total_Mass'])
    # EF = total/(data['Weight_kg'][0]+0.01)
    return data, total

# data,total, EF = emissionSum(Final_df)
# data.to_excel('data/fi_df_P7000073756.xlsx',index =False)

def decorator(fun):
    def wrapper(*args,**kargs):
        print(*args)
        c = fun(*args,**kargs)
        return c
    return wrapper



if __name__== '__main__':
    PN = 'P6000197742'
    data, EFe,EF_Transport= LoadingData.LoadingData(PN)
    # read data from loacal iBOM and emission factor table
    EFe = GetEF.getEF(EFe)
    # create EFe dict ignore the region currently
    # data1= DFClear.dfClear(data)
    data0 =DataClear.dataClear(data)
    description = data0['PART DESCRIPTION'][0][:35]
    df_toplevelpn,df_purchased,df_production= queryData(data0,PN=PN)
    #create dataframe of purchased parts and their puchase region
    df_PN_loction = MfgLocation.sourcingPN(df_production,df_purchased)
    # clear the iBOM data drop nan value
    df_toplevelpn =DFClear.dfClear(df_toplevelpn)
    # clear data convert each of column data into str type
    test_data = DataPrepare()
    # df_toplevelpn.to_excel('data/HCR.xlsx')
    # df_toplevelpn.shape, len(set(data0['PART']))
    data_rawlv=test_data.getData(data0,df_toplevelpn)
    # create subtotal and weight_kg column
    Prs = ProcessIdentify(data_rawlv)
    df_Prs =Prs.proccesSet()
    # identify each of PN itself mfg process 
    df_complete= Twodfconcat.combine(data_rawlv,df_Prs,start=0)
    # calculation transportation and logistic co2 emission
    df_comp_stran= TransEmission.transCal(df_complete,df_PN_loction,EF_Trans=EF_Transport,plant_ixd=1)
    TransEmissionTotal =df_comp_stran['Trans_total'][0]
    # concatenate iBOM and process data
    Raw = RawMaterial()
    df_final =Raw.setUp(df_complete)
    data_table = ProcesEF.procesEF(df_final, EFe)
    # sum up each of line co2 emission
    data_excel, proces_total = emissionSum(data_table)
    EF = (proces_total+TransEmissionTotal)/data_excel['Weight_kg'][0]
    data_excel['ProcessTotal']= proces_total
    data_excel['GrandTotal']=proces_total+TransEmissionTotal
    data_excel['GrandTotal'][1:]=''
    data_excel['ProcessTotal'][1:]=''
    data_excel['EF'] = EF
    data_excel['EF'][1:] =''
    path = r'C:\Users\zhusj\python\Output_data\TFMC'
    data_excel.to_excel(os.path.join(path,f'{PN}_final_table.xlsx'),index =False)
    df_comp_stran.to_excel(os.path.join(path,f'{PN}_final_table.xlsx'),index =False)
    print(f'Per piece {description} {PN} mfg process and raw materieal total emission is {proces_total:.0f} kg')
    print("*"*80)
    print(f'Per piece {description} {PN} mfg site is in {data_excel["TFMC_plant_country"][0]} plant and T&L mfg is {TransEmissionTotal:.0f} kg')
    print("*"*80)
    print(f'{PN} {description} emission factor is {EF:.2f} kg/kg')
    print(f"T&L contribution total is {(TransEmissionTotal/(proces_total+TransEmissionTotal)):.0%}")
