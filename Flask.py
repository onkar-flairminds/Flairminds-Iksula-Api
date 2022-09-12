
from typing import Dict
import numpy as np
import pandas as pd
import numpy as np
import datetime
import re
from fuzzywuzzy import fuzz
import mysql.connector as connection

from flask import Flask, request

try:
    mydb = connection.connect(host="localhost", database = 'iksula',user="root", passwd="Pass@123",use_pure=True)
    query = "Select * from customer_master_records;"
    customer = pd.read_sql(query,mydb)
    mydb.close() #close the connection
except Exception as e:
    mydb.close()
    print('Error : '+str(e))
try:
    mydb = connection.connect(host="localhost", database = 'iksula',user="root", passwd="Pass@123",use_pure=True)
    query = "Select * from master_products_datas;"
    product = pd.read_sql(query,mydb)
    mydb.close() #close the connection
except Exception as e:
    mydb.close()
    print('Error : '+str(e))

app = Flask(__name__)

def applyDictionaryLogic(pid, pid_2_list, prod_pid, prod_df, identifier, exactAtt, fuzzyAtt, Attributes):
    pid_2_list = np.array(pid_2_list)
    if (pid in Dict.keys()):
        return None
    else:
        for key in Dict.keys():
            if pid in Dict[key]:
                return None
    Dict[pid] = []
    
    for pid_2 in pid_2_list:

        if (pid_2 in Dict.keys()):
            return None
        else:
            for key in Dict.keys():
                if pid_2 in Dict[key]:
                    return None
                
        prod_1 = prod_pid[prod_pid[identifier]==pid].iloc[0]
        prod_2 = prod_df[prod_df[identifier]==pid_2].iloc[0]
        exactAttMatched_List = ' | '
        exactAttMatched = []
        fuzzyAttmatched_List = ' | '
        fuzzyAttMatched = []
        matching_attributes = []
        
        for att in Attributes:
            att_dict = {}
            if str(prod_1[att])=='nan' or prod_2[att]=='nan':
                continue
            if att in exactAtt:
                if str(prod_1[att]).strip()==str(prod_2[att]).strip():
                    exactAttMatched_List += '{} | '.format(str(att))
                    exactAttMatched.append(str(att))
                    att_dict['attributes_name'] = str(att)
                    att_dict['current_value'] = str(prod_1[att])
                    att_dict['found_value'] = str(prod_2[att])
                    att_dict['score'] = 1.0
                    matching_attributes.append(att_dict)
            if att in fuzzyAtt:
                score = (fuzz.token_set_ratio( str(prod_1[att]).lower(), str(prod_2[att]).lower()  ) )*0.01
                if score>0.95:
                    fuzzyAttmatched_List += '{} | '.format(str(att))
                    fuzzyAttMatched.append(str(att))
                    att_dict['attributes_name'] = str(att)
                    att_dict['current_value'] = str(prod_1[att])
                    att_dict['found_value'] = str(prod_2[att])
                    att_dict['score'] = round(score,4)
                    matching_attributes.append(att_dict)
        exactScore = len(exactAttMatched)/len(exactAtt)
        fuzzyScore = len(fuzzyAttMatched)/len(fuzzyAtt)
        
        Matching_Score = round((3*(exactScore) + 2*(fuzzyScore))/5 , 4)
        if Matching_Score > 0.70:
            Dict[pid].append(pid_2)
            Similarity_Dict['{}:{}'.format(pid, pid_2)] = {}
            Similarity_Dict['{}:{}'.format(pid, pid_2)]['matching_attributes'] = matching_attributes
            Similarity_Dict['{}:{}'.format(pid, pid_2)]['matching_score'] = Matching_Score

@app.route('/get_results', methods = ['POST'])
def Run():
    request_data = request.data.decode()
    test = pd.read_json(request_data)
    print(type(test), test)
    global Dict
    Dict = {}
    global Similarity_Dict
    Similarity_Dict = {}

    if test['data_type'].iloc[0]=='customer':
        exactAtt = ['first_name', 'last_name', 'zip', 'phone1', 'phone2', 'email', 'web']
        fuzzyAtt = ['company_name', 'address', 'city', 'county', 'state']
        Attributes = exactAtt + fuzzyAtt
        prod_pid = test
        identifier = 'id'
        prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], customer[customer[identifier]!=x[identifier]][identifier], prod_pid, customer, identifier, exactAtt, fuzzyAtt, Attributes)
                    , axis = 1)
        Dataframe = pd.DataFrame()
        for PID in Dict.keys():
            Prod = []
            for pid_2 in Dict[PID]:
                prod_1 = prod_pid[prod_pid[identifier]==PID]
                prod_1['matching_score'] = Similarity_Dict['{}:{}'.format(PID, pid_2)]['matching_score']
                prod_1['matching_{}'.format(identifier)] = pid_2
                prod_1['matching_attributes'] = pd.io.json.dumps(Similarity_Dict['{}:{}'.format(PID, pid_2)]['matching_attributes'])
                Prod.append(prod_1)
            if Prod!=[]:
                Dataframe = pd.concat(Prod)
    elif test['data_type'].iloc[0]=='product':
        exactAtt = ['parent_leaf_guid', 'file_path','mfg_brand_name', 'model_number', 'product_name_120', 'parts_accessories_type',
                'product_length_in', 'downrod_length_in','product_length_in', 'downrod_length_in', 'material',
                'product_height_in', 'product_depth_in',
                'product_weight-lb','product_width_in', 'product_diameter_in']
        fuzzyAtt = ['marketing_copy_1500','bullet01', 'bullet02', 'bullet03', 'bullet04',
            'bullet05','manufacturer_warranty', 'bullet06',
            'california_title_20_compliant', 'shade_shape', 'shade_fitter_type',
            'shade_color_family', 'lighting_product_type',
            'certifications_and_listings', 'fixture_color_finish_family',
            'lamp_shade_material','color_finish']
        Attributes = exactAtt + fuzzyAtt
        prod_pid = test
        identifier = 'id'
        prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], product[product[identifier]!=x[identifier]][identifier], prod_pid, product, identifier, exactAtt, fuzzyAtt, Attributes)
                    , axis = 1)
        Dataframe = pd.DataFrame()
        for PID in Dict.keys():
            Prod = []
            for pid_2 in Dict[PID]:
                prod_1 = prod_pid[prod_pid[identifier]==PID]
                prod_1['matching_score'] = Similarity_Dict['{}:{}'.format(PID, pid_2)]['matching_score']
                prod_1['matching_{}'.format(identifier)] = pid_2
                prod_1['matching_attributes'] = pd.io.json.dumps(Similarity_Dict['{}:{}'.format(PID, pid_2)]['matching_attributes'])
                Prod.append(prod_1)
            if Prod!=[]:
                Dataframe = pd.concat(Prod)
    return Dataframe.to_json(orient = 'records')


if __name__ == '__main__':
    app.run(host = '0.0.0.0',port='5000',debug=True)