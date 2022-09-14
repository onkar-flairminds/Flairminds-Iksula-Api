
from typing import Dict
import numpy as np
import pandas as pd
import numpy as np
import datetime
import re
from fuzzywuzzy import fuzz
import mysql.connector as connection

from flask import Flask, request, jsonify

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
    # if (pid in Dict.keys()):
    #     return None
    # else:
    for key in Dict.keys():
        if pid in Dict[key]:
            return None
    Dict[pid] = []
    
    matching_Score_Dict = {}
    matching_Attributes_Dict = {}
    for pid_2 in pid_2_list:

        # if (pid_2 in Dict.keys()):
        #     return None
        # else:
        for key in Dict.keys():
            if pid_2 in Dict[key]:
                return None
                
        prod_1 = prod_pid[prod_pid[identifier]==pid].iloc[0]
        prod_2 = prod_df[prod_df[identifier]==pid_2].iloc[0]
        exactAttScore = 0
        exactAttMatched = []
        fuzzyAttMatched = []
        matching_attributes = []
        
        for att in Attributes:
            att_dict = {}
            if str(prod_1[att])=='nan' or prod_2[att]=='nan':
                continue
            if str(prod_1[att])=='' or prod_2[att]=='':
                continue
            if att in exactAtt:
                if str(prod_1[att]).strip()==str(prod_2[att]).strip():
                    exactAttMatched.append(str(att))
                    att_dict['attributes_name'] = str(att)
                    att_dict['current_value'] = str(prod_1[att])
                    att_dict['found_value'] = str(prod_2[att])
                    att_dict['score'] = 1.0 
                    exactAttScore = 1.0
                    matching_attributes.append(att_dict)
            if att in fuzzyAtt:
                score = ( fuzz.token_sort_ratio( str(prod_1[att]).lower(), str(prod_2[att]).lower()  ) )*0.01
                if score>0.10:
                    fuzzyAttMatched.append(str(att))
                    att_dict['attributes_name'] = str(att)
                    att_dict['current_value'] = str(prod_1[att])
                    att_dict['found_value'] = str(prod_2[att])
                    att_dict['score'] = round(score,4)
                    matching_attributes.append(att_dict)
        exactScore = exactAttScore 
        fuzzyScore = len(fuzzyAttMatched)/len(fuzzyAtt)
        
        if exactScore==1.0:
            Matching_Score = 1.0
        else:
            Matching_Score = round((3*(exactScore) + 2*(fuzzyScore))/5 , 4)
        # if Matching_Score > 0.70:
        matching_Score_Dict['{}'.format(pid_2)] = Matching_Score
        matching_Attributes_Dict['{}'.format(pid_2)] = matching_attributes

    pid_2_keys = sorted(matching_Score_Dict, key=matching_Score_Dict.get, reverse=True)[:3]
    for pid_2 in pid_2_keys:
        Dict[pid].append(pid_2)
        Similarity_Dict['{}:{}'.format(pid, pid_2)] = {}
        Similarity_Dict['{}:{}'.format(pid, pid_2)]['matching_attributes'] = matching_Attributes_Dict['{}'.format(pid_2)]
        Similarity_Dict['{}:{}'.format(pid, pid_2)]['matching_score'] = matching_Score_Dict['{}'.format(pid_2)]

@app.route('/get_results', methods = ['POST'])
def Run():
    request_data = pd.io.json.loads(request.data)
    
    try:
        # request_data = eval(pd.io.json.loads(request.data))
        # data_df = pd.read_json(request_data)
        request_data = request.json
        test = pd.io.json.json_normalize(request_data['data'])
        filter = request_data['filter']
        
        global Dict
        Dict = {}
        global Similarity_Dict
        Similarity_Dict = {}

        if test['data_type'].iloc[0]=='customer':
            # exactAtt = ['phone1', 'phone2', 'email']
            # fuzzyAtt = ['first_name', 'last_name', 'zip', 'company_name', 'address', 'city', 'county', 'state']
            exactAtt = request_data['exact']
            fuzzyAtt = request_data['similar']
            Attributes = exactAtt + fuzzyAtt
            prod_pid = test
            identifier = 'id'
            if filter=='':
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], customer[identifier], prod_pid, customer, identifier, exactAtt, fuzzyAtt, Attributes)
                        , axis = 1)
            else:
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], customer[customer[filter]==x[filter]][identifier], prod_pid, customer, identifier, exactAtt, fuzzyAtt, Attributes)
                            , axis = 1)
            Dataframe = pd.DataFrame()
            print(Dict)
            for PID in Dict.keys():
                Prod = []
                for pid_2 in Dict[PID]:
                    prod_1 = prod_pid[prod_pid[identifier]==PID]
                    prod_1['matching_score'] = Similarity_Dict['{}:{}'.format(PID, pid_2)]['matching_score']
                    prod_1['matching_{}'.format(identifier)] = pid_2
                    prod_1['matching_attributes'] = pd.io.json.dumps(Similarity_Dict['{}:{}'.format(PID, pid_2)]['matching_attributes'])
                    Prod.append(prod_1)
                if Prod!=[]:
                    Dataframe = Dataframe.append( pd.concat(Prod) , ignore_index = True )
        elif test['data_type'].iloc[0]=='product':
            # exactAtt = ['model_number', 'id']
            # fuzzyAtt = ['parent_leaf_guid', 'product_name_120', 'mfg_brand_name', 'marketing_copy_1500','bullet01', 'bullet02', 'bullet03', 'bullet04',
            #     'bullet05','manufacturer_warranty', 'bullet06',
            #     'california_title_20_compliant', 'shade_shape', 'shade_fitter_type',
            #     'shade_color_family', 'lighting_product_type',
            #     'certifications_and_listings', 'fixture_color_finish_family',
            #     'lamp_shade_material','color_finish', 'parts_accessories_type',
            #     'product_length_in', 'downrod_length_in','product_length_in', 'downrod_length_in', 'material',
            #     'product_height_in', 'product_depth_in',
            #     'product_weight-lb','product_width_in', 'product_diameter_in']
            exactAtt = request_data['exact']
            fuzzyAtt = request_data['similar']

            Attributes = exactAtt + fuzzyAtt
            prod_pid = test
            identifier = 'id'
            if filter=='':
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], product[identifier], prod_pid, product, identifier, exactAtt, fuzzyAtt, Attributes)
                        , axis = 1)
            else:
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], product[product[filter]==x[filter]][identifier], prod_pid, product, identifier, exactAtt, fuzzyAtt, Attributes)
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
                    Dataframe = Dataframe.append( pd.concat(Prod) , ignore_index = True )
        return jsonify(Dataframe.to_json(orient = 'records'))
    except Exception as e:
        dict = {
            'Error' : str(e),
            'check' : request_data
        }
        return jsonify(dict)


if __name__ == '__main__':
    app.run(host = '0.0.0.0',port='5000',debug=True)