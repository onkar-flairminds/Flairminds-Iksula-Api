
from typing import Dict
from urllib.parse import ParseResultBytes
import numpy as np
import pandas as pd
import numpy as np
import datetime
import re
import os
from fuzzywuzzy import fuzz
from string import punctuation
try:
    import Levenshtein
except:
    os.system('pip3 install python-Levenshtein')
    import Levenshtein
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


def exactSimilarMatch(string1, string2):
    string1 = string1.lower().strip()
    string2 = string2.lower().strip()
    if string1 == string2:
        return 1.0
    return 0.0

def stringToWords(string):
    string = string.lower()
    string = re.sub(r'[^\w\s]', ' ', string)
    return_Array = [ word.strip() for word in string.split()]
    return return_Array

def JaccardSimilarity(stringArray1, stringArray2):
    stringArray1 = set(stringArray1)
    stringArray2 = set(stringArray2)
    TotalWords = len(set.union(stringArray1, stringArray2 ))
    CommonWords = len( stringArray1.intersection(stringArray2) )
    Score = CommonWords / TotalWords
    return round(Score, 4)

#ExactPreProcessing
def cleanMfgBrand(string):
    cleaned = string.lower()
    pattern = "[-, ,/]"
    cleaned = re.sub(pattern,"", string)
    return cleaned.lower()
def cleanPhoneNumber(phone_number):
    pattern = "[^\d]+"
    cleaned_phone_number = re.sub(pattern,"", phone_number)[-9:]
    return cleaned_phone_number
def cleanModelNumber(string):
    cleaned = string.lower()
    pattern = "[-, ,/]"
    cleaned = re.sub(pattern,"", cleaned)
    return cleaned
def cleanEmailTail(string):
    return re.sub(r'@.*', '', string)
def cleanWarranty(string):
    cleaned = string.lower()
    pattern = "[(,)]"
    cleaned = re.sub(pattern,"", cleaned)
    cleaned = cleaned.replace('-',' ')
    numlist = re.findall(r'\d+', cleaned)
    if len(numlist) > 0:
        return numlist[0]
    if 'none' in cleaned:
        return 'none'
    if 'no' in cleaned:
        return 'no'

#JacardPreProcessing
def cleanShadeShape(string):
    cleaned = string.lower()
    cleaned = cleaned.split(',')
    return cleaned

# Preprocessing
def PreProcesscustomer(customer):
    pass
def PreprocessProduct(product):
    pass

def groupmatching(att, matching_header, label, match_type, process, prod_1, prod_2, phone_fields, condition, group_array):
    prod_val1 = str(prod_1[att]).strip()
    att_group_dict = {}
    att_group_dict['group_name'] = label
    att_group_dict['field_name'] = att
    att_group_dict['matching_fields'] = {}
    score_list = []
    for header in matching_header:
        prod_val2 = str(prod_2[header]).strip()
        if header in phone_fields:
            prod_val1 = cleanPhoneNumber(prod_val1)
            prod_val2 = cleanPhoneNumber(prod_val2)
        if process=='email':
            prod_val1 = cleanEmailTail(prod_val1)
            prod_val2 = cleanEmailTail(prod_val2)
        if match_type=="exact":
            score = exactSimilarMatch(prod_val1, prod_val2)
            att_group_dict['matching_fields'][header] = score
            score_list.append(score)
        elif match_type=="similar":
            stringArray1 = stringToWords(str(prod_1[att]))
            stringArray2 = stringToWords(str(prod_2[att]))
            score = JaccardSimilarity(stringArray1, stringArray2)
            att_group_dict['matching_fields'][header] = score
            if score>condition:
                score_list.append(score)

    if len(score_list)!=0:
        group_score = max(score_list)
    else:
        group_score = 0
    att_group_dict['group_score'] = group_score
    group_array.append(att_group_dict)
    return group_score


def applyDictionaryLogic(pid, pid_2_list, prod_pid, prod_df, identifier, exactAtt, fuzzyAtt, Attributes, similar_exact, phone_fields,group1,group2,group3, condition):
    pid_2_list = np.array(pid_2_list)
    # if (pid in Dict.keys()):
    #     return None
    # else:
    # for key in Dict.keys():
    #     if pid in Dict[key]:
    #         return None
    Dict[pid] = []
    matching_Score_Dict = {}
    matching_Attributes_Dict = {}
    group1_att_dict = {}
    group2_att_dict = {}
    group3_att_dict = {}
    for pid_2 in pid_2_list:
        # if (pid_2 in Dict.keys()):
        #     return None
        # else:
        # for key in Dict.keys():
        #     if pid_2 in Dict[key]:
        #         return None
        prod_1 = prod_pid[prod_pid[identifier]==pid].iloc[0]
        prod_2 = prod_df[prod_df[identifier]==pid_2].iloc[0]
        exactAttScore = 0
        exactAttMatched = []
        fuzzyAttMatched = []
        matching_attributes = []
        group1_matching = []
        group1_scores = []
        group2_matching = []
        group2_scores = []
        group3_matching = []
        group3_scores = []
        
        for att in Attributes:
           
            att_dict = {}
            if str(prod_1[att])=='nan' or prod_2[att]=='nan':
                continue
            if str(prod_1[att])=='' or prod_2[att]=='':
                continue
            if att in exactAtt:
                prod_val1 = str(prod_1[att]).strip()
                prod_val2 = str(prod_2[att]).strip()
                if att in phone_fields:
                    prod_val1 = cleanPhoneNumber(prod_val1)
                    prod_val2 = cleanPhoneNumber(prod_val2)
                elif att=='model_number':
                    prod_val1 = cleanModelNumber(prod_val1)
                    prod_val2 = cleanModelNumber(prod_val2)
                
                if prod_val1==prod_val2:
                    exactAttMatched.append(str(att))
                    att_dict['attributes_name'] = str(att)
                    att_dict['current_value'] = str(prod_1[att])
                    att_dict['found_value'] = str(prod_2[att])
                    att_dict['score'] = 1.0
                    exactAttScore = 1.0
                    matching_attributes.append(att_dict)
                    break
                else:
                    att_dict['attributes_name'] = str(att)
                    att_dict['current_value'] = str(prod_1[att])
                    att_dict['found_value'] = str(prod_2[att])
                    att_dict['score'] = 0.0
                    matching_attributes.append(att_dict)
            if att in group1['data']:
                group1_scores.append(groupmatching(att, group1['data'], group1["label"], group1["match-type"], group1["process"], prod_1, prod_2,phone_fields, condition, group1_matching))
            if att in group2['data']:
                group2_scores.append(groupmatching(att, group2['data'], group2["label"], group2["match-type"], group2["process"], prod_1, prod_2,phone_fields, condition, group2_matching))
            if att in group3['data']:
                group3_scores.append(groupmatching(att, group3['data'], group3["label"], group3["match-type"], group3["process"], prod_1, prod_2,phone_fields, condition, group3_matching))
            
            if att in fuzzyAtt:
                # score = ( fuzz.token_sort_ratio( str(prod_1[att]).lower(), str(prod_2[att]).lower()  ) )*0.01
                
                # att_dict['attributes_name'] = str(att)
                # att_dict['current_value'] = str(prod_1[att])
                # att_dict['found_value'] = str(prod_2[att])
                # if score<=0.5:
                #     att_dict['score'] = 0.0
                # else:
                #     att_dict['score'] = round(score,4)

                # matching_attributes.append(att_dict)
                # if score>0.75:
                #     fuzzyAttMatched.append(str(att))

                if att in similar_exact:
                    prod_val1 = str(prod_1[att]).strip()
                    prod_val2 = str(prod_2[att]).strip()

                    if att =='mfg_brand_name':
                       prod_val1 = cleanMfgBrand(prod_val1)
                       prod_val2 = cleanMfgBrand(prod_val2)
                    elif att =='manufacturer_warranty':
                       prod_val1 = cleanWarranty(prod_val1)
                       prod_val2 = cleanWarranty(prod_val2)

                    score = exactSimilarMatch(str(prod_1[att]), str(prod_2[att]))
                    att_dict['attributes_name'] = str(att)
                    att_dict['current_value'] = str(prod_1[att])
                    att_dict['found_value'] = str(prod_2[att])
                    att_dict['score'] = score
                    matching_attributes.append(att_dict)
                    if score == 1.0:
                        fuzzyAttMatched.append(str(att))
                else:
                    stringArray1 = stringToWords(str(prod_1[att]))
                    stringArray2 = stringToWords(str(prod_2[att]))
                    if att == 'shade_shape':
                        stringArray1 = cleanShadeShape(str(prod_1[att]))
                        stringArray2 = cleanShadeShape(str(prod_2[att]))  
                    score = JaccardSimilarity(stringArray1, stringArray2)
                    att_dict['attributes_name'] = str(att)
                    att_dict['current_value'] = str(prod_1[att])
                    att_dict['found_value'] = str(prod_2[att])
                    att_dict['score'] = score
                    matching_attributes.append(att_dict)
                    if score > condition:
                        fuzzyAttMatched.append(str(att))

        exactScore = exactAttScore
        fuzzyScore = len(fuzzyAttMatched)/len(fuzzyAtt)
        
        if exactScore==1.0:
            Matching_Score = 1.0
        else:
            if len(group1_scores)!=0:
                group1_score = sum(group1_scores)/len(group1_scores)
            else:
                group1_score = 0
            if len(group2_scores)!=0:
                group2_score = sum(group2_scores)/len(group2_scores)
            else:
                group2_score = 0
            if len(group3_scores)!=0:
                group3_score = sum(group3_scores)/len(group3_scores)
            else:
                group3_score = 0
            overall_score = (fuzzyScore + group1_score + group2_score + group3_score)/4
            Matching_Score = round(overall_score , 4)
        # if Matching_Score > 0.70:
        matching_Score_Dict['{}'.format(pid_2)] = Matching_Score
        matching_Attributes_Dict['{}'.format(pid_2)] = matching_attributes
        group1_att_dict['{}'.format(pid_2)] = group1_matching
        group2_att_dict['{}'.format(pid_2)] = group2_matching
        group3_att_dict['{}'.format(pid_2)] = group3_matching

    pid_2_keys = sorted(matching_Score_Dict, key=matching_Score_Dict.get, reverse=True)[:3]

    for pid_2 in pid_2_keys:
        matching_score = matching_Score_Dict['{}'.format(pid_2)]
        if matching_score!=0:
            Dict[pid].append(pid_2)
            Similarity_Dict['{}:{}'.format(pid, pid_2)] = {}
            Similarity_Dict['{}:{}'.format(pid, pid_2)]['matching_attributes'] = matching_Attributes_Dict['{}'.format(pid_2)]
            Similarity_Dict['{}:{}'.format(pid, pid_2)]['matching_score'] = matching_score
            Similarity_Dict['{}:{}'.format(pid, pid_2)]['group1_matching'] = group1_att_dict['{}'.format(pid_2)]
            Similarity_Dict['{}:{}'.format(pid, pid_2)]['group2_matching'] = group2_att_dict['{}'.format(pid_2)]
            Similarity_Dict['{}:{}'.format(pid, pid_2)]['group3_matching'] = group3_att_dict['{}'.format(pid_2)]

@app.route('/get_results', methods = ['POST'])
def Run():
    request_data = pd.io.json.loads(request.data)
    try:
        # request_data = eval(pd.io.json.loads(request.data))
        # data_df = pd.read_json(request_data)
        request_data = request.json
        test = pd.io.json.json_normalize(request_data['data'])
        filter = request_data['filter']
        group1 = request_data['action-group']['type1']
        group2 = request_data['action-group']['type2']
        group3 = request_data['action-group']['type3']
        # group1 = type1['data']
        # group2 = type2['data']
        # group3 = type3['data']
        global Dict
        Dict = {}
        global Similarity_Dict
        Similarity_Dict = {}

        if test['data_type'].iloc[0]=='customer':
            similar_exact = ['zip','city','country','state']
            phone_fields = ['phone1', 'phone2']
            exactAtt = request_data['exact']
            fuzzyAtt = request_data['similar']
            Attributes = exactAtt + fuzzyAtt
            prod_pid = test
            identifier = 'id'
            if filter=='':
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], customer[identifier], prod_pid, customer, identifier, exactAtt, fuzzyAtt, Attributes, similar_exact,phone_fields,group1,group2,group3,0.75)
                        , axis = 1)
            else:
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], customer[customer[filter]==x[filter]][identifier], prod_pid, customer, identifier, exactAtt, fuzzyAtt, Attributes, similar_exact,phone_fields,group1,group2,group3,0.75)
                            , axis = 1)
            Dataframe = pd.DataFrame()
            for PID in Dict.keys():
                Prod = []
                for pid_2 in Dict[PID]:
                    prod_1 = prod_pid[prod_pid[identifier]==PID]
                    prod_1['matching_score'] = Similarity_Dict['{}:{}'.format(PID, pid_2)]['matching_score']
                    prod_1['matching_{}'.format(identifier)] = pid_2
                    prod_1['group1_matching'] = pd.io.json.dumps(Similarity_Dict['{}:{}'.format(PID, pid_2)]['group1_matching'])
                    prod_1['group2_matching'] = pd.io.json.dumps(Similarity_Dict['{}:{}'.format(PID, pid_2)]['group2_matching'])
                    prod_1['group3_matching'] = pd.io.json.dumps(Similarity_Dict['{}:{}'.format(PID, pid_2)]['group3_matching'])
                    prod_1['matching_attributes'] = pd.io.json.dumps(Similarity_Dict['{}:{}'.format(PID, pid_2)]['matching_attributes'])
                    Prod.append(prod_1)
                if Prod!=[]:
                    Dataframe = Dataframe.append( pd.concat(Prod) , ignore_index = True )
        elif test['data_type'].iloc[0]=='product':
            exactAtt = request_data['exact']
            fuzzyAtt = request_data['similar']
            similar_exact = ['parent_leaf_guid', 'mfg_brand_name', 'price', 'parts_accessories_type', 'product_length_in','manufacturer_warranty',
            'downrod_length_in', 'material','product_height_in','product_depth_in','housing_color_family', 'color_finish', 'product_weight_lb',
            'product_width_in','series_collection','product_diameter_in','california_title_20_compliant','shade_fitter_type','shade_color_family',
            'lighting_product_type','certifications_and_listings', 'fixture_color_finish_family', 'lamp_shade_material']
            phone_fields = []
            Attributes = exactAtt + fuzzyAtt
            prod_pid = test
            identifier = 'id'
            if filter=='':
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], product[identifier], prod_pid, product, identifier, exactAtt, fuzzyAtt, Attributes,similar_exact,phone_fields,group1,group2,group3,0.75)
                        , axis = 1)
            else:
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], product[product[filter]==x[filter]][identifier], prod_pid, product, identifier, exactAtt, fuzzyAtt, Attributes,similar_exact,phone_fields,group1,group2,group3,0.75)
                        , axis = 1)
            Dataframe = pd.DataFrame()
            for PID in Dict.keys():
                Prod = []
                for pid_2 in Dict[PID]:
                    prod_1 = prod_pid[prod_pid[identifier]==PID]
                    prod_1['matching_score'] = Similarity_Dict['{}:{}'.format(PID, pid_2)]['matching_score']
                    prod_1['matching_{}'.format(identifier)] = pid_2
                    prod_1['group1_matching'] = pd.io.json.dumps(Similarity_Dict['{}:{}'.format(PID, pid_2)]['group1_matching'])
                    prod_1['group2_matching'] = pd.io.json.dumps(Similarity_Dict['{}:{}'.format(PID, pid_2)]['group2_matching'])
                    prod_1['group3_matching'] = pd.io.json.dumps(Similarity_Dict['{}:{}'.format(PID, pid_2)]['group3_matching'])
                    prod_1['matching_attributes'] = pd.io.json.dumps(Similarity_Dict['{}:{}'.format(PID, pid_2)]['matching_attributes'])
                    Prod.append(prod_1)
                if Prod!=[]:
                    Dataframe = pd.concat( [Dataframe, Prod] , ignore_index = True )
        return jsonify(Dataframe.to_json(orient = 'records'))
    except Exception as e:
        dict = {
            'Error' : str(e),
            'check' : request_data
        }
        return jsonify(dict)

if __name__ == '__main__':
    app.run(host = '0.0.0.0',port='5000',debug=True)