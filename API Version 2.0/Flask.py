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
    import phonenumbers
    from difflib import SequenceMatcher
    import jellyfish
except:
    os.system('pip3 install python-Levenshtein')
    os.system('pip3 install Phonenumbers')
    os.system('pip3 install difflib')
    os.system('pip3 install jellyfish')
    from difflib import SequenceMatcher
    import Levenshtein
    import phonenumbers
    import jellyfish

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
def isValidPhoneNumber(phoneString, stateCode):
    try:
        if stateCode=='' or stateCode==None:
            number = phonenumbers.parse(phoneString)
            return phonenumbers.is_valid_number(number)
        number = phonenumbers.parse(phoneString,stateCode)
        return phonenumbers.is_valid_number(number)
    except:
        return False
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
def cleanZip(string):
    return string.split('.')[0]

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

def longestCommonSubstringScore(string1,string2): 
    seqMatch = SequenceMatcher(None,string1,string2) 
    match = seqMatch.find_longest_match(0, len(string1), 0, len(string2))
    if (match.size!=0):
        if len(string1)>=len(string2):
            score = match.size/len(string1) 
        elif len(string2)>=len(string1):
            score = match.size/len(string2)
        return score
    else:
        return 0

def SimilarityScore(prod_1,prod_2,att):
    stringArray1 = stringToWords(str(prod_1[att]))
    stringArray2 = stringToWords(str(prod_2[att]))
    if att == 'shade_shape':
        stringArray1 = cleanShadeShape(str(prod_1[att]))
        stringArray2 = cleanShadeShape(str(prod_2[att]))
    str1 = jellyfish.nysiis(str(prod_1[att]))
    str2 = jellyfish.nysiis(str(prod_2[att]))
    # soundex_score = ( fuzz.token_sort_ratio( str1, str2) )*0.01
    jacc_score = JaccardSimilarity(stringArray1, stringArray2)
    fuzzy_score = ( fuzz.token_sort_ratio( str(prod_1[att]).lower(), str(prod_2[att]).lower()  ) )*0.01
    LCS_score = ( longestCommonSubstringScore( str(prod_1[att]).lower(), str(prod_2[att]).lower()) )
    # if jacc_score==0:
    #     score = (10*LCS_score + 5*soundex_score + 15*fuzzy_score)/30
    # else:
    #     score = (10*LCS_score + 5*soundex_score+ 6*jacc_score + 15*fuzzy_score)/36
    if jacc_score==0:
        score = (10*LCS_score + 18*fuzzy_score)/28
    else:
        score = (10*LCS_score + 18*jacc_score + 6*fuzzy_score)/34
    return round(score,4)

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
            stateCode1 = str(prod_1['state']).strip()
            stateCode2 = str(prod_2['state']).strip()
            if process=='phone':
                check1 = isValidPhoneNumber(prod_val1, stateCode1)
                check2 = isValidPhoneNumber(prod_val2, stateCode2)
                if check1==False or check2==False:
                    att_group_dict['group_score'] = 0
                    group_array.append(att_group_dict)
                    return 0
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


def applyDictionaryLogic(pid, pid_2_list, prod_pid, prod_df, identifier, exactAtt, fuzzyAtt, Attributes, similar_exact, phone_fields,group,group_types, condition):
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
    group_att_dict = {}
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
        group_matching = {}
        group_score = {}
        for type in group_types:
            group_matching[type] = []
            group_score[type] = []

        for att in Attributes:

            att_dict = {}
            if str(prod_1[att])=='nan' or prod_2[att]=='nan':
                continue
            if str(prod_1[att])=='' or prod_2[att]=='':
                continue
            if str(prod_1[att])==None or prod_2[att]==None:
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
            for type in group_types:
                if att in group[type]['data']:
                    score = groupmatching(att, group[type]['data'], group[type]["label"], group[type]["match-type"], group[type]["process"], prod_1, prod_2,phone_fields, condition, group_matching[type])
                    if group[type]["match-type"]=='exact':
                        if score == 1.0:
                            exactAttScore = 1.0
                            group_score[type].append(score)
                    else:
                        group_score[type].append(score)
            
            if att in fuzzyAtt:
                if att in similar_exact:
                    prod_val1 = str(prod_1[att]).strip()
                    prod_val2 = str(prod_2[att]).strip()

                    if att =='mfg_brand_name':
                       prod_val1 = cleanMfgBrand(prod_val1)
                       prod_val2 = cleanMfgBrand(prod_val2)
                    elif att =='manufacturer_warranty':
                       prod_val1 = cleanWarranty(prod_val1)
                       prod_val2 = cleanWarranty(prod_val2)
                    elif att=='zip':
                        prod_val1 = cleanZip(prod_val1)
                        prod_val2 = cleanZip(prod_val2)
                    score = exactSimilarMatch(str(prod_val1), str(prod_val2))
                    att_dict['attributes_name'] = str(att)
                    att_dict['current_value'] = str(prod_1[att])
                    att_dict['found_value'] = str(prod_2[att])
                    att_dict['score'] = score
                    matching_attributes.append(att_dict)
                    if score == 1.0:
                        fuzzyAttMatched.append(str(att))
                else:
                    # stringArray1 = stringToWords(str(prod_1[att]))
                    # stringArray2 = stringToWords(str(prod_2[att]))
                    # if att == 'shade_shape':
                    #     stringArray1 = cleanShadeShape(str(prod_1[att]))
                    #     stringArray2 = cleanShadeShape(str(prod_2[att]))
                    # score = JaccardSimilarity(stringArray1, stringArray2)
                    score = SimilarityScore(prod_1,prod_2,att)
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
            Matching_Score = round(fuzzyScore , 4)
        # if Matching_Score > 0.70:
        matching_Score_Dict['{}'.format(pid_2)] = Matching_Score
        matching_Attributes_Dict['{}'.format(pid_2)] = matching_attributes
        group_att_dict['{}'.format(pid_2)] = group_matching

    pid_2_keys = sorted(matching_Score_Dict, key=matching_Score_Dict.get, reverse=True)[:3]

    for pid_2 in pid_2_keys:
        matching_score = matching_Score_Dict['{}'.format(pid_2)]
        if matching_score!=0:
            Dict[pid].append(pid_2)
            Similarity_Dict['{}:{}'.format(pid, pid_2)] = {}
            Similarity_Dict['{}:{}'.format(pid, pid_2)]['matching_attributes'] = matching_Attributes_Dict['{}'.format(pid_2)]
            Similarity_Dict['{}:{}'.format(pid, pid_2)]['matching_score'] = matching_score
            Similarity_Dict['{}:{}'.format(pid, pid_2)]['group_matching'] = group_att_dict['{}'.format(pid_2)]

@app.route('/get_results', methods = ['POST'])
def Run():
    request_data = pd.io.json.loads(request.data)
    try:
        # request_data = eval(pd.io.json.loads(request.data))
        # data_df = pd.read_json(request_data)
        request_data = request.json
        test = pd.io.json.json_normalize(request_data['data'])
        filter = request_data['filter']
        group = request_data['action-group']
        group_att = []
        for type in group.keys():
            group_att = group_att + group[type]['data']
        group_att = list(set.intersection(set(group_att), set(list(test.columns))))
        group_types = list(request_data['action-group'].keys())
        global Dict
        Dict = {}
        global Similarity_Dict
        Similarity_Dict = {}

        if test['data_type'].iloc[0]=='customer':
            similar_exact = ['zip','city','country','state']
            # similar_exact = []
            phone_fields = ['phone1', 'phone2']
            exactAtt = request_data['exact']
            fuzzyAtt = request_data['similar']
            Attributes = list(set(exactAtt + fuzzyAtt+ group_att))
            prod_pid = test
            identifier = 'id'
            if filter=='':
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], customer[identifier], prod_pid, customer, identifier, exactAtt, fuzzyAtt, Attributes, similar_exact,phone_fields,group,group_types,0.3)
                        , axis = 1)
            else:
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], customer[customer[filter]==x[filter]][identifier], prod_pid, customer, identifier, exactAtt, fuzzyAtt, Attributes, similar_exact,phone_fields,group,group_types,0.3)
                            , axis = 1)
        elif test['data_type'].iloc[0]=='product':
            exactAtt = request_data['exact']
            fuzzyAtt = request_data['similar']
            similar_exact = ['parent_leaf_guid', 'mfg_brand_name', 'price', 'parts_accessories_type', 'product_length_in','manufacturer_warranty',
            'downrod_length_in', 'material','product_height_in','product_depth_in','housing_color_family', 'color_finish', 'product_weight_lb',
            'product_width_in','series_collection','product_diameter_in','california_title_20_compliant','shade_fitter_type','shade_color_family',
            'lighting_product_type','certifications_and_listings', 'fixture_color_finish_family', 'lamp_shade_material']
            # similar_exact = []
            phone_fields = []
            Attributes = exactAtt + fuzzyAtt
            prod_pid = test
            identifier = 'id'
            if filter=='':
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], product[identifier], prod_pid, product, identifier, exactAtt, fuzzyAtt, Attributes,similar_exact,phone_fields,group,group_types,0.3)
                        , axis = 1)
            else:
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], product[product[filter]==x[filter]][identifier], prod_pid, product, identifier, exactAtt, fuzzyAtt, Attributes,similar_exact,phone_fields,group,group_types,0.3)
                        , axis = 1)

        Dataframe = pd.DataFrame()
        for PID in Dict.keys():
            Prod = []
            for pid_2 in Dict[PID]:
                prod_1 = prod_pid[prod_pid[identifier]==PID]
                prod_1['matching_score'] = Similarity_Dict['{}:{}'.format(PID, pid_2)]['matching_score']
                prod_1['matching_{}'.format(identifier)] = pid_2
                prod_1['group_matching'] = pd.io.json.dumps(Similarity_Dict['{}:{}'.format(PID, pid_2)]['group_matching'])
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
    app.run(host = '0.0.0.0',port='5000',debug=False)