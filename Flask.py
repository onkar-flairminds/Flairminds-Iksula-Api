from operator import indexOf
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
    from sqlalchemy import engine,create_engine
    from difflib import SequenceMatcher
except:
    os.system('pip3 install python-Levenshtein')
    os.system('pip3 install Phonenumbers')
    os.system('pip3 install sqlalchemy')
    os.system('pip3 install difflib')
    import Levenshtein
    import phonenumbers
    from sqlalchemy import engine,create_engine
    from difflib import SequenceMatcher

from flask import Flask, request, jsonify

connection_url = engine.URL.create(
    drivername="mysql+pymysql",
    username="root",
    password="Pass@123",
    host="localhost",
    port = 3306,
    database="iksula",
)
cnx = create_engine(connection_url)
try:
    query = "SELECT * FROM search_data_customer;"
    customer = pd.read_sql(query,cnx)
    Cust = customer.copy(deep=True)
    # cnx.close() #close the connection
except Exception as e:
    # cnx.close()
    print('Error : '+str(e))
try:
    query = "SELECT * FROM search_data_address;"
    address = pd.read_sql(query,cnx)
    # cnx.close() #close the connection
except Exception as e:
    # cnx.close()
    print('Error : '+str(e))
try:
    query = "SELECT * FROM search_data_email;"
    email = pd.read_sql(query,cnx)
    # cnx.close() #close the connection
except Exception as e:
    # cnx.close()
    print('Error : '+str(e))
try:
    query = "SELECT * FROM search_data_phone;"
    phone = pd.read_sql(query,cnx)
    # cnx.close() #close the connection
except Exception as e:
    # cnx.close()
    print('Error : '+str(e))
try:
    query = "Select * from master_products_datas;"
    product = pd.read_sql(query,cnx)
    # cnx.close() #close the connection
except Exception as e:
    # cnx.close()
    print('Error : '+str(e))

def GroupAndMerge(Cust,df,agg_dict):
    combined_df = df.groupby('CustTreeNodeID').agg(agg_dict).reset_index()
    return Cust.merge(combined_df,how = 'left', on = 'CustTreeNodeID')

def MergeAllData(Cust, address, email, phone):
    agg_dict = {'AddressID':list,'Street':list,'Street2':list,'City':list,'StateCode':list,'PostalCode':list,
                'County':list,'GeoCode':list,'CountryCode':list}
    Cust = GroupAndMerge(Cust,address,agg_dict)
    agg_dict = {'PhoneID':list,'PhoneNumber':list}
    Cust = GroupAndMerge(Cust,phone,agg_dict)
    agg_dict = {'EmailID':list,'Email':list}
    Cust = GroupAndMerge(Cust,email,agg_dict)
    return Cust

def checkForFilterandMerge(customer, email, address, phone, filter, filter_value):
    if filter in customer.columns:
        customer_filtered = customer[customer[filter]==filter_value]
    else:
        customer_filtered = customer
    if filter in email.columns:
        email_filtered = email[email[filter]==filter_value]
    else:
        email_filtered = email
    if filter in address.columns:
        address_filtered = address[address[filter]==filter_value]
    else:
        address_filtered = address
    if filter in phone.columns:
        phone_filtered = phone[phone[filter]==filter_value]
    else:
        phone_filtered = phone
    merged = MergeAllData(customer_filtered, address_filtered, email_filtered, phone_filtered)
    return merged

try:
    Cust = MergeAllData(Cust, address, email, phone)
except Exception as e:
    print('Error on Merging : '+str(e))
app = Flask(__name__)

def exactSimilarMatch(string1, string2):
    string1 = string1.lower().strip()
    string2 = string2.lower().strip()
    if string1 == string2:
        return 1.0
    return 0.0
def isValidPhoneNumber(phoneString):
    phoneString = phoneString[-10:]
    Pattern = re.compile("[6-9][0-9]{9}")
    return Pattern.match(phoneString)

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

def SimilarityScore(string1,string2):
    stringArray1 = stringToWords(str(string1))
    stringArray2 = stringToWords(str(string2))
    jacc_score = JaccardSimilarity(stringArray1, stringArray2)
    fuzzy_score = ( fuzz.token_sort_ratio( str(string1).lower().strip(), str(string2).lower().strip()) )*0.01
    LCS_score = ( longestCommonSubstringScore( str(string1).lower(), str(string2).lower()) )
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
        prod_val2 = [str(value).lower().strip() for value in prod_2[att]]
        if str(prod_1[att])=='nan' or str(prod_1[att])=='' or prod_1[att]==None:
            continue
        index_list = []
        for i in range(len(prod_val2)):
            if str(prod_val2)=='' or str(prod_val2)=='nan' or prod_val2==None:
                continue
            index_list.append(i)
        if header in phone_fields:
            if process=='phone':
                check1 = isValidPhoneNumber(prod_val1)
                phone_index = []
                check2 = False
                for i in range(len(prod_val2)):
                    if isValidPhoneNumber(prod_val2[i]):
                        phone_index.append(i)
                        check2 = True
                if check1==False or check2==False:
                    att_group_dict['group_score'] = 0
                    group_array.append(att_group_dict)
                    return 0
            prod_val1 = cleanPhoneNumber(prod_val1)
            prod_val2 = [ cleanPhoneNumber(value) for value in prod_val2]
        if process=='email':
            prod_val1 = cleanEmailTail(prod_val1)
            prod_val2 = [ cleanEmailTail(value) for value in prod_val2]
        if match_type=="exact":
            match = np.intersect1d(np.array(prod_val1), np.array(prod_val2))
            # score = exactSimilarMatch(prod_val1, prod_val2)
            score = 0
            if len(match)!=0:
                for match_value in match:
                    index = prod_val2.index(match_value)
                    if index in index_list:
                        if header in phone_fields and match_value in phone_index:
                            score = 1
                        else:
                            score = 1
                att_group_dict['matching_fields'][header] = score
                score_list.append(score)
        elif match_type=="similar":
            stringArray1 = stringToWords(str(prod_1[att]))
            stringArray2 = stringToWords(str(prod_2[att]))
            score = JaccardSimilarity(stringArray1, stringArray2)
            att_group_dict['matching_fields'][header] = score
            if score>condition:
                score_list.append(score)
            fuzz_score = 0
            for index in range(len(prod_val2)):
                score = SimilarityScore(prod_val1,prod_val2[index])
                # score = JaccardSimilarity(str(prod_1[att]), str(prod_2[att]))
                if index in index_list and score > condition:
                    if score>=fuzz_score:
                        fuzz_score = score
            att_group_dict['matching_fields'][header] = score
            if fuzz_score > condition:
                score_list.append(fuzz_score)

    if len(score_list)!=0:
        group_score = max(score_list)
    else:
        group_score = 0
    att_group_dict['group_score'] = group_score
    group_array.append(att_group_dict)
    return group_score


def applyDictionaryLogic(pid, pid_2_list, prod_pid, prod_df, identifier, exactAtt, fuzzyAtt, Attributes, similar_exact, phone_fields,group,group_types, condition):
    pid_2_list = np.array(pid_2_list)
    Dict[pid] = []
    matching_Score_Dict = {}
    matching_Attributes_Dict = {}
    group_att_dict = {}
    for pid_2 in pid_2_list:
        prod_1 = prod_pid[prod_pid[identifier]==pid].iloc[0]
        prod_2 = prod_df[prod_df[identifier]==pid_2].iloc[0]
        exactAttScore = 0
        exactAttMatched = []
        fuzzyAttMatched = []
        matching_attributes = {}
        group_matching = {}
        group_score = {}
        for type in group_types:
            group_matching[type] = []
            group_score[type] = []

        for att in Attributes:
            att_dict = {}
            prod_val1 = str(prod_1[att]).lower().strip()
            if prod_2[att]==None or str(prod_2[att])=='nan' or prod_2[att]=='':
                continue
            prod_val2 = [str(value).lower().strip() for value in prod_2[att]]
            if str(prod_1[att])=='nan' or str(prod_1[att])=='' or prod_1[att]==None:
                continue
            index_list = []
            for i in range(len(prod_val2)):
                if str(prod_val2)=='' or str(prod_val2)=='nan' or prod_val2==None:
                    continue
                index_list.append(i)
            if att in exactAtt:
                if att in phone_fields:
                    prod_val1 = [cleanPhoneNumber(value) for value in prod_val1]
                    prod_val2 = [cleanPhoneNumber(value) for value in prod_val2]
                elif att=='model_number':
                    prod_val1 = [cleanModelNumber(value) for value in prod_val1]
                    prod_val2 = [cleanModelNumber(value) for value in prod_val2]
                match = np.intersect1d(np.array(prod_val1), np.array(prod_val2))
                att_dict = {}
                if len(match)!=0:
                    matching_attributes[str(att)] = []
                    for match_value in match:
                        index = prod_val2.index(match_value)
                        if index in index_list:
                            att_identifier = Identifier_Dict[att]
                            att_dict['current_value'] = match_value
                            att_dict['found_value'] = match_value
                            att_dict[att_identifier] = prod_2[att_identifier][index]
                            att_dict['score'] = 1.0
                            
                            matching_attributes[str(att)].append(att_dict)
                            exactAttScore = 1.0
                    if exactAttScore == 1.0:
                        exactAttMatched.append(str(att))
                        break
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
                    if att =='mfg_brand_name':
                        prod_val1 = [cleanMfgBrand(value) for value in prod_val1]
                        prod_val2 = [cleanMfgBrand(value) for value in prod_val2]
                    elif att =='manufacturer_warranty':
                        prod_val1 = [cleanWarranty(value) for value in prod_val1]
                        prod_val2 = [cleanWarranty(value) for value in prod_val2]
                    elif att=='zip':
                        prod_val1 = [cleanZip(value) for value in prod_val1]
                        prod_val2 = [cleanZip(value) for value in prod_val2]
            
                    match = np.intersect1d(np.array(prod_val1), np.array(prod_val2))
                    att_dict = {}
                    if len(match)!=0:
                        fuzz_score = 0
                        matching_attributes[str(att)] = []
                        for match_value in match:
                            index = prod_val2.index(match_value)
                            if index in index_list:
                                att_identifier = Identifier_Dict[att]
                                att_dict['current_value'] = match_value
                                att_dict['found_value'] = match_value
                                att_dict[att_identifier] = prod_2[att_identifier][index]
                                att_dict['score'] = 1.0
                                matching_attributes[str(att)].append(att_dict)
                                if score>=fuzz_score:
                                   fuzz_score = score
                        if fuzz_score==1.0:
                            fuzzyAttMatched.append(str(att))
                else:
                    # stringArray1 = stringToWords(str(prod_1[att]))
                    # stringArray2 = stringToWords(str(prod_2[att]))
                    if att == 'shade_shape':
                        stringArray1 = cleanShadeShape(prod_val1)
                        stringArray2 = [cleanShadeShape(str(value)) for value in prod_val2]
                    fuzz_score = 0
                    att_dict = {}
                    matching_attributes[str(att)] = []
                    for index in range(len(prod_val2)):
                        score = SimilarityScore(prod_val1,prod_val2[index])
                        # score = JaccardSimilarity(str(prod_1[att]), str(prod_2[att]))
                        if index in index_list and score > condition:
                            att_identifier = Identifier_Dict[att]
                            att_dict['current_value'] = prod_val1
                            att_dict['found_value'] = prod_val2[i]
                            att_dict[att_identifier] = prod_2[att_identifier][index]
                            att_dict['score'] = score
                            matching_attributes[str(att)].append(att_dict)
                            if score>=fuzz_score:
                                fuzz_score = score
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
        
        # if filter!='':
        #     Customer_Merged = checkForFilterandMerge(customer, email, address, phone, filter)
        # else:
        #     Customer_Merged = Cust
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
        global Identifier_Dict
        Identifier_Dict = {'Email' : 'EmailID', 'PhoneNumber':'PhoneID', 'Street':'AddressID', 'Street2':'AddressID', 'City':'AddressID',
                        'StateCode':'AddressID', 'PostalCode':'AddressID', 'County':'AddressID','GeoCode':'AddressID', 'CountryCode':'AddressID'}

        if test['data_type'].iloc[0]=='customer':
            similar_exact = ['zip','city','country','state']
            # similar_exact = []
            phone_fields = ['phone1', 'phone2', 'PhoneNumber']
            exactAtt = request_data['exact']
            fuzzyAtt = request_data['similar']
            Attributes = list(set(exactAtt + fuzzyAtt+ group_att))
            
            prod_pid = test
            master = Cust
            identifier = 'id'
            if filter=='':
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], master[identifier], prod_pid, master, identifier, exactAtt, fuzzyAtt, Attributes, similar_exact,phone_fields,group,group_types,0.3)
                        , axis = 1)
            else:
                prod_pid['df'] = prod_pid.apply(lambda x : checkForFilterandMerge(customer, email, address, phone, filter, x[filter])
                            , axis = 1)
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], x['df'][identifier], prod_pid, x['df'], identifier, exactAtt, fuzzyAtt, Attributes, similar_exact,phone_fields,group,group_types,0.3)
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
            master = product
            identifier = 'id'
            if filter=='':
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], master[identifier], prod_pid, master, identifier, exactAtt, fuzzyAtt, Attributes,similar_exact,phone_fields,group,group_types,0.3)
                        , axis = 1)
            else:
                prod_pid.apply(lambda x : applyDictionaryLogic(x[identifier], master[master[filter]==x[filter]][identifier], prod_pid, master, identifier, exactAtt, fuzzyAtt, Attributes,similar_exact,phone_fields,group,group_types,0.3)
                        , axis = 1)

        Dataframe = pd.DataFrame()
        print(Dict)
        for PID in Dict.keys():
            Prod = []
            for pid_2 in Dict[PID]:
                prod_1 = master[master[identifier]==int(pid_2)]
                prod_1['matching_score'] = Similarity_Dict['{}:{}'.format(PID, pid_2)]['matching_score']
                prod_1['matching_{}'.format(identifier)] = PID
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
    cnx.dispose()