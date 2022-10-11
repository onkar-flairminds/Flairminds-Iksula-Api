from operator import indexOf
from tkinter import scrolledtext
from typing import Dict
import numpy as np
import pandas as pd
import numpy as np
import datetime
import re
import os
from urllib.parse import ParseResultBytes,quote_plus
from dotenv import load_dotenv
try:
    from sqlalchemy import engine,create_engine
except:
    os.system('pip3 install sqlalchemy')

from flask import Flask, request, jsonify
env_file_Location = "connection.env"
if os.path.exists(env_file_Location):
    load_dotenv(env_file_Location)

# connection_url = engine.URL.create(
    # drivername="mysql+pymysql",
    # username="root",
    # password="Pass@123",
    # host="localhost",
    # port = 3306,
    # database="iksula",
# )
# cnx = create_engine(connection_url)

drivername="mysql+pymysql"
username=os.getenv("user")
password=os.getenv("password")
host=os.getenv("host")
port = os.getenv("port")
database=os.getenv("database")
customer_table = os.getenv("customer_table")
email_table = os.getenv("email_table")
phone_table = os.getenv("phone_table")
address_table = os.getenv("address_table")
product_table = os.getenv("product_table")
master_common_identifier = os.getenv("master_common_identifier")
test_identifier = os.getenv("test_identifier")
master_phone_columns = os.getenv("master_phone_columns")

url = r"postgresql://{}:{}@{}:{}/{}".format(username,quote_plus(str(password)),host,port,database)
print(url)
engine = create_engine(url, pool_size=50, echo=False)


try:
    query = "SELECT * FROM {};".format(customer_table)
    customer = pd.read_sql(query,engine)
    Cust = customer.copy(deep=True)
     #close the connection
except Exception as e:
    print('Error : '+str(e))
try:
    query = "SELECT * FROM {};".format(address_table)
    address = pd.read_sql(query,engine)
    #close the connection
except Exception as e:
    print('Error : '+str(e))
try:
    query = "SELECT * FROM {};".format(email_table)
    email = pd.read_sql(query,engine)
     #close the connection
except Exception as e:
    print('Error : '+str(e))
try:
    query = "SELECT * FROM {};".format(phone_table)
    phone = pd.read_sql(query,engine)
     #close the connection
except Exception as e:
    print('Error : '+str(e))
try:
    query = "Select * from {};".format(product_table)
    # product = pd.read_sql(query,engine)
     #close the connection
except Exception as e:
    print('Error : '+str(e))

def cleanEmailTail(string):
    return re.sub(r'@.*', '', str(string))

def createMatchingAttributesJson(att_list, current_val_list, row, mode='similar'):
    matching_attributes = {}
    for i in range(len(att_list)):
        att_dict = {}
        att_dict['attribute_name'] = str(att_list[i])
        att_dict['current_value'] = str(current_val_list[i])
        att_dict['found_value'] = str(row[att_list[i]])
        if mode=='similar':
            att_dict['score'] = round(row['score_{}'.format(str(att_list[i])).lower()],4)
        elif mode=='exact':
            if att_dict['current_value']==att_dict['found_value']:
                att_dict['score'] = 1.0
            else:
                att_dict['score'] = 0.0
        matching_attributes[str(att_list[i])] = [att_dict]
    return pd.io.json.dumps(matching_attributes)
def createGroupMatchingJson(att_group_info, group, group_similar, test_row, row, mode='similar'):
    group_matching = {}
    for type in group.keys():
        group_matching[type] = []
        for att in group[type]['data']:
            if mode=='similar':
                if group[type]['match-type']=='similar':
                    group_att = {}
                    group_att['group_name'] = att_group_info[att]['label']
                    group_att['field_name'] = str(att)
                    
                    score_list = []
                    if att_group_info[str(att)]['label']=='phone':
                        for master_att in master_phone_columns.split():
                            score = round(row['score_{}_{}_{}'.format(type.lower(),att.lower(),master_att.lower())],4)
                            score_list.append(score)
                            group_att['matching_fields'][master_att] = score
                    else:
                        for master_att in group[type]['data']:
                            score = round(row['score_{}_{}_{}'.format(type.lower(),att.lower(),master_att.lower())],4)
                            score_list.append(score)
                            group_att['matching_fields'][master_att] = score
                    group_att['group_score'] = max(score_list)
                    group_matching[type].append(group_att)
            else:
                if group[type]['match-type']=='exact':
                    group_att = {}
                    group_att['group_name'] = att_group_info[att]['label']
                    group_att['field_name'] = str(att)
                    group_att['matching_fields'] = {}
                    score_list = []
                    att_val_test = str(test_row[att])
                    if att_group_info[str(att)]['label']=='phone':
                        for master_att in master_phone_columns.split():
                            att_val_master = row[master_att]
                            if group[type]['process']=='email':
                                att_val_master = cleanEmailTail(row[master_att])
                                att_val_test = cleanEmailTail(test_row[att])
                            if att_val_test==att_val_master:
                                group_att['matching_fields'][master_att] = 1.0
                                score_list.append(1.0)
                                group_att['matching_value'] = row[master_att]
                            else:
                                group_att['matching_fields'][master_att] = 0.0
                                score_list.append(0.0)
                                group_att['matching_value'] = ""
                    else:
                        for master_att in group[type]['data']:
                            att_val_master = row[master_att]
                            if group[type]['process']=='email':
                                att_val_master = cleanEmailTail(row[master_att])
                                att_val_test = cleanEmailTail(test_row[att])
                            if att_val_test==att_val_master:
                                group_att['matching_fields'][master_att] = 1.0
                                score_list.append(1.0)
                                group_att['matching_value'] = row[master_att]
                            else:
                                group_att['matching_fields'][master_att] = 0.0
                                score_list.append(0.0)
                                group_att['matching_value'] = ""
                    group_att['group_score'] = max(score_list)
                    group_matching[type].append(group_att)
    return pd.io.json.dumps(group_matching)
def checkMatching(att_group_info, group, group_exact, group_similar,filter_col, filter_val, matching_id,exactAtt, exactValue, fuzzyAtt, fuzzyValue, customer_columns, address_columns, phone_columns, email_columns, row):
    url = r"postgresql://{}:{}@{}:{}/{}".format(username,quote_plus(password),host,port,database)
    exact_String = ''
    if len(exactAtt)!=0 or len(group_exact)!=0:
        for i in range(len(exactAtt)):
            if exactAtt[i] in customer_columns:
                if type(exactValue[i])==str:
                    if exactValue[i]!="":
                        exact_String += """customer."{}"='{}' or """.format(exactAtt[i], exactValue[i],exactAtt[i])
                else:
                    exact_String += """customer."{}"={} or """.format(exactAtt[i], exactValue[i],exactAtt[i])
            elif exactAtt[i] in address_columns:
                if type(exactValue[i])==str:
                    if exactValue[i]!="":
                        exact_String += """address."{}"='{}' or """.format(exactAtt[i], exactValue[i],exactAtt[i])
                else:
                    exact_String += """email."{}"={} or """.format(exactAtt[i], exactValue[i], exactAtt[i])
            elif exactAtt[i] in phone_columns:
                if type(exactValue[i])==str:
                    if exactValue[i]!="":
                        exact_String += """phone."{}"='{}' or """.format(exactAtt[i], exactValue[i],exactAtt[i])
                else:
                    exact_String += """phone."{}"={} or """.format(exactAtt[i], exactValue[i], exactAtt[i]) 
            elif exactAtt[i] in email_columns:
                if type(exactValue[i])==str:
                    if exactValue[i]!="":
                        exact_String += """email."{}"='{}' or """.format(exactAtt[i], exactValue[i],exactAtt[i])
                else:
                    exact_String += """email."{}"={} or """.format(exactAtt[i], exactValue[i], exactAtt[i])
        group_exact_String = ''
        for att in group_exact:
            if att_group_info[str(att)]['label']=='phone':
                for master_att in master_phone_columns.split():
                    if row[att]!="":
                        group_exact_String += """phone."{}"::text='{}' or """.format(master_att, row[att],att)
            else:
                for master_att in att_group_info[att]['data']:
                    if att in customer_columns:
                        if type(row[att])==str:
                            if row[master_att]!="":
                                group_exact_String += """customer."{}"::text='{}' or """.format(master_att, row[att],att)
                    elif att in address_columns:
                        if type(row[master_att])==str:
                            if row[att]!="":
                                group_exact_String += """address."{}"::text='{}' or """.format(master_att, row[att],att)
                    elif att in phone_columns:
                        if type(row[master_att])==str:
                            if row[att]!="":
                                group_exact_String += """phone."{}"::text='{}' or """.format(master_att, row[att],att)
                    elif att in email_columns:
                        if type(row[master_att])==str:
                            if row[att]!="":
                                if att_group_info[att]['process']=='email':
                                    group_exact_String += """REGEXP_REPLACE(email."{}"::text, '@.*', '')=REGEXP_REPLACE('{}', '@.*', '') or """.format(master_att, row[att],att)
                                else:
                                    group_exact_String += """email."{}"::text='{}' or """.format(master_att, row[att],att)
        exact_String = exact_String[:-4]
        if group_exact_String!='':
            group_exact_String = group_exact_String[:-4]
        filter_table = ''
        if filter_col in customer_columns:
            filter_table = "customer"
        elif filter_col in phone_columns:
            filter_table = "phone"
        elif filter_col in email_columns:
            filter_table = "email"
        elif filter_col in address_columns:
            filter_table = "address"
        if filter_col == '' or filter_table=='':
            if group_exact_String=='':
                exact_Query = f"""
                select *
                from {customer_table} as customer 
                left join {email_table} as email using ("{master_common_identifier}")
                left join {phone_table} as phone using ("{master_common_identifier}")
                left join {address_table} as address using ("{master_common_identifier}")
                where {exact_String};
                """
            else:
                exact_Query = f"""
                select *
                from {customer_table} as customer 
                left join {email_table} as email using ("{master_common_identifier}")
                left join {phone_table} as phone using ("{master_common_identifier}")
                left join {address_table} as address using ("{master_common_identifier}")
                where {exact_String} or {group_exact_String};
                """
        else:
            if group_exact_String=='':
                exact_Query = f"""
                select *
                from {customer_table} as customer 
                left join {email_table} as email using ("{master_common_identifier}")
                left join {phone_table} as phone using ("{master_common_identifier}")
                left join {address_table} as address using ("{master_common_identifier}")
                where ({exact_String}) and {filter_table}."{filter_col}" = '{filter_val}';
                """
            else:
                exact_Query = f"""
                select *
                from {customer_table} as customer 
                left join {email_table} as email using ("{master_common_identifier}")
                left join {phone_table} as phone using ("{master_common_identifier}")
                left join {address_table} as address using ("{master_common_identifier}")
                where ({exact_String} or {group_exact_String}) and {filter_table}."{filter_col}" = '{filter_val}';
                """
        if exact_String=='' and group_exact_String!='':
            if filter_col == '' or filter_table=='':
                exact_Query = f"""
                select *
                from {customer_table} as customer 
                left join {email_table} as email using ("{master_common_identifier}")
                left join {phone_table} as phone using ("{master_common_identifier}")
                left join {address_table} as address using ("{master_common_identifier}")
                where ({group_exact_String}) ;
                """
            else:
                exact_Query = f"""
                    select *
                    from {customer_table} as customer 
                    left join {email_table} as email using ("{master_common_identifier}")
                    left join {phone_table} as phone using ("{master_common_identifier}")
                    left join {address_table} as address using ("{master_common_identifier}")
                    where ({group_exact_String}) and {filter_table}."{filter_col}" = '{filter_val}';
                    """
        print(exact_Query)
        if exact_String!='' or group_exact_String!='':
            exact_match = pd.read_sql(exact_Query,engine)
            if not exact_match.empty:
                exact_match['matching_score'] = 1.0
                Try = exact_match.loc[exact_match.groupby(f'{master_common_identifier}').matching_score.idxmax()]
                exact_match = Try
                exact_match['matching_id'] = matching_id
                exact_match['matching_attributes'] = exact_match.apply(lambda x: createMatchingAttributesJson(exactAtt, exactValue, x, mode='exact'), axis = 1)
                exact_match['group_matching'] = exact_match.apply(lambda x: createGroupMatchingJson(att_group_info, group, group_similar, row, x, mode='exact'), axis = 1)
                return exact_match
    if len(fuzzyAtt)!=0 or len(group_similar)!=0:
        Similarity_string = ''
        Addition_String = ''
        filter_fuzzy_att = []
        filter_fuzzy_val = []
        identifier_dict = {}
        score_col_list = []
        for i in range(len(fuzzyAtt)):
            if fuzzyValue[i]!="":
                score_col_list.append('score_{}'.format(str(fuzzyAtt[i]).lower()))
                Addition_String+= 'score_{} + '.format(fuzzyAtt[i])
                filter_fuzzy_att.append(fuzzyAtt[i])
                filter_fuzzy_val.append(fuzzyValue[i])
                if fuzzyAtt[i] in customer_columns:
                    Similarity_string += """SIMILARITY(customer."{}"::text, '{}') AS score_{},""".format(fuzzyAtt[i], fuzzyValue[i],fuzzyAtt[i])
                elif fuzzyAtt[i] in address_columns:
                    Similarity_string += """SIMILARITY(address."{}"::text, '{}') AS score_{},""".format(fuzzyAtt[i], fuzzyValue[i],fuzzyAtt[i])
                elif fuzzyAtt[i] in phone_columns:
                    Similarity_string += """SIMILARITY(phone."{}"::text, '{}') AS score_{},""".format(fuzzyAtt[i], fuzzyValue[i],fuzzyAtt[i])
                elif fuzzyAtt[i] in email_columns:
                    Similarity_string += """SIMILARITY(email."{}"::text, '{}') AS score_{},""".format(fuzzyAtt[i], fuzzyValue[i],fuzzyAtt[i])
        group_similar_String = ''
        group_score_list = []
        for att in group_similar:
            if row[att]!="":
                if att_group_info[str(att)]['label']=='phone':
                    for master_att in master_phone_columns.split():
                        group_similar_String += """SIMILARITY(phone."{}"::text, '{}') AS score_{}_{}_{},""".format(master_att, row[att],att_group_info[att]['type'],att,master_att)
                else:
                    for master_att in att_group_info[att]['data']:
                        group_score_list.append('score_{}_{}_{}'.format(att_group_info[att]['type'].lower(),att.lower(),master_att.lower()))
                        if att in customer_columns:
                            group_similar_String += """SIMILARITY(customer."{}"::text, '{}') AS score_{}_{}_{},""".format(master_att, row[att], att_group_info[att]['type'],att,master_att)
                        elif att in address_columns:
                            group_similar_String += """SIMILARITY(address."{}"::text, '{}') AS score_{}_{}_{},""".format(master_att, row[att],att_group_info[att]['type'],att,master_att)
                        elif att in phone_columns:
                            group_similar_String += """SIMILARITY(phone."{}"::text, '{}') AS score_{}_{}_{},""".format(master_att, row[att],att_group_info[att]['type'],att,master_att)
                        elif att in email_columns:
                            if att_group_info[att]['process']=='email':
                                group_similar_String += """SIMILARITY(REGEXP_REPLACE(email."{}"::text, '@.*', ''),REGEXP_REPLACE('{}', '@.*', '')) AS score_{}_{}_{},""".format(master_att, row[att],att)
                            else:
                                group_similar_String += """SIMILARITY(email."{}"::text, '{}') AS score_{}_{}_{},""".format(master_att, row[att],fuzzyAtt[i])
        Similarity_string = Similarity_string[:-1]
        group_similar_String = group_similar_String[:-1]
        Addition_String  = Addition_String[:-3]
        Add_count = len(filter_fuzzy_att)
        if filter_col == '':
            if group_similar_String=='':
                similar_Query = f"""
                select *, (({Addition_String})/{Add_count}) as matching_score from (select * ,
                {Similarity_string}
                from {customer_table} as customer
                left join {email_table} as email using ("{master_common_identifier}")
                left join {phone_table} as phone using ("{master_common_identifier}")
                left join {address_table} as address using ("{master_common_identifier}")) as new_table
                order by matching_score desc limit 200;
                """
            else:
                similar_Query = f"""
                select *, (({Addition_String})/{Add_count}) as matching_score from (select * ,
                {Similarity_string}, {group_similar_String}
                from {customer_table} as customer
                left join {email_table} as email using ("{master_common_identifier}")
                left join {phone_table} as phone using ("{master_common_identifier}")
                left join {address_table} as address using ("{master_common_identifier}")) as new_table
                order by matching_score desc limit 200;
                """
        else:
            if group_similar_String=='':
                similar_Query = f"""
                select *, (({Addition_String})/{Add_count}) as matching_score from (select * ,
                {Similarity_string}
                from {customer_table} as customer
                left join {email_table} as email using ("{master_common_identifier}")
                left join {phone_table} as phone using ("{master_common_identifier}")
                left join {address_table} as address using ("{master_common_identifier}")) as new_table
                where "{filter_col}" = '{filter_val}'
                order by matching_score desc limit 200;
                """
            else:
                similar_Query = f"""
                select *, (({Addition_String})/{Add_count}) as matching_score from (select * ,
                {Similarity_string} , {group_similar_String}
                from {customer_table} as customer
                left join {email_table} as email using ("{master_common_identifier}")
                left join {phone_table} as phone using ("{master_common_identifier}")
                left join {address_table} as address using ("{master_common_identifier}")) as new_table
                where "{filter_col}" = '{filter_val}'
                order by matching_score desc limit 200;
                """
        if Similarity_string!='' or group_similar_String!='':
            print(similar_Query)
            similar_match = pd.read_sql(similar_Query,engine)
            if similar_match.empty:
                return pd.DataFrame()
            similar_match = similar_match.sort_values(by = 'matching_score', ascending = False)
            Try = similar_match.loc[similar_match.groupby('CustTreeNodeID').matching_score.idxmax()]
            Try = Try.sort_values(by = 'matching_score', ascending = False)
            final = Try.iloc[:3]
            final['matching_score'] = final.apply(lambda x : round(x['matching_score'], 4), axis=1)
            final['matching_id'] = matching_id
            final['matching_attributes'] = final.apply(lambda x: createMatchingAttributesJson(filter_fuzzy_att, filter_fuzzy_val, x), axis = 1)
            final['group_matching'] = final.apply(lambda x: createGroupMatchingJson(att_group_info, group, group_similar, row, x), axis = 1)
            final.drop(score_col_list, axis = 1, inplace = True)
            final.drop(group_score_list, axis = 1, inplace = True)
            return final
        else:
            return pd.DataFrame()
    else:
        return pd.DataFrame()

app = Flask(__name__)

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
        exactAtt = request_data['exact']
        fuzzyAtt = request_data['similar']
        exactAtt = [att for att in set.intersection(set(test.columns),set(exactAtt))]
        fuzzyAtt = [att for att in set.intersection(set(test.columns),set(fuzzyAtt))]
        group = request_data['action-group']
        group_att = []
        group_exact = []
        group_similar = []
        all_cols = list(set(list(customer.columns)+list(address.columns)+list(phone.columns)+list(email.columns)))
        if filter not in all_cols:
            filter = ''
        att_group_info = {}
        for type in group.keys():
            if group[type]['match-type']=='exact':
                group_exact += group[type]['data']
            elif group[type]['match-type']=='similar':
                group_similar += group[type]['data']
            for att in group[type]['data']:
                att_group_info[str(att)] = {}
                att_group_info[str(att)]['type'] = type
                att_group_info[str(att)]['label'] = group[type]['label']
                att_group_info[str(att)]['process'] = group[type]['process']
                att_group_info[str(att)]['match-type'] = group[type]['match-type']
                att_group_info[str(att)]['data'] = group[type]['data']
            group_att = group_att + group[type]['data']
        # group_exact = list(set.intersection(set(group_exact), set(all_cols)))
        # group_similar = list(set.intersection(set(group_similar), set(all_cols)))
        # group_att = list(set.intersection(set(group_att), set(all_cols)))

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
            Attributes = list(set(exactAtt + fuzzyAtt+ group_att))
            exactAtt = [att for att in set.intersection(set(all_cols),set(exactAtt))]
            fuzzyAtt = [att for att in set.intersection(set(all_cols),set(fuzzyAtt))]
            prod_pid = test
            
            if filter!='':
                prod_pid['df'] = prod_pid.apply(lambda x : checkMatching(att_group_info,group,group_exact,group_similar, filter, x[filter], x[test_identifier], exactAtt, list(x[exactAtt]), fuzzyAtt, list(x[fuzzyAtt]), list(customer.columns), list(address.columns), list(phone.columns), list(email.columns),x)
                            , axis = 1)
            else:
                prod_pid['df'] = prod_pid.apply(lambda x : checkMatching(att_group_info,group,group_exact,group_similar,filter, '', x[test_identifier], exactAtt, list(x[exactAtt]), fuzzyAtt, list(x[fuzzyAtt]), list(customer.columns), list(address.columns), list(phone.columns), list(email.columns),x)
                            , axis = 1)
        elif test['data_type'].iloc[0]=='product':
            similar_exact = ['parent_leaf_guid', 'mfg_brand_name', 'price', 'parts_accessories_type', 'product_length_in','manufacturer_warranty',
            'downrod_length_in', 'material','product_height_in','product_depth_in','housing_color_family', 'color_finish', 'product_weight_lb',
            'product_width_in','series_collection','product_diameter_in','california_title_20_compliant','shade_fitter_type','shade_color_family',
            'lighting_product_type','certifications_and_listings', 'fixture_color_finish_family', 'lamp_shade_material']
            # similar_exact = []
            phone_fields = []
            Attributes = exactAtt + fuzzyAtt
        Dataframe = pd.DataFrame()
        for i in range(len(prod_pid)):
            prod_row = prod_pid.iloc[i]
            df = prod_row['df']
            if df.empty:
                
                continue
            Dataframe = pd.concat([Dataframe,df.reset_index(drop=True)], ignore_index=True)
        Dataframe = Dataframe.loc[:,~Dataframe.columns.duplicated(keep='first')]
        return Dataframe.reset_index(drop=True).to_dict(orient = 'records')
    except Exception as e:
        dict = {
            'Error' : str(e),
            'check' : request_data
        }
        return jsonify(dict)

if __name__ == '__main__':
    app.run(host = '0.0.0.0',port='5000',debug=False)
    engine.dispose()