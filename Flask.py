from operator import indexOf
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
def createMatchingAttributesJson(att_list, current_val_list, row, mode='similar'):
    matching_attributes = []
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
        matching_attributes.append(att_dict)
    return pd.io.json.dumps(matching_attributes)
def checkMatching(matching_id,exactAtt, exactValue, fuzzyAtt, fuzzyValue, customer_columns, address_columns, phone_columns, email_columns):
    url = r"postgresql://{}:{}@{}:{}/{}".format(username,quote_plus(password),host,port,database)
    engine = create_engine(url, pool_size=50, echo=False)
    exact_String = ''
    if len(exactAtt)!=0:
        for i in range(len(exactAtt)):
            if exactAtt[i] in customer_columns:
                if type(exactAtt[i])==str:
                    exact_String += """customer."{}"='{}' or """.format(exactAtt[i], exactValue[i],exactAtt[i])
                else:
                    exact_String += """customer."{}"={} or """.format(exactAtt[i], exactValue[i],exactAtt[i])
            elif exactAtt[i] in address_columns:
                if type(exactAtt[i])==str:
                    exact_String += """address."{}"='{}' or """.format(exactAtt[i], exactValue[i],exactAtt[i])
                else:
                    exact_String += """email."{}"={} or """.format(exactAtt[i], exactValue[i], exactAtt[i])
            elif exactAtt[i] in phone_columns:
                if type(exactAtt[i])==str:
                    exact_String += """phone."{}"='{}' or """.format(exactAtt[i], exactValue[i],exactAtt[i])
                else:
                    exact_String += """phone."{}"={} or """.format(exactAtt[i], exactValue[i], exactAtt[i]) 
            elif exactAtt[i] in email_columns:
                if type(exactAtt[i])==str:
                    exact_String += """email."{}"='{}' or """.format(exactAtt[i], exactValue[i],exactAtt[i])
                else:
                    exact_String += """email."{}"={} or """.format(exactAtt[i], exactValue[i], exactAtt[i])
        exact_String = exact_String[:-4]
        exact_Query = f"""
        select *
        from {customer_table} as customer 
        inner join {email_table} as email using ("{master_common_identifier}")
        inner join {phone_table} as phone using ("{master_common_identifier}")
        inner join {address_table} as address using ("{master_common_identifier}")
        where {exact_String};
        """
        exact_match = pd.read_sql(exact_Query,engine)
        if not exact_match.empty:
            exact_match['matching_score'] = 1.0
            exact_match['matching_id'] = matching_id
            exact_match['matching_attributes'] = exact_match.apply(lambda x: createMatchingAttributesJson(exactAtt, exactValue, x, mode='exact'), axis = 1)
            return exact_match
    if len(fuzzyAtt)!=0:
        Similarity_string = ''
        Addition_String = ''
        Add_count = len(fuzzyAtt)
        identifier_dict = {}
        score_col_list = []
        for i in range(len(fuzzyAtt)):
            score_col_list.append('score_{}'.format(str(fuzzyAtt[i]).lower()))
            Addition_String+= 'score_{} + '.format(fuzzyAtt[i])
            if fuzzyAtt[i] in customer_columns:
                if type(fuzzyValue[i])==str:
                    Similarity_string += """SIMILARITY(customer."{}", '{}') AS score_{},""".format(fuzzyAtt[i], fuzzyValue[i],fuzzyAtt[i])
                else:
                    Similarity_string += """SIMILARITY(customer."{}", {}) AS score_{},""".format(fuzzyAtt[i], fuzzyValue[i],fuzzyAtt[i])
            elif fuzzyAtt[i] in address_columns:
                if type(fuzzyValue[i])==str:
                    Similarity_string += """SIMILARITY(address."{}", '{}') AS score_{},""".format(fuzzyAtt[i], fuzzyValue[i],fuzzyAtt[i])
                else:
                    Similarity_string += """SIMILARITY(email."{}", {}) AS score_{},""".format(fuzzyAtt[i], fuzzyValue[i], fuzzyAtt[i])
            elif fuzzyAtt[i] in phone_columns:
                if type(fuzzyValue[i])==str:
                    Similarity_string += """SIMILARITY(phone."{}", '{}') AS score_{},""".format(fuzzyAtt[i], fuzzyValue[i],fuzzyAtt[i])
                else:
                    Similarity_string += """SIMILARITY(phone."{}", {}) AS score_{},""".format(fuzzyAtt[i], fuzzyValue[i], fuzzyAtt[i]) 
            elif fuzzyAtt[i] in email_columns:
                if type(fuzzyValue[i])==str:
                    Similarity_string += """SIMILARITY(email."{}", '{}') AS score_{},""".format(fuzzyAtt[i], fuzzyValue[i],fuzzyAtt[i])
                else:
                    Similarity_string += """SIMILARITY(email."{}", {}) AS score_{},""".format(fuzzyAtt[i], fuzzyValue[i], fuzzyAtt[i])
        Similarity_string = Similarity_string[:-1]
        Addition_String  = Addition_String[:-3]
        similar_Query = f"""
        select *, (({Addition_String})/{Add_count}) as matching_score from (select * ,
        {Similarity_string}
        from {customer_table} as customer
        inner join {email_table} as email using ("{master_common_identifier}")
        inner join {phone_table} as phone using ("{master_common_identifier}")
        inner join {address_table} as address using ("{master_common_identifier}")) as new_table
        order by matching_score desc limit 100;
        """
        similar_match = pd.read_sql(similar_Query,engine)
        similar_match = similar_match.sort_values(by = 'matching_score', ascending = False)
        Try = similar_match.loc[similar_match.groupby('CustTreeNodeID').matching_score.idxmax()]
        Try = Try.sort_values(by = 'matching_score', ascending = False)
        final = Try.iloc[:3]
        final['matching_id'] = matching_id
        final['matching_score'] = final.apply(lambda x : round(x['matching_score'], 4), axis=1)
        final['matching_attributes'] = final.apply(lambda x: createMatchingAttributesJson(fuzzyAtt, fuzzyValue, x), axis = 1)
        final.drop(score_col_list, axis = 1, inplace = True)
        return final
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
            Attributes = list(set(exactAtt + fuzzyAtt+ group_att))
            all_cols = list(set(list(customer.columns)+list(address.columns)+list(phone.columns)+list(email.columns)))
            exactAtt = [att for att in set.intersection(set(all_cols),set(exactAtt))]
            fuzzyAtt = [att for att in set.intersection(set(all_cols),set(fuzzyAtt))]
            prod_pid = test
            
            prod_pid['df'] = prod_pid.apply(lambda x : checkMatching(x[test_identifier], exactAtt, list(x[exactAtt]), fuzzyAtt, list(x[fuzzyAtt]), list(customer.columns), list(address.columns), list(phone.columns), list(email.columns))
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
            Dataframe = pd.concat([Dataframe,df.reset_index(drop=True)])
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
    # cnx.dispose()