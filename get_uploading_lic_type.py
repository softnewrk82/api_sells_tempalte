


import requests
import json

import cryptography

import pandas as pd
import numpy as np

import xmltodict

import re
import requests

import warnings
warnings.simplefilter("ignore")

from functools import lru_cache

import importlib

import modules.api_info
importlib.reload(modules.api_info)

from datetime import datetime

from sqlalchemy import create_engine

from modules.api_info import var_encrypt_var_app_client_id
from modules.api_info import var_encrypt_var_app_secret
from modules.api_info import var_encrypt_var_secret_key

from modules.api_info import var_encrypt_url_sbis
from modules.api_info import var_encrypt_url_sbis_unloading

from modules.api_info import var_encrypt_var_db_user_name
from modules.api_info import var_encrypt_var_db_user_pass

from modules.api_info import var_encrypt_var_db_host
from modules.api_info import var_encrypt_var_db_port

from modules.api_info import var_encrypt_var_db_name
from modules.api_info import var_encrypt_var_db_name_for_upl
from modules.api_info import var_encrypt_var_db_schema
from modules.api_info import var_encryptvar_API_sbis
from modules.api_info import var_encrypt_API_sbis_pass

from modules.api_info import f_decrypt, load_key_external


var_app_client_id = f_decrypt(var_encrypt_var_app_client_id, load_key_external()).decode("utf-8")
var_app_secret = f_decrypt(var_encrypt_var_app_secret, load_key_external()).decode("utf-8")
var_secret_key = f_decrypt(var_encrypt_var_secret_key, load_key_external()).decode("utf-8")

url_sbis = f_decrypt(var_encrypt_url_sbis, load_key_external()).decode("utf-8")
url_sbis_unloading = f_decrypt(var_encrypt_url_sbis_unloading, load_key_external()).decode("utf-8")

var_db_user_name = f_decrypt(var_encrypt_var_db_user_name, load_key_external()).decode("utf-8")
var_db_user_pass = f_decrypt(var_encrypt_var_db_user_pass, load_key_external()).decode("utf-8")

var_db_host = f_decrypt(var_encrypt_var_db_host, load_key_external()).decode("utf-8")
var_db_port = f_decrypt(var_encrypt_var_db_port, load_key_external()).decode("utf-8")

var_db_name = f_decrypt(var_encrypt_var_db_name, load_key_external()).decode("utf-8")

var_db_name_for_upl = f_decrypt(var_encrypt_var_db_name_for_upl, load_key_external()).decode("utf-8")


var_db_schema = f_decrypt(var_encrypt_var_db_schema, load_key_external()).decode("utf-8")

API_sbis = f_decrypt(var_encryptvar_API_sbis, load_key_external()).decode("utf-8")
API_sbis_pass = f_decrypt(var_encrypt_API_sbis_pass, load_key_external()).decode("utf-8")



from modules.api_info import var_encrypt_TOKEN_yandex_users, f_decrypt, load_key_external
from modules.api_info import var_encrypt_var_login_da, var_encrypt_var_pass_da

var_TOKEN = f_decrypt(var_encrypt_TOKEN_yandex_users, load_key_external()).decode("utf-8")
login_da = f_decrypt(var_encrypt_var_login_da, load_key_external()).decode("utf-8")
pass_da = f_decrypt(var_encrypt_var_pass_da, load_key_external()).decode("utf-8")




def send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn):
        
        var_login_da = str(login_da)
        var_pass_da = str(pass_da)


        myuuid_sbis_down = "b57d09dc-a631-4dbc-9e01-f706920fcb29"

        url = "https://online.sbis.ru/auth/service/" 


        method = "СБИС.Аутентифицировать"

        params = {
            "Параметр": {
                "Логин": f"{var_login_da}",
                "Пароль": f"{var_pass_da}",
            }

        }
        parameters = {
        "jsonrpc": "2.0",
        "method": method,
        "params":params,
        "id": 0
        }
            
        response = requests.post(url, json=parameters)
        response.encoding = 'utf-8'


        str_to_dict = json.loads(response.text)
        access_token = str_to_dict["result"]

        headers = {
        "X-SBISSessionID": access_token,
        "Content-Type": "application/json",
        }  



        parameters_real = {

        "jsonrpc": "2.0",
        "protocol": 6,
        "method": "PublicMsgApi.MessageSend",
        "params": {
            "dialogID": myuuid_sbis_down,
            "messageID": None,
            "answer": None,
            "text": f"{datetime.now().date()} {var_link}, {var_doc_number}, {var_doc_data_main}, {var_doc_type}, {var_doc_counterparty_inn}",
            "document": "bc70081f-3ccf-4507-b753-0e185191bf8c",
            "files": {
                "fileId": "bc70081f-3ccf-4507-b753-0e185191bf8c",
                "isLink": "true",
            },
            "recipients": [
            "c943a420-f494-4a38-8975-d9db61c3dba7",
            ],
            "options": {
                "d": [
                    "СБИС.API (errors)",
                    0,
                    {}
                ],
                "s": [
                    {
                    "t": "Строка",
                    "n": "Title"
                    },
                    {
                    "t": "Число целое",
                    "n": "TextFormat"
                    },
                    {
                    "t": "JSON-объект",
                    "n": "ServiceObject"
                    }
                ],
                "_type": "record"
                }
            },
            "id": 1
            }



        url_real = "https://online.sbis.ru/msg/service/"

        response_points = requests.post(url_real, json=parameters_real, headers=headers)




def sbis_real_processing_0(date_from, date_to, name_unloading, name_unloading_exc):
    var_now = datetime.now()
    
    
    
    
    
    date_from = date_from
    date_to = date_to
    
    
    print("date_from:", date_from)
    print("date_to:", date_to)
    
    
    
    def doc_append():
        doc_id.append(var_link)
        doc_type.append(var_doc_type)
        doc_number.append(var_doc_number)
        doc_full_name.append(var_doc_full_name)
        doc_data_main.append(var_doc_data_main)
        doc_at_created.append(var_doc_at_created)
        doc_counterparty_inn.append(var_doc_counterparty_inn)
        doc_counterparty_kpp.append(var_doc_counterparty_kpp)
        doc_counterparty_full_name.append(var_doc_counterparty_full_name)
        doc_provider_inn.append(var_doc_provider_inn)
        doc_provider_kpp.append(var_doc_provider_kpp)
        doc_provider_full_name.append(var_doc_provider_full_name)
    
        doc_assigned_manager.append(var_doc_assigned_manager)
        doc_department.append(var_doc_department)

        base_doc_conn_type.append(var_base_doc_conn_type)
        base_doc_date.append(var_base_doc_date)
        base_doc_id.append(var_base_doc_id)
        base_doc_dir.append(var_base_doc_dir)
        base_doc_num.append(var_base_doc_num)
        base_doc_reglament_id.append(var_base_doc_reglament_id)
        base_doc_name.append(var_base_doc_name)
        base_doc_type.append(var_base_doc_type)
        base_doc_sum.append(var_base_doc_sum)

        consequence_doc_conn_type.append(var_consequence_doc_conn_type)
        consequence_doc_date.append(var_consequence_doc_date)
        consequence_doc_id.append(var_consequence_doc_id)
        consequence_doc_dir.append(var_consequence_doc_dir)
        consequence_doc_num.append(var_consequence_doc_num)
        consequence_doc_reglament_id.append(var_consequence_doc_reglament_id)
        consequence_doc_name.append(var_consequence_doc_name)
        consequence_doc_type.append(var_consequence_doc_type)
        consequence_doc_sum.append(var_consequence_doc_sum)
    
    def doc_append_exc():
        doc_id_exc.append(var_link)
        doc_type_exc.append(var_doc_type)
        doc_number_exc.append(var_doc_number)
        doc_full_name_exc.append(var_doc_full_name)
        doc_data_main_exc.append(var_doc_data_main)
        doc_at_created_exc.append(var_doc_at_created)
        doc_counterparty_inn_exc.append(var_doc_counterparty_inn)
        doc_counterparty_kpp_exc.append(var_doc_counterparty_kpp)
        doc_counterparty_full_name_exc.append(var_doc_counterparty_full_name)
        doc_provider_inn_exc.append(var_doc_provider_inn)
        doc_provider_kpp_exc.append(var_doc_provider_kpp)
        doc_provider_full_name_exc.append(var_doc_provider_full_name)
    
        doc_assigned_manager_exc.append(var_doc_assigned_manager)
        doc_department_exc.append(var_doc_department)
    
    def inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_sn,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price,
                        var_agent,
                        var_date_from,
                        var_license_type,
                        var_warehouse,
                        ):
        
        inside_doc_author.append(var_inside_doc_author)
        inside_doc_type.append(var_inside_doc_type)
        inside_doc_item_full_doc_price.append(var_inside_doc_item_full_doc_price)
        
        inside_doc_item_note.append(var_inside_doc_item_note)
        
        inside_doc_item_code.append(var_inside_doc_item_code)
        inside_doc_item_article.append(var_inside_doc_item_article)
        inside_doc_item_sn.append(var_inside_doc_item_sn)
        inside_doc_item_name.append(var_inside_doc_item_name)
        
        inside_doc_item_quantity.append(var_inside_doc_item_quantity)
        inside_doc_item_unit.append(var_inside_doc_item_unit)
        
        inside_doc_item_price.append(var_inside_doc_item_price)
        inside_doc_item_full_item_price.append(var_inside_doc_item_full_item_price)
        inside_agent.append(var_agent)
        inside_date_from.append(var_date_from)
        
        inside_license_type.append(var_license_type)
        
        inside_warehouse.append(var_warehouse)
    
    
    
    def def_act_vr(xml_a, var_inside_doc_author, var_inside_doc_type):
        
        def def_act_vr_print(var_inside_doc_author, var_inside_doc_type):
            
            if type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]) == dict:
                print(var_inside_doc_type)
                print("dict")
                
                var_quantity = 1 
                
                for x in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["ИнфПолеОписРабот"]:
                    if x["@Идентиф"] == "ПоляНоменклатуры":
                        try:
                            print("var_inside_doc_item_article", re.findall(f"\d+\-\d+", x["@Значен"])[0])
                            print("var_inside_doc_item_quantity", re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', ''))
                        except:
                            try:
                                print("var_inside_doc_item_article", re.findall(f"услуга|услуги", x["@Значен"].lower())[0])
                                print("var_inside_doc_item_quantity", re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', ''))
                            except:
                                print("var_inside_doc_item_article", np.nan)
                    elif x["@Идентиф"] == "Примечание":
                        print("var_inside_doc_item_note", x["@Значен"])            
                    elif x["@Идентиф"] == "НазваниеПоставщика":
                        print("var_inside_doc_item_name", x["@Значен"])            
                    elif x["@Идентиф"] == "КодПоставщика":
                        print("var_inside_doc_item_code", x["@Значен"])    
                                                    
                try:
                    print("var_inside_doc_item_quantity", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Количество"])
                except: 
                    pass
                print("var_inside_doc_item_unit", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@НаимЕдИзм"])
                try:
                    print("var_inside_doc_item_name", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@НаимРабот"])
                except:
                    pass
                
                try:
                    print("var_inside_doc_item_price", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Цена"])
                except:
                    print("var_inside_doc_item_price", np.nan) 
                
                print("var_inside_doc_item_full_doc_price", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"])     
                print("_____________________")
    
    
            elif type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]) == list:
                    
                    sum_inside_doc = 0
                    
                    for k in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]:
                        sum_inside_doc += float(k["@СтоимУчНДС"])
            
                    for k in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]:
            
                        print(var_inside_doc_type)
                        print("list")
                        
                        var_quantity = 1
                        
                        for x in k["ИнфПолеОписРабот"]:
                            if x["@Идентиф"] == "ПоляНоменклатуры":
                                try:
                                    print("var_inside_doc_item_article", re.findall(f"\d+\-\d+", x["@Значен"])[0])
                                    print("var_inside_doc_item_quantity", re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', ''))
                                    var_quantity = re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', '')  
                                except:
                                    try:
                                        print("var_inside_doc_item_article", re.findall(f"услуга|услуги", x["@Значен"].lower())[0])
                                        print("var_inside_doc_item_quantity", re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', ''))
                                        var_quantity = re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', '')
                                    except:
                                        print("@Артикул", "отсутствует")
                            elif x["@Идентиф"] == "Примечание":
                                print("var_inside_doc_item_note", x["@Значен"])            
                            elif x["@Идентиф"] == "НазваниеПоставщика":
                                print("var_inside_doc_item_name", x["@Значен"])            
                            elif x["@Идентиф"] == "КодПоставщика":
                                print("var_inside_doc_item_code", x["@Значен"])
                            
                            elif x["@Идентиф"] == "Примечание":
                                var_inside_doc_item_note = x["@Значен"] 
            
                            elif x["@Идентиф"] == "НазваниеПоставщика":
                                var_inside_doc_item_name = x["@Значен"]
            
                            elif x["@Идентиф"] == "КодПоставщика":  
                                var_inside_doc_item_code =  x["@Значен"]
            
                        try:
                            print("var_inside_doc_item_quantity", k["@Количество"])
                        except:
                            pass
                        try:
                            print("var_inside_doc_item_name", k["@НаимРабот"])
                        except:
                            pass
    
                        try:
                            print("var_inside_doc_item_price", k["@Цена"])
                        except:
                            print("var_inside_doc_item_price", np.nan)
                            
                        print("var_inside_doc_item_full_doc_price", k["@СтоимУчНДС"])     
                        print("_____________________")
    
                    
        def def_act_vr_set_variable(var_inside_doc_author, var_inside_doc_type):
            if type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]) == dict:
                
                var_inside_doc_item_quantity = 1  
        
                var_inside_doc_author = var_inside_doc_author
                var_inside_doc_type = var_inside_doc_type
                var_inside_doc_item_full_doc_price = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"]
                
                var_inside_doc_item_note = ""
                var_inside_doc_item_article = ""
                var_inside_doc_item_code = ""
    
    
    
                x = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]
                
                if type(x["ИнфПолеОписРабот"]) == dict:
                    if x["ИнфПолеОписРабот"]["@Идентиф"] == "Ид":
                        var_inside_doc_item_code =  x["ИнфПолеОписРабот"]["@Значен"]
    
                elif type(x["ИнфПолеОписРабот"]) == list:
                    for i_info in range(len(x["ИнфПолеОписРабот"])):
                        if x["ИнфПолеОписРабот"][i_info]["@Идентиф"] == "Примечание":
                           var_inside_doc_item_note = x["ИнфПолеОписРабот"][i_info]["@Значен"] 
                        elif x["ИнфПолеОписРабот"][i_info]["@Идентиф"] == "Ид":
                           var_inside_doc_item_code = x["ИнфПолеОписРабот"][i_info]["@Значен"] 
                        elif x["ИнфПолеОписРабот"][i_info]["@Идентиф"] == "Артикул":
                           var_inside_doc_item_article = x["ИнфПолеОписРабот"][i_info]["@Значен"] 
                        
                var_inside_doc_item_sn = ''
    
                try:
                    var_inside_doc_item_full_doc_price = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"]                    
                except: 
                    var_inside_doc_item_full_doc_price = np.nan
    
                
                try:
                    var_inside_doc_item_price = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Цена"]                    
                except: 
                    var_inside_doc_item_price = np.nan
    
                try:
                    var_inside_doc_item_full_item_price = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"]                    
                except: 
                    var_inside_doc_item_full_item_price = np.nan
    
                try:
                    var_inside_doc_item_quantity =  xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Количество"]
                except: 
                    pass
        
                try:
                    var_inside_doc_item_unit = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@НаимЕдИзм"]
                except:
                    var_inside_doc_item_unit = np.nan
        
                try:
                    var_inside_doc_item_name = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@НаимРабот"]
                except:
                    
                    try:
                        var_inside_doc_item_name = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["Описание"]
                    except:
                        var_inside_doc_item_name = ''
    
                var_agent = ''
                var_date_from = ''
                var_warehouse = ''
        
                try:
                    if type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ИнфПолФХЖ1"]["ТекстИнф"]) == list:
                        str_agent = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ИнфПолФХЖ1"]["ТекстИнф"]
                        for v_a_d in str_agent:
                            if v_a_d["@Идентиф"].lower() == 'агент':
                                    try:
                                        var_agent = v_a_d["@Значен"].replace("&quot;", "")
                                    except:    
                                        var_agent = v_a_d["@Значен"]
                            
                            elif v_a_d["@Идентиф"].lower() == 'дата' or v_a_d["@Идентиф"].lower() == 'датас':
                                    var_date_from = v_a_d["@Значен"]
    
                            elif v_a_d["@Идентиф"].lower() == 'складнаименование':
                                    var_warehouse = v_a_d["@Значен"]
                            
                            else:
                                pass
                except:
                    pass
    
    
                            
                            
                
                try:  
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''   
                except: 
                    var_license_type = ''               
                    
    
    
                
                
                
                doc_append()
                inside_doc_append(var_inside_doc_author,
                    var_inside_doc_type,
                    var_inside_doc_item_full_doc_price,
                    var_inside_doc_item_note,
                    var_inside_doc_item_code,
                    var_inside_doc_item_article,
                    var_inside_doc_item_sn,
                    var_inside_doc_item_name,
                    var_inside_doc_item_quantity,
                    var_inside_doc_item_unit,
                    var_inside_doc_item_price,
                    var_inside_doc_item_full_item_price,
                    var_agent,
                    var_date_from,
                    var_license_type,
                    var_warehouse,
                    )
    
                
    
    
    
            
            elif type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]) == list:
                
                sum_inside_doc = 0
                        
                for k in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]:
                    sum_inside_doc += float(k["@СтоимУчНДС"])
                    
                    
                var_agent = ''
                var_date_from = ''
                var_warehouse = ''
    
                try:
                    if type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ИнфПолФХЖ1"]["ТекстИнф"]) == list:
                        str_agent = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ИнфПолФХЖ1"]["ТекстИнф"]
                        for v_a_d in str_agent:
                            if v_a_d["@Идентиф"].lower() == 'агент':
                                    try:
                                        var_agent = v_a_d["@Значен"].replace("&quot;", "")
                                    except:    
                                        var_agent = v_a_d["@Значен"]
                            
                            elif v_a_d["@Идентиф"].lower() == 'дата' or v_a_d["@Идентиф"].lower() == 'датас':
                                    var_date_from = v_a_d["@Значен"]
    
                            elif v_a_d["@Идентиф"].lower() == 'складнаименование':
                                    var_warehouse = v_a_d["@Значен"]
                            
                            else:
                                pass
                except:
                    pass
    
               
                            
                            
                
                try: 
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''
                except:
                    var_license_type = ''                    
                    
    
                
                    
                    
        
                for k in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]:
        
                    var_inside_doc_item_quantity = 1
        
                    var_inside_doc_author = var_inside_doc_author
                    var_inside_doc_type = var_inside_doc_type
                    var_inside_doc_item_full_doc_price = sum_inside_doc
                
                    var_inside_doc_item_note = ""
                    var_inside_doc_item_article = ""
                    var_inside_doc_item_code = ""
    
                    
                    if type(k["ИнфПолеОписРабот"]) == dict:
                          if k["ИнфПолеОписРабот"]["@Идентиф"] == "Ид":
                             var_inside_doc_item_code =  k["ИнфПолеОписРабот"]["@Значен"]
                    
                    elif type(k["ИнфПолеОписРабот"]) == list:
                          for i_info in range(len(k["ИнфПолеОписРабот"])):
                             if k["ИнфПолеОписРабот"][i_info]["@Идентиф"] == "Примечание":
                                var_inside_doc_item_note = k["ИнфПолеОписРабот"][i_info]["@Значен"] 
                             elif k["ИнфПолеОписРабот"][i_info]["@Идентиф"] == "Ид":
                                var_inside_doc_item_code = k["ИнфПолеОписРабот"][i_info]["@Значен"] 
                             elif k["ИнфПолеОписРабот"][i_info]["@Идентиф"] == "Артикул":
                                var_inside_doc_item_article = k["ИнфПолеОписРабот"][i_info]["@Значен"]                          
        
                    var_inside_doc_item_sn = ''
                    
                    try:
                        var_inside_doc_item_full_doc_price = sum_inside_doc                 
                    except: 
                        var_inside_doc_item_full_doc_price = np.nan
    
                    
                    try:
                        var_inside_doc_item_price = k["@Цена"]                    
                    except: 
                        var_inside_doc_item_price = np.nan
    
                    try:
                        var_inside_doc_item_full_item_price = k["@СтоимУчНДС"]                    
                    except: 
                        var_inside_doc_item_full_item_price = np.nan
    
                    try:
                        var_inside_doc_item_quantity =  k["@Количество"]
                    except: 
                        pass
            
                    try:
                        var_inside_doc_item_unit = k["@НаимЕдИзм"]
                    except:
                        var_inside_doc_item_unit = np.nan
            
                    try:
                        var_inside_doc_item_name = k["@НаимРабот"]
                    except:
                        try:
                            var_inside_doc_item_name = k["Описание"]
                        except:
                            var_inside_doc_item_name = ''
        
                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_sn,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price,
                        var_agent,
                        var_date_from,
                        var_license_type,
                        var_warehouse,
                        )
    
    
    
    
        def_act_vr_set_variable(var_inside_doc_author, var_inside_doc_type)
        
    def def_edo_nakl(xml_a, var_inside_doc_author, var_inside_doc_type):
        
        def def_edo_nakl_print(var_inside_doc_author, var_inside_doc_type):
            
            if type(xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]) == dict:
                var_inside_doc_item_note = ""
                print(var_inside_doc_type)
                print("dict")
            
                try: 
                    print("var_inside_doc_item_article", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@АртикулТов"])
                except:
                    print("var_inside_doc_item_article", np.nan)
                print("var_inside_doc_item_code", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@КодТов"])
                print("var_inside_doc_item_name", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@НаимТов"])
                print("var_inside_doc_item_quantity", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@НеттоПередано"])
                try:
                    print("var_inside_doc_item_unit", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@НаимЕдИзм"])
                except: 
                    print("var_inside_doc_item_unit", np.nan)
                print("var_inside_doc_item_price", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@Цена"])
                print("var_inside_doc_item_full_item_price", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@СтУчНДС"])
                print("var_inside_doc_item_full_doc_price", xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@СтУчНДС"])
                print("_____________________")
    
    
            elif type(xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]) == list:
                sum_inside_doc = 0
                
                for k in xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]:
                    sum_inside_doc += float(k["@СтУчНДС"])
            
                for k in xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]: 
                    
                    print(var_inside_doc_type)
                    print("list")
                    
                    try:
                        print("var_inside_doc_item_article", k["@АртикулТов"])
                    except:
                        print("var_inside_doc_item_article", "отсутствует")
                    print("var_inside_doc_item_code", k["@КодТов"])
                    print("var_inside_doc_item_name", k["@НаимТов"])
                    print("var_inside_doc_item_quantity", k["@НеттоПередано"])
                    print("var_inside_doc_item_unit", k["@НаимЕдИзм"])
                    print("var_inside_doc_item_price", k["@Цена"])
                    print("var_inside_doc_item_full_item_price", k["@СтУчНДС"])
                    print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                    print("_____________________")
            
                    
        def def_edo_nakl_set_variable(var_inside_doc_author, var_inside_doc_type):
            if type(xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]) == dict:
    
                var_inside_doc_item_note = ""
                var_inside_doc_author = var_inside_doc_author
                var_inside_doc_type = var_inside_doc_type
                try: 
                    var_inside_doc_item_full_doc_price = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@СтУчНДС"]
                except: 
                    var_inside_doc_item_full_doc_price = np.nan
                try:
                    var_inside_doc_item_code = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@КодТов"]
                except:
                    var_inside_doc_item_code = np.nan
                try:
                    var_inside_doc_item_article = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@АртикулТов"]
                except:
                    var_inside_doc_item_article = np.nan
                 
                var_inside_doc_item_sn = ''
                
                try:    
                    for l in range(len(xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["ИнфПолФХЖ2"])): 
                        if xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["ИнфПолФХЖ2"][l]["@Идентиф"] == 'SerialNumber':
                            var_inside_doc_item_sn = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["ИнфПолФХЖ2"][l]["@Значен"]
                        else:
                            pass
                except:
                    var_inside_doc_item_sn = ''
                
                    
                    
                try:
                    var_inside_doc_item_name = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@НаимТов"]
                except:
                    var_inside_doc_item_name = np.nan
                try:
                    var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@НеттоПередано"]
                except: 
                    var_inside_doc_item_quantity = np.nan
                try:
                    var_inside_doc_item_unit = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@НаимЕдИзм"]
                except:
                    var_inside_doc_item_unit = np.nan
                try:
                    try:
                        var_inside_doc_item_price = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@Цена"]
                    except:
                        var_inside_doc_item_price = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@СтУчНДС"]
                except:
                    var_inside_doc_item_price = np.nan
                    
                try:
                    var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]["@СтУчНДС"])
                except:
                    var_inside_doc_item_full_item_price = np.nan
                    
    
    
                var_warehouse = ''
                try:
                        str_warehouse = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СвДокПТПр"]["СодФХЖ1"]["ИнфПолФХЖ1"]["ТекстИнф"]
                        for v_warehouse in str_warehouse:
                            if v_warehouse["@Идентиф"].lower() == 'складнаименование':
                                try:
                                    var_warehouse = v_warehouse["@Значен"]
                                except:    
                                    var_warehouse = v_warehouse["@Значен"]
                            else:
                                pass
                except:
                        var_warehouse = ''
    
    
                
                var_agent = ''
                try:
                        str_agent = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СвДокПТПр"]["СодФХЖ1"]["ИнфПолФХЖ1"]["ТекстИнф"]
                        for v_agent in str_agent:
                            if v_agent["@Идентиф"].lower() == 'агент':
                                try:
                                    var_agent = v_agent["@Значен"][2:-2].replace("&quot;", "")
                                except:    
                                    var_agent = v_agent["@Значен"][2:-2]
                            else:
                                pass
                except:
                        var_agent = ''
                    
                    
                var_date_from = ''
                try:
                        str_date_from = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СвДокПТПр"]["СодФХЖ1"]["ИнфПолФХЖ1"]["ТекстИнф"]
                        for v_date_from in str_date_from:
                            if v_date_from["@Идентиф"].lower() == 'дата' or v_date_from["@Идентиф"].lower() == 'датас':
                                var_date_from = v_date_from["@Значен"]
                            else:
                                pass
                except:
                        var_date_from = ''
                        
                try: 
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''
                except: 
                    var_license_type = ''                    
                    
                    
                            
                    
                    
                    
    
                doc_append()
                inside_doc_append(var_inside_doc_author,
                    var_inside_doc_type,
                    var_inside_doc_item_full_doc_price,
                    var_inside_doc_item_note,
                    var_inside_doc_item_code,
                    var_inside_doc_item_article,
                    var_inside_doc_item_sn,
                    var_inside_doc_item_name,
                    var_inside_doc_item_quantity,
                    var_inside_doc_item_unit,
                    var_inside_doc_item_price,
                    var_inside_doc_item_full_item_price,
                    var_agent,
                    var_date_from,
                    var_license_type,
                    var_warehouse,
                    )
                
                
            elif type(xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]) == list:
                
                    sum_inside_doc = 0
                    
                
                    for k in xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]:
                        try:
                            sum_inside_doc += float(k["@СтУчНДС"])
                        except:
                            sum_inside_doc += 0
                            
    
    
    
                    var_warehouse = ''
                    try:
                            str_warehouse = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СвДокПТПр"]["СодФХЖ1"]["ИнфПолФХЖ1"]["ТекстИнф"]
                            for v_warehouse in str_warehouse:
                                if v_warehouse["@Идентиф"].lower() == 'складнаименование':
                                    try:
                                        var_warehouse = v_warehouse["@Значен"]
                                    except:    
                                        var_warehouse = v_warehouse["@Значен"]
                                else:
                                    pass
                    except:
                            var_warehouse = ''
    
                
                            
                    var_agent = ''
                    try:
                            str_agent = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СвДокПТПр"]["СодФХЖ1"]["ИнфПолФХЖ1"]["ТекстИнф"]
                            for v_agent in str_agent:
                                if v_agent["@Идентиф"].lower() == 'агент':
                                    try:
                                        var_agent = v_agent["@Значен"][2:-2].replace("&quot;", "")
                                    except:    
                                        var_agent = v_agent["@Значен"][2:-2]
                                else:
                                    pass
                    except:
                            var_agent = ''
                        
                        
                    var_date_from = ''
                    try:
                            str_date_from = xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СвДокПТПр"]["СодФХЖ1"]["ИнфПолФХЖ1"]["ТекстИнф"]
                            for v_date_from in str_date_from:
                                if v_date_from["@Идентиф"].lower() == 'дата' or v_date_from["@Идентиф"].lower() == 'датас':
                                    var_date_from = v_date_from["@Значен"]
                                else:
                                    pass
                    except:
                            var_date_from = ''
                        
    
                    var_license_type = ''
                    try:
                        for var_i_license_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            try:
                                if var_i_license_type["@Идентиф"] == 'ХарактНоменклатуры':
                                    var_license_type = var_i_license_type["@Значен"]
                                else:
                                    pass
                            except:
                                var_license_type = ''                        
                    except:
                        var_license_type = ''
                                
                                   
                            
                    
                    for k in xml_a["Файл"]["Документ"]["СвДокПТПрКроме"]["СодФХЖ2"]["СвТов"]: 
                        
                        var_inside_doc_item_note = ""
    
                        var_inside_doc_author = var_inside_doc_author
                        var_inside_doc_type = var_inside_doc_type
                        try:
                            var_inside_doc_item_full_doc_price = sum_inside_doc
                        except: 
                            var_inside_doc_item_full_doc_price = np.nan
                        try:
                            var_inside_doc_item_code =  k["@КодТов"]
                        except: 
                            var_inside_doc_item_code = np.nan
                        try:
                            var_inside_doc_item_article = k["@АртикулТов"]
                        except:
                            var_inside_doc_item_article = np.nan
                        
                        var_inside_doc_item_sn = ''    
                            
                        try:
                            for l in range(len(k["ИнфПолФХЖ2"])):
                                if k["ИнфПолФХЖ2"][l]["@Идентиф"] == 'SerialNumber':
                                    var_inside_doc_item_sn = k["ИнфПолФХЖ2"][l]["@Значен"]
                                else: 
                                    pass
                        except:
                            var_inside_doc_item_sn = ''    
                            
                        try:
                            var_inside_doc_item_name = k["@НаимТов"]
                        except:
                            var_inside_doc_item_name = np.nan
                        try:
                            var_inside_doc_item_quantity =  k["@НеттоПередано"]
                        except:
                            var_inside_doc_item_quantity = np.nan
                        try:
                            var_inside_doc_item_unit = k["@НаимЕдИзм"]
                        except: 
                            var_inside_doc_item_unit = np.nan
                        try:
                            try:                                
                                var_inside_doc_item_price = k["@Цена"]
                            except:
                                var_inside_doc_item_price = k["@СтУчНДС"]
                        except:
                            var_inside_doc_item_price = np.nan
                        try:
                            var_inside_doc_item_full_item_price = float(k["@СтУчНДС"])
                        except:
                            var_inside_doc_item_full_item_price = np.nan
                
        
                        doc_append()
                        inside_doc_append(var_inside_doc_author,
                            var_inside_doc_type,
                            var_inside_doc_item_full_doc_price,
                            var_inside_doc_item_note,
                            var_inside_doc_item_code,
                            var_inside_doc_item_article,
                            var_inside_doc_item_sn,
                            var_inside_doc_item_name,
                            var_inside_doc_item_quantity,
                            var_inside_doc_item_unit,
                            var_inside_doc_item_price,
                            var_inside_doc_item_full_item_price,
                            var_agent,
                            var_date_from,
                            var_license_type,
                            var_warehouse,
                            )
    
    
    
    
    
        def_edo_nakl_set_variable(var_inside_doc_author, var_inside_doc_type)
    
    def def_act_pp(xml_a, var_inside_doc_author, var_inside_doc_type):
        
        def def_act_pp_print(var_inside_doc_author, var_inside_doc_type):
            
    
            if type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]) == dict:
                print(var_inside_doc_type)
                print("dict")
    
                var_inside_doc_item_note = ""
            
                for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
            
                    try:
                        print("var_inside_doc_item_article", re.findall(f"\d+\-\d+", k["@ИнфПолСтр"])[0])
                    except:
                        try:
                            print("var_inside_doc_item_article", re.findall(f"услуга|услуги", k["@ИнфПолСтр"])[0])
            
                        except:
                            print("var_inside_doc_item_article", np.nan)
                
            
            
                print("var_inside_doc_item_code", xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@КодПП"])
                print("var_inside_doc_item_quantity", xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@КолПП"])
                print("var_inside_doc_item_name", xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@НаимПП"])
                try:
                    print("var_inside_doc_item_price", xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Цена"])
                except:
                    print("var_inside_doc_item_price", np.nan)               
                print("var_inside_doc_item_full_item_price", float(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Стоим"]))
                sum_inside_doc = float(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Стоим"])
                print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                print("_____________________")
    
            
            elif type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]) == list:
                print(var_inside_doc_type)
                print("list")
    
                print(type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]))
                
                sum_inside_doc = 0
            
                for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
                    sum_inside_doc += float(k["@Стоим"])
                                                            
                for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
                    
                    try:
                        print("var_inside_doc_item_article", re.findall(f"\d+\-\d+", k["@ИнфПолСтр"])[0])
                    except:
                        try:
                            print("var_inside_doc_item_article", re.findall(f"услуга|услуги", k["@ИнфПолСтр"])[0])
            
                        except:
                            print("var_inside_doc_item_article", np.nan)
                    
                    print("var_inside_doc_item_code", k["@КодПП"])
                    print("var_inside_doc_item_quantity", k["@КолПП"])
                    print("var_inside_doc_item_name", k["@НаимПП"])
                    try:
                        print("var_inside_doc_item_price", k["@Цена"])
                    except:
                        print("var_inside_doc_item_price", np.nan)
                    print("var_inside_doc_item_full_item_price", float(k["@Стоим"]))
                    print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                    print("_____________________")
    
                
                                
                    
        def def_act_pp_set_variable(var_inside_doc_author, var_inside_doc_type):
    
    
            if type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]) == dict:
                
                var_inside_doc_item_note = ""
    
                var_inside_doc_author = var_inside_doc_author
                var_inside_doc_type = var_inside_doc_type
                var_inside_doc_item_full_doc_price = float(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Стоим"])
                var_inside_doc_item_code = xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@КодПП"]
            
            
                for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
            
                    try:
                        var_inside_doc_item_article = re.findall(f"\d+\-\d+", k["@ИнфПолСтр"])[0]
                    except:
                        try:
                            var_inside_doc_item_article = re.findall(f"услуга|услуги", k["@ИнфПолСтр"])[0]
                        except:
                            var_inside_doc_item_article = np.nan
                
                var_inside_doc_item_sn = ''
                    
                var_inside_doc_item_name = xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@НаимПП"]
                var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@КолПП"]
                var_inside_doc_item_unit = np.nan
                try:
                    var_inside_doc_item_price = xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Цена"]
                except:
                    var_inside_doc_item_price = np.nan
                var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Стоим"])
                
    
                
                var_warehouse = ''
                try:
                    str_warehouse = xml_a["Файл"]["Документ"]["СведДок"]["ИнфПол"]["@ТекстИнф"]
                    if len(re.findall(f'Имя="СкладНаименование"', str_warehouse)) != 0:
                        start_index = re.search(f'\\[&apos;', str_warehouse).span()[1]
                        end_index = re.search(f'&apos;\\]', str_warehouse).span()[0]
                        try:
                            var_warehouse = str_warehouse[start_index:end_index].replace("&quot;", "")
                        except:
                            var_warehouse = str_warehouse[start_index:end_index]
                    else:
                        var_warehouse = ''
    
                except:
                    var_warehouse = ''
    
                
                var_agent = ''
                try:
                    str_agent = xml_a["Файл"]["Документ"]["СведДок"]["ИнфПол"]["@ТекстИнф"]
                    if len(re.findall(f'Имя="Агент"', str_agent)) != 0:
                        start_index = re.search(f'\\[&apos;', str_agent).span()[1]
                        end_index = re.search(f'&apos;\\]', str_agent).span()[0]
                        try:
                            var_agent = str_agent[start_index:end_index].replace("&quot;", "")
                        except:
                            var_agent = str_agent[start_index:end_index]
                    else:
                        var_agent = ''
    
                except:
                    var_agent = ''
    
    
    
                var_date_from = ''
                try:
                    str_date_from = xml_a["Файл"]["Документ"]["СведДок"]["ИнфПол"]["@ТекстИнф"]
                    if len(re.findall(f'Имя="ДАТА"', str_date_from)) != 0:
                        index_start_str = re.search(f'Имя="ДАТА"', str_date_from).span()[1]
                        temp_str = str_date_from[index_start_str:]
                        var_date_from = re.findall(f'\d+\.\d+\.\d+', temp_str)[0]
                    else:
                        var_date_from = ''
                except:
                    var_date_from = ''
                    
                try: 
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''
                except:
                    var_license_type = ''                    
                    
    
                
                
                
                doc_append()
                inside_doc_append(var_inside_doc_author,
                    var_inside_doc_type,
                    var_inside_doc_item_full_doc_price,
                    var_inside_doc_item_note,
                    var_inside_doc_item_code,
                    var_inside_doc_item_article,
                    var_inside_doc_item_sn,
                    var_inside_doc_item_name,
                    var_inside_doc_item_quantity,
                    var_inside_doc_item_unit,
                    var_inside_doc_item_price,
                    var_inside_doc_item_full_item_price,
                    var_agent,
                    var_date_from,
                    var_license_type,
                    var_warehouse,
                    ) 
                
                
            
            elif type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]) == list:      
    
                sum_inside_doc = 0
    
                for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
                    sum_inside_doc += float(k["@Стоим"]) 
    
                
                var_warehouse = ''
                try:
                    str_warehouse = xml_a["Файл"]["Документ"]["СведДок"]["ИнфПол"]["@ТекстИнф"]
                    if len(re.findall(f'Имя="СкладНаименование"', str_warehouse)) != 0:
                        start_index = re.search(f'\\[&apos;', str_warehouse).span()[1]
                        end_index = re.search(f'&apos;\\]', str_warehouse).span()[0]
                        try:
                            var_warehouse = str_warehouse[start_index:end_index].replace("&quot;", "")
                        except:
                            var_warehouse = str_warehouse[start_index:end_index]
                    else:
                        var_warehouse = ''
    
                except:
                    var_warehouse = ''                    
                
                
                var_agent = ''
                try:
                    str_agent = xml_a["Файл"]["Документ"]["СведДок"]["ИнфПол"]["@ТекстИнф"]
                    if len(re.findall(f'Имя="Агент"', str_agent)) != 0:
                        start_index = re.search(f'\\[&apos;', str_agent).span()[1]
                        end_index = re.search(f'&apos;\\]', str_agent).span()[0]
                        try:
                            var_agent = str_agent[start_index:end_index].replace("&quot;", "")
                        except:
                            var_agent = str_agent[start_index:end_index]
                    else:
                        var_agent = ''
    
                except:
                    var_agent = ''
    
    
    
                var_date_from = ''
                try:
                    str_date_from = xml_a["Файл"]["Документ"]["СведДок"]["ИнфПол"]["@ТекстИнф"]
                    if len(re.findall(f'Имя="ДАТА"', str_date_from)) != 0:
                        index_start_str = re.search(f'Имя="ДАТА"', str_date_from).span()[1]
                        temp_str = str_date_from[index_start_str:]
                        var_date_from = re.findall(f'\d+\.\d+\.\d+', temp_str)[0]
                    else:
                        var_date_from = ''
                except:
                    var_date_from = ''                        
                    
                try: 
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''
                except:
                    var_license_type = ''
                    
                    
                    
                
                for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
    
                    var_inside_doc_item_note = ""
        
                    var_inside_doc_author = var_inside_doc_author
                    var_inside_doc_type = var_inside_doc_type
                    var_inside_doc_item_full_doc_price = sum_inside_doc
                    var_inside_doc_item_code = k["@КодПП"]
        
                    try:
                        var_inside_doc_item_article = re.findall(f"\d+\-\d+", k["@ИнфПолСтр"])[0]
                    except:
                        try:
                            var_inside_doc_item_article = re.findall(f"услуга|услуги", k["@ИнфПолСтр"])[0]
        
                        except:
                            var_inside_doc_item_article = np.nan
        
                    var_inside_doc_item_sn = ''
                        
                    var_inside_doc_item_name = k["@НаимПП"]
                    try:
                        var_inside_doc_item_quantity = k["@КолПП"]
                    except:
                        var_inside_doc_item_quantity = np.nan
    
                    var_inside_doc_item_unit = np.nan
                    try:
                        var_inside_doc_item_price = k["@Цена"]
                    except:
                        var_inside_doc_item_price = np.nan
                        
                    var_inside_doc_item_full_item_price = float(k["@Стоим"])       
                    
                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_sn,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price,
                        var_agent,
                        var_date_from,
                        var_license_type,
                        var_warehouse,
                        ) 
    
    
    
    
    
        def_act_pp_set_variable(var_inside_doc_author, var_inside_doc_type)
    
    def def_upd_dop(xml_a, var_inside_doc_author, var_inside_doc_type):
        
        def def_upd_dop_print(var_inside_doc_author, var_inside_doc_type):
    
            if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == dict:
            
                print(var_inside_doc_type)
                print("dict")
            
                var_inside_doc_item_note = ""
                
                try:
                    print("var_inside_doc_item_quantity", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@КолТов"])
                except:
                    print("var_inside_doc_item_quantity", np.nan)
            
                print("var_inside_doc_item_name", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@НаимТов"])
                try:
                    print("var_inside_doc_item_price", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@ЦенаТов"])
                except: 
                    print("var_inside_doc_item_price", np.nan)
                try:
                    print("var_inside_doc_item_article", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@АртикулТов"])
                except: 
                    print("var_inside_doc_item_article", np.nan)
            
                print("var_inside_doc_item_code", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@КодТов"])
                try:
                    print("var_inside_doc_item_unit", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@НаимЕдИзм"])
                except:
                    print("var_inside_doc_item_unit", np.nan)
            
                try:
                    print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["ВсегоОпл"]["@СтТовУчНалВсего"]))
                except:
                    print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"]))
    
    
            elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == list:
                                
                print(var_inside_doc_type)
                print("list")
            
                
                sum_inside_doc = 0
                            
                for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:
                    sum_inside_doc += float(k["@СтТовУчНал"])
                        
                for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:                    
                    
                    print("list")
                    
                    try:                
                        print("var_inside_doc_item_quantity", k["@КолТов"])
                    except:
                        print("var_inside_doc_item_quantity", np.nan)
                        
                    print("var_inside_doc_item_name", k["@НаимТов"])
                    try:
                        print("var_inside_doc_item_price", k["@ЦенаТов"])
                    except:
                        print("var_inside_doc_item_price", np.nan)
                    try:
                        print("var_inside_doc_item_article", k["ДопСведТов"]["@АртикулТов"])
                    except:
                        print("var_inside_doc_item_article", np.nan)
            
                    print("var_inside_doc_item_code", k["ДопСведТов"]["@КодТов"])
                    try:
                        print("var_inside_doc_item_unit", k["ДопСведТов"]["@НаимЕдИзм"])
                    except:
                        print("var_inside_doc_item_unit", np.nan)
                    try:
                        print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["ВсегоОпл"]["@СтТовУчНалВсего"]))
                    except: 
                        print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                    print("var_inside_doc_item_full_item_price", float(k["@СтТовУчНал"]))
    
        def def_upd_dop_set_variable(var_inside_doc_author, var_inside_doc_type):
            
            if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == dict:
                
                var_inside_doc_item_note = ""
                
                sum_inside_doc = 0
                try:
                    sum_inside_doc = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
                except:
                    sum_inside_doc = np.nan
                    
                var_inside_doc_author = var_inside_doc_author
                var_inside_doc_type = var_inside_doc_type
                var_inside_doc_item_full_doc_price = sum_inside_doc
                var_inside_doc_item_code = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@КодТов"]
                
                
            
            
                try:
                    var_inside_doc_item_article =  xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@АртикулТов"]
                except: 
                    var_inside_doc_item_article = np.nan
                    
                var_inside_doc_item_sn = ''
                
                try:
                    for l in range(len(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"])): 
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"][l]["@Идентиф"] == 'SerialNumber':
                            var_inside_doc_item_sn = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"][l]["@Значен"]
                        else:
                            pass
                except:
                    var_inside_doc_item_sn = ''                
            
            
            
            
            
                    
                var_inside_doc_item_name = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@НаимТов"]
                try:
                    var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@КолТов"]
                except: 
                    var_inside_doc_item_quantity = np.nan
                try:
                    var_inside_doc_item_unit = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@НаимЕдИзм"]
                except:
                    var_inside_doc_item_unit = np.nan
                try:
                    var_inside_doc_item_price = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@ЦенаТов"]
                except: 
                    var_inside_doc_item_price = np.nan
                    
                try:
                    var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
                except:
                    var_inside_doc_item_full_item_price = np.nan
                
                
    
                var_warehouse = ''
                try:
                    str_warehouse = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_warehouse in str_warehouse:
                        if v_warehouse["@Идентиф"].lower() == 'складнаименование':
                                try:
                                    var_warehouse = v_warehouse["@Значен"]
                                except:    
                                    var_warehouse = v_warehouse["@Значен"]
                        else:
                            pass
                except:
                    var_warehouse = ''
    
                
                var_agent = ''
                try:
                    str_agent = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_agent in str_agent:
                        if v_agent["@Идентиф"].lower() == 'агент':
                            try:
                                var_agent = v_agent["@Значен"][2:-2].replace("&quot;", "")
                            except:    
                                var_agent = v_agent["@Значен"][2:-2]
                        else:
                            pass
                except:
                    var_agent = ''
                
                
                var_date_from = ''
                try:
                    str_date_from = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_date_from in str_date_from:
                        if v_date_from["@Идентиф"].lower() == 'дата' or v_date_from["@Идентиф"].lower() == 'датас':
                            var_date_from = v_date_from["@Значен"]
                        else:
                            pass
                except:
                    var_date_from = ''                    
                
                 
    
                try:
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''
                except:
                    var_license_type = ''
    
        
    
                doc_append()
                inside_doc_append(var_inside_doc_author,
                    var_inside_doc_type,
                    var_inside_doc_item_full_doc_price,
                    var_inside_doc_item_note,
                    var_inside_doc_item_code,
                    var_inside_doc_item_article,
                    var_inside_doc_item_sn,
                    var_inside_doc_item_name,
                    var_inside_doc_item_quantity,
                    var_inside_doc_item_unit,
                    var_inside_doc_item_price,
                    var_inside_doc_item_full_item_price,
                    var_agent,
                    var_date_from,
                    var_license_type,
                    var_warehouse,
                    ) 
    
                
    
            elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == list:
    
                
                sum_inside_doc = 0
                            
                try:
                    for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:
                        sum_inside_doc += float(k["@СтТовУчНал"])
                except:
                    sum_inside_doc += 0
                    
                
                
    
                var_warehouse = ''
                try:
                    str_warehouse = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_warehouse in str_warehouse:
                        if v_warehouse["@Идентиф"].lower() == 'складнаименование':
                                try:
                                    var_warehouse = v_warehouse["@Значен"]
                                except:    
                                    var_warehouse = v_warehouse["@Значен"]
                        else:
                            pass
                except:
                    var_warehouse = ''
    
                
                var_agent = ''
                try:
                    str_agent = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_agent in str_agent:
                        if v_agent["@Идентиф"].lower() == 'агент':
                            try:
                                var_agent = v_agent["@Значен"][2:-2].replace("&quot;", "")
                            except:    
                                var_agent = v_agent["@Значен"][2:-2]
                        else:
                            pass
                except:
                    var_agent = ''
                
                
                var_date_from = ''
                try:
                    str_date_from = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_date_from in str_date_from:
                        if v_date_from["@Идентиф"].lower() == 'дата' or v_date_from["@Идентиф"].lower() == 'датас':
                            var_date_from = v_date_from["@Значен"]
                        else:
                            pass
                except:
                    var_date_from = ''
                
    
    
                try:
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''
                except:
                    var_license_type = ''
    
                
                                                            
    
                        
                for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:                     
                
                
                    var_inside_doc_item_note = ""
                
                    var_inside_doc_author = var_inside_doc_author
                    var_inside_doc_type = var_inside_doc_type
                    var_inside_doc_item_full_doc_price = sum_inside_doc
                    var_inside_doc_item_code = k["ДопСведТов"]["@КодТов"]
    
    
               
    
                    
                    try:
                        var_inside_doc_item_article =  k["ДопСведТов"]["@АртикулТов"]
                    except:
                        var_inside_doc_item_article =  np.nan
                        
                        
                    var_inside_doc_item_sn = ''     
                        
                    try:
                        for l in range(len(k["ИнфПолФХЖ2"])):
                            if k["ИнфПолФХЖ2"][l]["@Идентиф"] == 'SerialNumber':
                                var_inside_doc_item_sn = k["ИнфПолФХЖ2"][l]["@Значен"]
                            else: 
                                pass
                    except:
                        var_inside_doc_item_sn = ''        
                
                        
                    var_inside_doc_item_name = k["@НаимТов"]
                    
                    try:
                        var_inside_doc_item_quantity = k["@КолТов"]
                    except:
                        var_inside_doc_item_quantity = np.nan
            
                    try:
                        var_inside_doc_item_unit = k["ДопСведТов"]["@НаимЕдИзм"]
                    except:
                        var_inside_doc_item_unit = np.nan
                    try:
                        var_inside_doc_item_price = k["@ЦенаТов"]
                    except:
                        var_inside_doc_item_price = np.nan
    
                    try:
                        var_inside_doc_item_full_item_price = float(k["@СтТовУчНал"])
                    except: 
                        var_inside_doc_item_full_item_price = np.nan
                        
                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_sn,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price,
                        var_agent,
                        var_date_from,
                        var_license_type,
                        var_warehouse,
                        )    
    
    
    
    
    
    
        def_upd_dop_set_variable(var_inside_doc_author, var_inside_doc_type)
    
    def def_upd_s_dop(xml_a, var_inside_doc_author, var_inside_doc_type):
        
        def def_upd_s_dop_print(var_inside_doc_author, var_inside_doc_type):
    
            if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == dict:
    
                try:
                    print("var_inside_doc_item_quantity", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@КолТов"])
                except: 
                    print("var_inside_doc_item_quantity", np.nan)
                print("var_inside_doc_item_name", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@НаимТов"])
                
                try:
                    print("var_inside_doc_item_price", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@ЦенаТов"])
                except:
                    print("var_inside_doc_item_price", np.nan)
                
                try:
                    print("var_inside_doc_item_article", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@АртикулТов"])
                except:
                    print("var_inside_doc_item_article", np.nan)
                
                print("var_inside_doc_item_code", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@КодТов"])
                
                try:
                    print("var_inside_doc_item_unit", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@НаимЕдИзм"])
                except:
                    print("var_inside_doc_item_unit", np.nan)
            
                try:
                    print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["ВсегоОпл"]["@СтТовУчНалВсего"]))
                except:
                    print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"]))
                
                sum_inside_doc = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
                print("var_inside_doc_item_full_item_price", sum_inside_doc)
    
    
            elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == list:
                                
                print(var_inside_doc_type)
                print("list")
                                
                sum_inside_doc = 0
                
                try:
                    for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:
                        sum_inside_doc += float(k["@СтТовУчНал"])
                except:
                    sum_inside_doc += 0
                                                            
                for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:                    
            
                    print("list")
                    try:
                        print("var_inside_doc_item_quantity",k["@КолТов"])
                    except: 
                        print("var_inside_doc_item_quantity",np.nan)
                    print("var_inside_doc_item_name", k["@НаимТов"])
                    try:
                        print("var_inside_doc_item_price", k["@ЦенаТов"])
                    except:
                        print("var_inside_doc_item_price", np.nan)
                    try:
                        print("var_inside_doc_item_article", k["ДопСведТов"]["@АртикулТов"])
                    except: 
                        print("var_inside_doc_item_article", np.nan)
                    print("var_inside_doc_item_code", k["ДопСведТов"]["@КодТов"])
                    try:
                        print("var_inside_doc_item_unit", k["ДопСведТов"]["@НаимЕдИзм"])
                    except:
                        print("var_inside_doc_item_unit", np.nan)
            
                    try: 
                        print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["ВсегоОпл"]["@СтТовУчНалВсего"]))
                    except: 
                        print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                    print("var_inside_doc_item_full_item_price", float(k["@СтТовУчНал"]))
                        
    
        def def_upd_s_dop_set_variable(var_inside_doc_author, var_inside_doc_type):
            
            if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == dict:
                
                var_inside_doc_item_note = ""
    
    
        
    
                var_warehouse = ''
                try:
                    str_warehouse = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_warehouse in str_warehouse:
                        if v_warehouse["@Идентиф"].lower() == 'складнаименование':
                                try:
                                    var_warehouse = v_warehouse["@Значен"]
                                except:    
                                    var_warehouse = v_warehouse["@Значен"]
                        else:
                            pass
                except:
                    var_warehouse = ''
    
               
                var_agent = ''
                try:
                    str_agent = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_agent in str_agent:
                        if v_agent["@Идентиф"].lower() == 'агент':
                                try:
                                    var_agent = v_agent["@Значен"][2:-2].replace("&quot;", "")
                                except:    
                                    var_agent = v_agent["@Значен"][2:-2]
                        else:
                            pass
                except:
                    var_agent = ''
                
                
                var_date_from = ''
                try:
                    str_date_from = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_date_from in str_date_from:
                        if v_date_from["@Идентиф"].lower() == 'дата' or v_date_from["@Идентиф"].lower() == 'датас':
                            var_date_from = v_date_from["@Значен"]
                        else:
                            pass
                except:
                    var_date_from = ''
    
                try: 
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''
                except:
                    var_license_type = ''                    
                    
                
                var_inside_doc_author = var_inside_doc_author
                var_inside_doc_type = var_inside_doc_type
                
                try:
                    var_inside_doc_item_full_doc_price = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
                except: 
                    var_inside_doc_item_full_doc_price = np.nan
                    
                var_inside_doc_item_code = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@КодТов"]
            
                try:
                    var_inside_doc_item_article = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@АртикулТов"]
                except:
                    var_inside_doc_item_article = np.nan
                    
                var_inside_doc_item_sn = ''
                 
                try:
                    for l in range(len(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"])): 
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"][l]["@Идентиф"] == 'SerialNumber':
                            var_inside_doc_item_sn = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"][l]["@Значен"]
                        else:
                            pass
                except:
                    var_inside_doc_item_sn = ''     
                    
                    
                    
                                        
                var_inside_doc_item_name = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@НаимТов"]
                
                try:
                    var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@КолТов"]
                except:
                    var_inside_doc_item_quantity = np.nan
                
                
                try:
                    var_inside_doc_item_unit = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@НаимЕдИзм"]
                except: 
                    var_inside_doc_item_unit = np.nan
            
                try:
                    var_inside_doc_item_price = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@ЦенаТов"]
                except:
                    var_inside_doc_item_price = np.nan
                
                try:
                    var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
                except:
                    var_inside_doc_item_full_item_price = np.nan
                
                doc_append()
                inside_doc_append(var_inside_doc_author,
                    var_inside_doc_type,
                    var_inside_doc_item_full_doc_price,
                    var_inside_doc_item_note,
                    var_inside_doc_item_code,
                    var_inside_doc_item_article,
                    var_inside_doc_item_sn,
                    var_inside_doc_item_name,
                    var_inside_doc_item_quantity,
                    var_inside_doc_item_unit,
                    var_inside_doc_item_price,
                    var_inside_doc_item_full_item_price,
                    var_agent,
                    var_date_from,
                    var_license_type,
                    var_warehouse, 
                    )    
    
                
            
            elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == list:
    
                sum_inside_doc = 0    
    
                try:
                    for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:
                        sum_inside_doc += float(k["@СтТовУчНал"])
                except:
                    sum_inside_doc = np.nan
                    
    
                var_warehouse = ''
                try:
                    str_warehouse = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_warehouse in str_warehouse:
                        if v_warehouse["@Идентиф"].lower() == 'складнаименование':
                                try:
                                    var_warehouse = v_warehouse["@Значен"]
                                except:    
                                    var_warehouse = v_warehouse["@Значен"]
                        else:
                            pass
                except:
                    var_warehouse = ''
    
                   
                var_agent = ''
                try:
                    str_agent = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_agent in str_agent:
                        if v_agent["@Идентиф"].lower() == 'агент':
                                try:
                                    var_agent = v_agent["@Значен"][2:-2].replace("&quot;", "")
                                except:    
                                    var_agent = v_agent["@Значен"][2:-2]
                        else:
                            pass
                except:
                    var_agent = ''
                
                
                var_date_from = ''
                try:
                    str_date_from = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_date_from in str_date_from:
                        if v_date_from["@Идентиф"].lower() == 'дата' or v_date_from["@Идентиф"].lower() == 'датас':
                            var_date_from = v_date_from["@Значен"]
                        else:
                            pass
                except:
                    var_date_from = ''                        
                    
                try: 
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
                    
                            else:
                                var_license_type = ''
                except:
                    var_license_type = ''                    
                    
               
                    
                                                            
                for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:  
                
                    var_inside_doc_item_note = ""
            
                    var_inside_doc_author = var_inside_doc_author
                    var_inside_doc_type = var_inside_doc_type
                    var_inside_doc_item_full_doc_price = sum_inside_doc
                    var_inside_doc_item_code = k["ДопСведТов"]["@КодТов"]
            
            
                    try:
                        var_inside_doc_item_article =  k["ДопСведТов"]["@АртикулТов"]
                    except:
                        var_inside_doc_item_article =  np.nan
                        
                        
                    var_inside_doc_item_sn = ''
                    
                    try:
                        for l in range(len(k["ИнфПолФХЖ2"])):
                            if k["ИнфПолФХЖ2"][l]["@Идентиф"] == 'SerialNumber':
                                var_inside_doc_item_sn = k["ИнфПолФХЖ2"][l]["@Значен"]
                            else: 
                                pass
                    except:
                        var_inside_doc_item_sn = ''     
            
                        
                    var_inside_doc_item_name = k["@НаимТов"]
                    
                    try:
                        var_inside_doc_item_quantity = k["@КолТов"]
                    except:
                        var_inside_doc_item_quantity = np.nan
                        
                    try:
                        var_inside_doc_item_unit = k["ДопСведТов"]["@НаимЕдИзм"]
                    except:
                        var_inside_doc_item_unit = np.nan
                     
                    try:       
                        var_inside_doc_item_price = k["@ЦенаТов"]
                    except:
                        var_inside_doc_item_price = np.nan
                        
                    try:
                        var_inside_doc_item_full_item_price = float(k["@СтТовУчНал"])
                    except:
                        var_inside_doc_item_full_item_price = np.nan
                
                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_sn,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price,
                        var_agent,
                        var_date_from,
                        var_license_type,
                        var_warehouse,
                        )
                    
    
    
        def_upd_s_dop_set_variable(var_inside_doc_author, var_inside_doc_type)
            
            
    url = url_sbis
    
    method = "СБИС.Аутентифицировать"
    params = {
        "Параметр": {
            "Логин": API_sbis,
            "Пароль": API_sbis_pass
        }
    
    }
    parameters = {
    "jsonrpc": "2.0",
    "method": method,
    "params": params,
    "id": 0
    }
    
    response = requests.post(url, json=parameters)
    response.encoding = 'utf-8'
    
    str_to_dict = json.loads(response.text)
    access_token = str_to_dict["result"]
    
    headers = {
    "X-SBISSessionID": access_token,
    "Content-Type": "application/json",
    }  
    
    doc_id = []
    doc_type = []
    doc_number = []
    doc_full_name = []
    doc_data_main = []
    doc_at_created = []

    doc_counterparty_inn = []
    doc_counterparty_kpp = []
    doc_counterparty_full_name = []

    doc_provider_inn = []
    doc_provider_kpp = []
    doc_provider_full_name = []

    doc_assigned_manager = []
    doc_department = []

    base_doc_conn_type = []
    base_doc_date = []
    base_doc_id = []
    base_doc_dir = []
    base_doc_num = []
    base_doc_reglament_id = []
    base_doc_name = []
    base_doc_type = []
    base_doc_sum = []

    consequence_doc_conn_type = []
    consequence_doc_date = []
    consequence_doc_id = []
    consequence_doc_dir = []
    consequence_doc_num = []
    consequence_doc_reglament_id = []
    consequence_doc_name = []
    consequence_doc_type = []
    consequence_doc_sum = []
    
    inside_doc_author = []
    inside_doc_type = []
    inside_doc_item_full_doc_price = []
    
    inside_doc_item_note = []
    
    inside_doc_item_code = []
    inside_doc_item_article = []
    inside_doc_item_sn = []
    inside_doc_item_name = []
    
    inside_doc_item_quantity = []
    inside_doc_item_unit = []
    
    inside_doc_item_price = []
    inside_doc_item_full_item_price = []
    
    inside_agent = []
    inside_date_from = []
    
    inside_license_type = []
    
    inside_warehouse = []
    doc_id_exc = []
    doc_type_exc = []
    doc_number_exc = []
    doc_full_name_exc = []
    doc_data_main_exc = []
    doc_at_created_exc = []

    doc_counterparty_inn_exc = []
    doc_counterparty_kpp_exc = []
    doc_counterparty_full_name_exc = []

    doc_provider_inn_exc = []
    doc_provider_kpp_exc = []
    doc_provider_full_name_exc = []
    
    doc_assigned_manager_exc = []
    doc_department_exc = []
    
    var_status_has_more = "Да"
    i_page = 0
    
    while var_status_has_more == "Да":
        
        parameters_real = {
        "jsonrpc": "2.0",
        "method": "СБИС.СписокДокументов",
        "params": {
            "Фильтр": {
            "ДатаС": date_from,
            "ДатаПо": date_to,
            "Тип": "ДокОтгрИсх",
            "Регламент": {
                "Название": "Реализация"
            },
            "Навигация": {
                "Страница": i_page
            }
            }
        },
        "id": 0
        }
        
        url_real = url_sbis_unloading
        
        response_points = requests.post(url_real, json=parameters_real, headers=headers)
    
        str_to_dict_points_main = json.loads(response_points.text)
        
        json_data_points = json.dumps(str_to_dict_points_main, ensure_ascii=False, indent=4).encode("utf8").decode()
        
        
        j = 0
        for i in str_to_dict_points_main["result"]["Документ"]:
            j += 1
            if (re.findall("реал", i["Регламент"]["Название"].lower())[-1] == "реал") and (i["Расширение"]["Проведен"].lower() == 'да'):
                
                try:
                    var_link = i["Идентификатор"]
                except:
                    var_link = np.nan
                    
    
    
                try:
                    doc_manager_first_name = str(i["Ответственный"]["Имя"])
                except:
                    doc_manager_first_name = ""
                try:
                    doc_manager_last_name = str(i["Ответственный"]["Фамилия"])
                except:
                    doc_manager_last_name = ""
                try:
                    doc_manager_surname_name = str(i["Ответственный"]["Отчество"])
                except:
                    doc_manager_surname_name = ""
    
                try:
                    doc_manager_name = " ".join([doc_manager_last_name, doc_manager_first_name, doc_manager_surname_name])
                except:
                    doc_manager_name = np.nan
    
                try:
                    var_doc_type = i["Регламент"]["Название"]
                except:
                    var_doc_type = np.nan
    
                try:
                    var_doc_number = i["Номер"] 
                except:
                    var_doc_number = np.nan
    
                try:
                    var_doc_full_name = i["Название"]
                except:
                    var_doc_full_name = np.nan
    
                try:
                    var_doc_data_main = i["Дата"]
                except:
                    var_doc_data_main = np.nan
    
                try:
                    var_doc_at_created = i["ДатаВремяСоздания"]
                except:
                    var_doc_at_created = np.nan
                try:
                    try:
                        var_doc_counterparty_inn = i["Контрагент"]["СвФЛ"]["ИНН"]
                        try:
                            var_doc_counterparty_kpp = i["Контрагент"]["СвФЛ"]["КПП"]
                        except:
                            var_doc_counterparty_kpp = ''
                        var_doc_counterparty_full_name = i["Контрагент"]["СвФЛ"]["НазваниеПолное"]
                    except:
                        var_doc_counterparty_inn = i["Контрагент"]["СвЮЛ"]["ИНН"]
                        try:
                            var_doc_counterparty_kpp = i["Контрагент"]["СвЮЛ"]["КПП"]
                        except:
                            var_doc_counterparty_kpp = ''
                        var_doc_counterparty_full_name = i["Контрагент"]["СвЮЛ"]["НазваниеПолное"]
                except:
                    var_doc_counterparty_inn = ''
                    var_doc_counterparty_kpp = ''
                    var_doc_counterparty_full_name = ''
                try:
                    try:
                        var_doc_provider_inn = i["НашаОрганизация"]["СвФЛ"]["ИНН"]
                        try:
                            var_doc_provider_kpp = i["НашаОрганизация"]["СвФЛ"]["КПП"]
                        except:
                            var_doc_provider_kpp = ''
                        var_doc_provider_full_name = i["НашаОрганизация"]["СвФЛ"]["НазваниеПолное"]
                    except:
                        var_doc_provider_inn = i["НашаОрганизация"]["СвЮЛ"]["ИНН"]
                        try:
                            var_doc_provider_kpp = i["НашаОрганизация"]["СвЮЛ"]["КПП"]
                        except:
                            var_doc_provider_kpp = ''
                        var_doc_provider_full_name = i["НашаОрганизация"]["СвЮЛ"]["НазваниеПолное"]
                except:
                    var_doc_provider_inn = ''
                    var_doc_provider_kpp = ''
                    var_doc_provider_full_name = ''
    
                try:
                    var_doc_assigned_manager = doc_manager_name
                except:
                    var_doc_assigned_manager = np.nan
                try:
                    var_doc_department = i["Подразделение"]["Название"]
                except:
                    var_doc_department = np.nan    
                
                
                if j % 10 == 0:
                     
                     
                    url = url_sbis
                
                    method = "СБИС.Аутентифицировать"
                    params = {
                        "Параметр": {
                            "Логин": API_sbis,
                            "Пароль": API_sbis_pass
                        }
                
                    }
                    parameters = {
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": params,
                    "id": 0
                    }
                
                    response = requests.post(url, json=parameters)
                    response.encoding = 'utf-8'
                
                    str_to_dict = json.loads(response.text)
                    access_token = str_to_dict["result"]
                
                    headers = {
                    "X-SBISSessionID": access_token,
                    "Content-Type": "application/json",
                    }            
                else:
                    pass
                
    
                parameters_real = {
                "jsonrpc": "2.0",
                "method": "СБИС.ПрочитатьДокумент",
                "params": {
                    "Документ": {
                        "Идентификатор": var_link,
                        "ДопПоля": "ДополнительныеПоля"
                    }
                },
                "id": 0
                }
            
                url_real = url_sbis_unloading
            
                response_points = requests.post(url_real, json=parameters_real, headers=headers)
                str_to_dict_points = json.loads(response_points.text)
                
                try:
                    name = str_to_dict_points["result"]["Автор"]["Имя"]
                except:
                    name = ""
                try:
                    second_name = str_to_dict_points["result"]["Автор"]["Фамилия"]
                except:
                    second_name = ""
                try:
                    surname_name = str_to_dict_points["result"]["Автор"]["Отчество"]
                except:
                    surname_name = ""
            
                author_list = [name, second_name, surname_name]
                
                try:
                    var_inside_doc_author = " ".join(author_list).strip()
                except:
                    var_inside_doc_author = np.nan

                var_base_doc_conn_type = ''
                var_base_doc_date = ''
                var_base_doc_id = ''
                var_base_doc_dir = ''
                var_base_doc_num = ''
                var_base_doc_reglament_id = ''
                var_base_doc_name = ''
                var_base_doc_type = ''
                var_base_doc_sum = ''

                try:
                    for i_b in str_to_dict_points["result"]["ДокументОснование"]:
                        try:
                            if i_b["Документ"]["Тип"].lower() == 'счетисх' or i_b["Тип"].lower() == 'cчетисх':
                                var_base_doc_conn_type = i_b["ВидСвязи"]
                                var_base_doc_date = i_b["Документ"]["Дата"]
                                var_base_doc_id = i_b["Документ"]["Идентификатор"]
                                var_base_doc_dir = i_b["Документ"]["Направление"]
                                var_base_doc_num = i_b["Документ"]["Номер"]
                                var_base_doc_reglament_id = i_b["Документ"]["Регламент"]["Идентификатор"]
                                var_base_doc_name = i_b["Документ"]["Регламент"]["Название"]
                                var_base_doc_type = i_b["Документ"]["Тип"]
                                var_base_doc_sum = i_b["Сумма"]
                        except:
                            pass
                except:
                    pass

                var_consequence_doc_conn_type = ''
                var_consequence_doc_date = ''
                var_consequence_doc_id = ''
                var_consequence_doc_dir = ''
                var_consequence_doc_num = ''
                var_consequence_doc_reglament_id = ''
                var_consequence_doc_name = ''
                var_consequence_doc_type = ''
                var_consequence_doc_sum = ''

                try:
                    for i_c in str_to_dict_points["result"]["ДокументСледствие"]:
                        try:
                            if i_c["Документ"]["Тип"].lower() == 'входящийплатеж' or i_c["Тип"].lower() == 'входящийплатеж':
                                var_consequence_doc_conn_type = i_c["ВидСвязи"]
                                var_consequence_doc_date = i_c["Документ"]["Дата"]
                                var_consequence_doc_id = i_c["Документ"]["Идентификатор"]
                                var_consequence_doc_dir = i_c["Документ"]["Направление"]
                                var_consequence_doc_num = i_c["Документ"]["Номер"]
                                var_consequence_doc_reglament_id = i_c["Документ"]["Регламент"]["Идентификатор"]
                                var_consequence_doc_name = i_c["Документ"]["Регламент"]["Название"]
                                var_consequence_doc_type = i_c["Документ"]["Тип"]
                                var_consequence_doc_sum = i_c["Сумма"]
                        except:
                            pass
                except:
                    pass
                
           
                def common_part_print():
                    print(j)
                    try:
                        print("var_link", var_link)
                    except:
                        print("var_link", np.nan)
                    try:
                        print("var_doc_type", i["Регламент"]["Название"])
                    except:
                        print("var_doc_type", np.nan)
                    try:
                        print("var_doc_number", i["Номер"])
                    except:
                        print("var_doc_number", np.nan)
                    try:
                        print("var_doc_full_name", i["Название"])
                    except:
                        print("var_doc_full_name", np.nan)
    
                    try:
                        print("var_doc_data_main", i["Дата"])
                    except:
                        print("var_doc_data_main", np.nan)
                    try:
                        print("var_doc_at_created", i["ДатаВремяСоздания"])
                    except:
                        print("var_doc_at_created", np.nan)
                    
                    try:
                        try:
                            print("var_doc_counterparty_inn", i["Контрагент"]["СвФЛ"]["ИНН"])
                            print("var_doc_counterparty_full_name", i["Контрагент"]["СвФЛ"]["НазваниеПолное"])
                        except:
                            print("var_doc_counterparty_inn", i["Контрагент"]["СвЮЛ"]["ИНН"])
                            print("var_doc_counterparty_full_name", i["Контрагент"]["СвЮЛ"]["НазваниеПолное"])  
                    except:
                        print("var_doc_counterparty_inn", np.nan)
                        print("var_doc_counterparty_full_name", np.nan)                          
    
                    try:
                        try:
                            print("var_doc_provider_inn", i["НашаОрганизация"]["СвФЛ"]["ИНН"])
                            print("var_doc_provider_full_name", i["НашаОрганизация"]["СвФЛ"]["НазваниеПолное"])
                        except:
                            print("var_doc_provider_inn", i["НашаОрганизация"]["СвЮЛ"]["ИНН"])
                            print("var_doc_provider_full_name", i["НашаОрганизация"]["СвЮЛ"]["НазваниеПолное"])
                    except:
                        print("var_doc_provider_inn", np.nan)
                        print("var_doc_provider_full_name", np.nan)                        
                
                    print("var_doc_assigned_manager", doc_manager_name)
                    try:
                        print("var_doc_department", i["Подразделение"]["Название"])
                    except:
                        print("var_doc_department", np.nan)
    
                    try:
                        print("автор:", " ".join(author_list).strip())
                    except:
                        print("автор:", np.nan)
                    
    
                
                
                
                try:
                    attachments_id = {}
                    try:
                        for l in range(len(str_to_dict_points["result"]["ВложениеУчета"])):
                            if str_to_dict_points["result"]["ВложениеУчета"][l]["Тип"].lower() in ("актпп", "актвр", "эдонакл", "упддоп", "упдсчфдоп"):
                                attachments_id[str_to_dict_points["result"]["ВложениеУчета"][l]["Тип"].lower()] = str_to_dict_points["result"]["ВложениеУчета"][l]["Файл"]["Ссылка"]
                            else:
                                pass
                    except:
                        pass
                    
                    try:
                        link_xml = list(attachments_id.values())[0]
                        a_xml = requests.get(link_xml, headers=headers)
                        a_xml.encoding = "cp1251"
                        xml_a_try = xmltodict.parse(a_xml.text)
                    except:
                        print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                        try:                                
                            send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                            print('sent in sbis')
                            print('_____________')
                        except Exception as e:
                            print(e)
                            print('_____________')   
                
                except:
                    
                    attachments_id = {}
                    try:
                        for l in range(len(str_to_dict_points["result"]["Вложение"])):
                            if str_to_dict_points["result"]["Вложение"][l]["Тип"].lower() in ("актпп", "актвр", "эдонакл", "упддоп", "упдсчфдоп"):
                                attachments_id[str_to_dict_points["result"]["Вложение"][l]["Тип"].lower()] = str_to_dict_points["result"]["Вложение"][l]["Файл"]["Ссылка"]
                            else:
                                pass
                    except:
                        pass
                    
                    try:
                        
                        link_xml = list(attachments_id.values())[0]
                        a_xml = requests.get(link_xml, headers=headers)
                        a_xml.encoding = "cp1251"
                        xml_a_try = xmltodict.parse(a_xml.text)       
                    except:
                        print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                        try:                                
                            send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                            print('sent in sbis')
                            print('_____________')
                        except Exception as e:
                            print(e)
                            print('_____________')       
                             
                              
                if len(attachments_id) == 0:
                    
                    doc_append_exc()               
                elif len(attachments_id) > 0:
                
                        
                    for b in attachments_id.keys():
                        
                        if  b == "актвр":
                            a = requests.get(attachments_id["актвр"], headers=headers)
                            a.encoding = "cp1251"
                            
                            try:
                                xml_a = xmltodict.parse(a.text)
                                var_inside_doc_type = "актвр"
    
                                def_act_vr(xml_a, var_inside_doc_author, var_inside_doc_type)
                            except:
                                print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                                try:                                
                                    send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                                    print('sent in sbis')
                                    print('_____________')
                                except Exception as e:
                                    print(e)
                                    print('_____________')                          
                            
                        elif  b == "эдонакл":
                    
                            a = requests.get(attachments_id["эдонакл"], headers=headers)
                            a.encoding = "cp1251"
                            
                            try:
                                xml_a = xmltodict.parse(a.text)
                                var_inside_doc_type = "эдонакл"
                                def_edo_nakl(xml_a, var_inside_doc_author, var_inside_doc_type)
                                
                            except:
                                print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                                try:                                
                                    send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                                    print('sent in sbis')
                                    print('_____________')
                                except Exception as e:
                                    print(e)
                                    print('_____________')                         
                        
                        elif  b == "актпп":     
                                
                            a = requests.get(attachments_id["актпп"], headers=headers)
                            a.encoding = "cp1251"
    
                            try:
                                xml_a = xmltodict.parse(a.text)    
                                
                                var_inside_doc_type = "актпп"
                        
                                def_act_pp(xml_a, var_inside_doc_author, var_inside_doc_type)
                            
                            except:
                                print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                                try:                                
                                    send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                                    print('sent in sbis')
                                    print('_____________')
                                except Exception as e:
                                    print(e)
                                    print('_____________')                                                
                        
                        elif  b == "упддоп":   
                            
                            a = requests.get(attachments_id["упддоп"], headers=headers)
                            a.encoding = "cp1251"
                            
    
                            try:
                                xml_a = xmltodict.parse(a.text)    
    
                            
                                var_inside_doc_type = "упддоп"         
                            
                                def_upd_dop(xml_a, var_inside_doc_author, var_inside_doc_type)
                            
                            except:
                                print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                                try:                                
                                    send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                                    print('sent in sbis')
                                    print('_____________')
                                except Exception as e:
                                    print(e)
                                    print('_____________')  
                        elif  b == "упдсчфдоп":     
                            a = requests.get(attachments_id["упдсчфдоп"], headers=headers)
                            a.encoding = "cp1251"
                            
    
                            try:
                                xml_a = xmltodict.parse(a.text)    
    
                            
                                var_inside_doc_type = "упдсчфдоп"         
                            
                                def_upd_s_dop(xml_a, var_inside_doc_author, var_inside_doc_type)
                            
                            except:
                                print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                                try:                                
                                    send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                                    print('sent in sbis')
                                    print('_____________')
                                except Exception as e:
                                    print(e)
                                    print('_____________')  
    
            
                
                else:
                    doc_append_exc()      
                                        
        if var_status_has_more == "Нет":
            break
        elif str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"] == "Да":
            i_page += 1
        else:
            pass
        var_status_has_more = str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"]
            
            
    lst_append = [doc_id,
    doc_type,
    doc_number,
    doc_full_name,
    doc_data_main,
    doc_at_created,
    
    doc_counterparty_inn,
    doc_counterparty_kpp,
    doc_counterparty_full_name,
    
    doc_provider_inn,
    doc_provider_kpp,
    doc_provider_full_name,
    
    doc_assigned_manager,
    doc_department,

    base_doc_conn_type,
    base_doc_date,
    base_doc_id,
    base_doc_dir,
    base_doc_num,
    base_doc_reglament_id,
    base_doc_name,
    base_doc_type,
    base_doc_sum,

    consequence_doc_conn_type,
    consequence_doc_date,
    consequence_doc_id,
    consequence_doc_dir,
    consequence_doc_num,
    consequence_doc_reglament_id,
    consequence_doc_name,
    consequence_doc_type,
    consequence_doc_sum,
    
    inside_doc_author,
    inside_doc_type,
    inside_doc_item_full_doc_price,
    
    inside_doc_item_note,
    
    inside_doc_item_code,
    inside_doc_item_article,
    inside_doc_item_name,
    
    inside_doc_item_quantity,
    inside_doc_item_unit,
    
    inside_doc_item_price,
    inside_doc_item_full_item_price,
    
    inside_agent,
    inside_date_from,
    
    inside_license_type,
    
    inside_doc_item_sn,
                  
    inside_warehouse,
    ]
    
    lst_append_name = [
        "doc_id",
        "doc_type",
        "doc_number",
        "doc_full_name",
        "doc_data_main",
        "doc_at_created",
        
        "doc_counterparty_inn",
        "doc_counterparty_kpp",
        "doc_counterparty_full_name",
        
        "doc_provider_inn",
        "doc_provider_kpp",
        "doc_provider_full_name",
        
        "doc_assigned_manager",
        "doc_department",

        "base_doc_conn_type",
        "base_doc_date",
        "base_doc_id",
        "base_doc_dir",
        "base_doc_num",
        "base_doc_reglament_id",
        "base_doc_name",
        "base_doc_type",
        "base_doc_sum",

        "consequence_doc_conn_type",
        "consequence_doc_date",
        "consequence_doc_id",
        "consequence_doc_dir",
        "consequence_doc_num",
        "consequence_doc_reglament_id",
        "consequence_doc_name",
        "consequence_doc_type",
        "consequence_doc_sum",
        
        "inside_doc_author",
        "inside_doc_type",
        "inside_doc_item_full_doc_price",
        
        "inside_doc_item_note",
        
        "inside_doc_item_code",
        "inside_doc_item_article",
        "inside_doc_item_name",
        
        "inside_doc_item_quantity",
        "inside_doc_item_unit",
        
        "inside_doc_item_price",
        "inside_doc_item_full_item_price",
        
        "inside_agent",
        "inside_date_from",
        
        "inside_license_type",
        
        "inside_doc_item_sn",
        
        "inside_warehouse",
    
    ]    
            
    lst_append_exc_name = [
        "doc_id",
        "doc_type",
        "doc_number",
        "doc_full_name",
        "doc_data_main",
        "doc_at_created",
        
        "doc_counterparty_inn",
        "doc_counterparty_kpp",
        "doc_counterparty_full_name",
        
        "doc_provider_inn",
        "doc_provider_kpp",
        "doc_provider_full_name",
        
        "doc_assigned_manager",
        "doc_department",
    ]     
            
    df = pd.DataFrame(columns=lst_append_name, data=list(zip(
    doc_id,
    doc_type,
    doc_number,
    doc_full_name,
    doc_data_main,
    doc_at_created,
    
    doc_counterparty_inn,
    doc_counterparty_kpp,
    doc_counterparty_full_name,
    
    doc_provider_inn,
    doc_provider_kpp,
    doc_provider_full_name,
    
    doc_assigned_manager,
    doc_department,

    base_doc_conn_type,
    base_doc_date,
    base_doc_id,
    base_doc_dir,
    base_doc_num,
    base_doc_reglament_id,
    base_doc_name,
    base_doc_type,
    base_doc_sum,

    consequence_doc_conn_type,
    consequence_doc_date,
    consequence_doc_id,
    consequence_doc_dir,
    consequence_doc_num,
    consequence_doc_reglament_id,
    consequence_doc_name,
    consequence_doc_type,
    consequence_doc_sum,
    
    inside_doc_author,
    inside_doc_type,
    inside_doc_item_full_doc_price,
    
    inside_doc_item_note,
    
    inside_doc_item_code,
    inside_doc_item_article,
    inside_doc_item_name,
    
    inside_doc_item_quantity,
    inside_doc_item_unit,
    
    inside_doc_item_price,
    inside_doc_item_full_item_price,
    
    inside_agent,
    inside_date_from, 
    
    inside_license_type,   
    
    inside_doc_item_sn,
        
    inside_warehouse,
    )))
        
    def doc_append_exc():
        doc_id_exc.append(var_link)
        doc_type_exc.append(var_doc_type)
        doc_number_exc.append(var_doc_number)
        doc_full_name_exc.append(var_doc_full_name)
        doc_data_main_exc.append(var_doc_data_main)
        doc_at_created_exc.append(var_doc_at_created)
        doc_counterparty_inn_exc.append(var_doc_counterparty_inn)
        doc_counterparty_kpp_exc.append(var_doc_counterparty_kpp)
        doc_counterparty_full_name_exc.append(var_doc_counterparty_full_name)
        doc_provider_inn_exc.append(var_doc_provider_inn)
        doc_provider_kpp_exc.append(var_doc_provider_kpp)
        doc_provider_full_name_exc.append(var_doc_provider_full_name)
    
        doc_assigned_manager_exc.append(var_doc_assigned_manager)
        doc_department_exc.append(var_doc_department)       
            
    df_exc = pd.DataFrame(columns=lst_append_exc_name, data=list(zip(
    doc_id_exc,
    doc_type_exc,
    doc_number_exc,
    doc_full_name_exc,
    doc_data_main_exc,
    doc_at_created_exc,
    
    doc_counterparty_inn_exc,
    doc_counterparty_kpp_exc,
    doc_counterparty_full_name_exc,
    
    doc_provider_inn_exc,
    doc_provider_kpp_exc,
    doc_provider_full_name_exc,
    
    doc_assigned_manager_exc,
    doc_department_exc,
    )))
    
        
    my_conn = create_engine(f"postgresql+psycopg2://{var_db_user_name}:{var_db_user_pass}@{var_db_host}:{var_db_port}/{var_db_name}")
    try: 
        my_conn.connect()
        print('my_conn.connect()')
        my_conn = my_conn.connect()
        df.to_sql(name=f'{name_unloading}', con=my_conn, if_exists="replace")
        print("df.sent()")
        my_conn.close()
        print("my_conn.close()")
    except:
        print('failed')
        print('my_conn.failed()')        
            
    my_conn = create_engine(f"postgresql+psycopg2://{var_db_user_name}:{var_db_user_pass}@{var_db_host}:{var_db_port}/{var_db_name}")
    try: 
        my_conn.connect()
        print('my_conn.connect()')
        my_conn = my_conn.connect()
        df_exc.to_sql(name=f'{name_unloading_exc}', con=my_conn, if_exists="replace")
        print("df_exc.sent()")
        my_conn.close()
        print("my_conn.close()")
    except:
        print('failed')
        print('my_conn.failed()') 


# sbis_real_processing_0('17.04.2026', '17.04.2026', 'TEST_new_api_sbis', 'TEST_new_api_sbis_exc')