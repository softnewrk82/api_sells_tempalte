


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
        
        doc_notes.append(var_doc_notes)
        doc_revision_of_notes.append(var_doc_revision_of_notes)
        
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
                        # var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price,
                        
                        var_inside_doc_item_type_agreement,
                        var_inside_doc_item_validity,
                        ):
        
        inside_doc_author.append(var_inside_doc_author)
        inside_doc_type.append(var_inside_doc_type)
        inside_doc_item_full_doc_price.append(var_inside_doc_item_full_doc_price)
        
        # inside_doc_item_note.append(var_inside_doc_item_note)
        
        inside_doc_item_code.append(var_inside_doc_item_code)
        inside_doc_item_article.append(var_inside_doc_item_article)
        inside_doc_item_name.append(var_inside_doc_item_name)
        
        inside_doc_item_quantity.append(var_inside_doc_item_quantity)
        inside_doc_item_unit.append(var_inside_doc_item_unit)
        
        inside_doc_item_price.append(var_inside_doc_item_price)
        inside_doc_item_full_item_price.append(var_inside_doc_item_full_item_price)
        
        inside_doc_item_type_agreement.append(var_inside_doc_item_type_agreement)
        inside_doc_item_validity.append(var_inside_doc_item_validity)

    
    
    
    
    def def_edo_invoice(xml_a, var_inside_doc_author, var_inside_doc_type):
            
        def def_edo_invoice_set_variable(var_inside_doc_author, var_inside_doc_type):
            if type(xml_a["Файл"]["Документ"]["СодСч"]["СведТовЦенПок"]) == dict:
    
                # var_inside_doc_item_note = ""
                var_inside_doc_author = var_inside_doc_author
                var_inside_doc_type = var_inside_doc_type
                try: 
                    var_inside_doc_item_full_doc_price = xml_a["Файл"]["Документ"]["СодСч"]["ВсегоОпл"]["@СтТовУчНалВсего"]
                except: 
                    var_inside_doc_item_full_doc_price = np.nan
                try:
                    var_inside_doc_item_code = xml_a["Файл"]["Документ"]["СодСч"]["СведТовЦенПок"]["ДопСведТов"]["@КодТов"]
                except:
                    var_inside_doc_item_code = np.nan
                try:
                    var_inside_doc_item_article = xml_a["Файл"]["Документ"]["СодСч"]["СведТовЦенПок"]["ДопСведТов"]["@АртикулТов"]
                except:
                    var_inside_doc_item_article = np.nan
                 
                 
                try:
                    var_inside_doc_item_type_agreement = xml_a["Файл"]["Документ"]["СодСч"]["СведТовЦенПок"]["ИнфПол"]["ТекстИнф"]["@Значен"]
                except: 
                    var_inside_doc_item_type_agreement = ''
                

                var_inside_doc_item_validity = ''
                try:    
                    for i_validity in xml_a["Файл"]["Документ"]["ИнфПол"]["ТекстИнф"]:
                        var_x_y = re.findall('срокдействия', i_validity["@Идентиф"].lower())
                        if len(var_x_y) > 0:
                            if var_x_y[0] == 'срокдействия':
                                var_inside_doc_item_validity = i_validity["@Значен"]
                except:
                    pass
                

                    
                try:
                    var_inside_doc_item_name = xml_a["Файл"]["Документ"]["СодСч"]["СведТовЦенПок"]["@НаимТов"]
                except:
                    var_inside_doc_item_name = np.nan
                try:
                    var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["СодСч"]["СведТовЦенПок"]["@КолТов"]
                except: 
                    var_inside_doc_item_quantity = np.nan
                try:
                    var_inside_doc_item_unit = xml_a["Файл"]["Документ"]["СодСч"]["СведТовЦенПок"]["@НаимЕдИзм"]
                except:
                    var_inside_doc_item_unit = np.nan
                try:
                    try:
                        var_inside_doc_item_price = xml_a["Файл"]["Документ"]["СодСч"]["СведТовЦенПок"]["@СтТовБезНДС"]
                    except:
                        var_inside_doc_item_price = xml_a["Файл"]["Документ"]["СодСч"]["СведТовЦенПок"]["@ЦенаТов"]
                except:
                    var_inside_doc_item_price = np.nan
                    
                try:
                    var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["СодСч"]["СведТовЦенПок"]["@СтТовУчНал"])
                except:
                    var_inside_doc_item_full_item_price = np.nan
                    
                    
                            
                doc_append()
                inside_doc_append(var_inside_doc_author,
                    var_inside_doc_type,
                    var_inside_doc_item_full_doc_price,
                    # var_inside_doc_item_note,
                    var_inside_doc_item_code,
                    var_inside_doc_item_article,
                    var_inside_doc_item_name,
                    var_inside_doc_item_quantity,
                    var_inside_doc_item_unit,
                    var_inside_doc_item_price,
                    var_inside_doc_item_full_item_price,

                    var_inside_doc_item_type_agreement,
                    var_inside_doc_item_validity,

                    )
                
                
            elif type(xml_a["Файл"]["Документ"]["СодСч"]["СведТовЦенПок"]) == list:
                
                    sum_inside_doc = 0
                    
                
                    for k in xml_a["Файл"]["Документ"]["СодСч"]["СведТовЦенПок"]:
                        try:
                            sum_inside_doc += float(k["@СтТовУчНал"])
                        except:
                            sum_inside_doc += 0
                            
                            
                                   
                    
                    for k in xml_a["Файл"]["Документ"]["СодСч"]["СведТовЦенПок"]: 
                        
                        # var_inside_doc_item_note = ""
    
                        var_inside_doc_author = var_inside_doc_author
                        var_inside_doc_type = var_inside_doc_type
                        
                        
                        try:
                            var_inside_doc_item_type_agreement = k["ИнфПол"]["ТекстИнф"]["@Значен"]
                        except: 
                            var_inside_doc_item_type_agreement = ''
                        
                        
                        var_inside_doc_item_validity = ''
                        try:    
                            for i_validity in xml_a["Файл"]["Документ"]["ИнфПол"]["ТекстИнф"]:
                                var_x_y = re.findall('срокдействия', i_validity["@Идентиф"].lower())
                                if len(var_x_y) > 0:
                                    if var_x_y[0] == 'срокдействия':
                                        var_inside_doc_item_validity = i_validity["@Значен"]
                        except:
                            pass
                        
                            
                        
                        try:
                            var_inside_doc_item_full_doc_price = sum_inside_doc
                        except: 
                            var_inside_doc_item_full_doc_price = np.nan
                        try:
                            var_inside_doc_item_code =  k["ДопСведТов"]["@КодТов"]
                        except: 
                            var_inside_doc_item_code = np.nan
                        try:
                            var_inside_doc_item_article = k["ДопСведТов"]["@АртикулТов"]
                        except:
                            var_inside_doc_item_article = np.nan
                     
                            
                        try:
                            var_inside_doc_item_name = k["@НаимТов"]
                        except:
                            var_inside_doc_item_name = np.nan
                        try:
                            var_inside_doc_item_quantity =  k["@КолТов"]
                        except:
                            var_inside_doc_item_quantity = np.nan
                        try:
                            var_inside_doc_item_unit = k["@НаимЕдИзм"]
                        except: 
                            var_inside_doc_item_unit = np.nan
                        try:
                            try:                                
                                var_inside_doc_item_price = k["@СтТовБезНДС"]
                            except:
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
                            # var_inside_doc_item_note,
                            var_inside_doc_item_code,
                            var_inside_doc_item_article,
                            var_inside_doc_item_name,
                            var_inside_doc_item_quantity,
                            var_inside_doc_item_unit,
                            var_inside_doc_item_price,
                            var_inside_doc_item_full_item_price,

                            var_inside_doc_item_type_agreement,
                            var_inside_doc_item_validity,

                            )
    
    
        def_edo_invoice_set_variable(var_inside_doc_author, var_inside_doc_type)
   
    
 
         
    def def_edo_invoice_type2(xml_a, var_inside_doc_author, var_inside_doc_type):
            
        def def_edo_invoice_type2_set_variable(var_inside_doc_author, var_inside_doc_type):
            if type(xml_a["Файл"]["Документ"]["ТаблСчет"]["СведТов"]) == dict:
    
                # var_inside_doc_item_note = ""
                var_inside_doc_author = var_inside_doc_author
                var_inside_doc_type = var_inside_doc_type
                try: 
                    var_inside_doc_item_full_doc_price = xml_a["Файл"]["Документ"]["ТаблСчет"]["ВсегоОпл"]["@СтТовУчНалВсего"]
                except: 
                    var_inside_doc_item_full_doc_price = np.nan
                try:
                    var_inside_doc_item_code = xml_a["Файл"]["Документ"]["ТаблСчет"]["СведТов"]["@КодТов"]
                except:
                    var_inside_doc_item_code = np.nan
                try:
                    var_inside_doc_item_article = xml_a["Файл"]["Документ"]["ТаблСчет"]["СведТов"]["@АртикулТов"]
                except:
                    var_inside_doc_item_article = np.nan
                 
                 
                var_inside_doc_item_type_agreement = ''
                try:
                    list_var_inside_doc_item_type_agreement = re.findall(r'\[&apos;(.*?)&apos;\]', xml_a["Файл"]["Документ"]["ТаблСчет"]["СведТов"]["@ИнфПолСтр"])
                    if len(list_var_inside_doc_item_type_agreement) > 0:
                        var_inside_doc_item_type_agreement = str(list_var_inside_doc_item_type_agreement)
                except: 
                    pass
                
                var_inside_doc_item_validity = ''
                try:    
                    list_var_inside_doc_item_validity = re.findall(r'\[&apos;(.*?)&apos;\]', xml_a["Файл"]["Документ"]["СвСчет"]["ИнфПол"]["@ТекстИнф"])
                    if len(list_var_inside_doc_item_validity) > 0:
                        var_inside_doc_item_validity = str(list_var_inside_doc_item_validity)                    
                except:
                    pass
                

                    
                try:
                    var_inside_doc_item_name = xml_a["Файл"]["Документ"]["ТаблСчет"]["СведТов"]["@НаимТов"]
                except:
                    var_inside_doc_item_name = np.nan
                try:
                    var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["ТаблСчет"]["СведТов"]["@КолТов"]
                except: 
                    var_inside_doc_item_quantity = np.nan
                try:
                    var_inside_doc_item_unit = xml_a["Файл"]["Документ"]["ТаблСчет"]["СведТов"]["@ЕдИзм"]
                except:
                    var_inside_doc_item_unit = np.nan
                try:
                    try:
                        var_inside_doc_item_price = xml_a["Файл"]["Документ"]["ТаблСчет"]["СведТов"]["@ЦенаТовСНДС"]
                    except:
                        var_inside_doc_item_price = xml_a["Файл"]["Документ"]["ТаблСчет"]["СведТов"]["@ЦенаТов"]
                except:
                    var_inside_doc_item_price = np.nan
                    
                try:
                    var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["ТаблСчет"]["СведТов"]["@СтТовУчНал"])
                except:
                    var_inside_doc_item_full_item_price = np.nan
                    
                    
                            
                doc_append()
                inside_doc_append(var_inside_doc_author,
                    var_inside_doc_type,
                    var_inside_doc_item_full_doc_price,
                    # var_inside_doc_item_note,
                    var_inside_doc_item_code,
                    var_inside_doc_item_article,
                    var_inside_doc_item_name,
                    var_inside_doc_item_quantity,
                    var_inside_doc_item_unit,
                    var_inside_doc_item_price,
                    var_inside_doc_item_full_item_price,

                    var_inside_doc_item_type_agreement,
                    var_inside_doc_item_validity,

                    )
                
                
            elif type(xml_a["Файл"]["Документ"]["ТаблСчет"]["СведТов"]) == list:
                
                    sum_inside_doc = 0
                    
                
                    for k in xml_a["Файл"]["Документ"]["ТаблСчет"]["СведТов"]:
                        try:
                            sum_inside_doc += float(k["@СтТовУчНал"])
                        except:
                            sum_inside_doc += 0
                            
                            
                                   
                    
                    for k in xml_a["Файл"]["Документ"]["ТаблСчет"]["СведТов"]: 
                        
                        # var_inside_doc_item_note = ""
    
                        var_inside_doc_author = var_inside_doc_author
                        var_inside_doc_type = var_inside_doc_type
                        


                        try:
                            list_var_inside_doc_item_type_agreement = re.findall(r'\[&apos;(.*?)&apos;\]', k["@ИнфПолСтр"])
                            if len(list_var_inside_doc_item_type_agreement) > 0:
                                var_inside_doc_item_type_agreement = str(list_var_inside_doc_item_type_agreement)
                        except: 
                            var_inside_doc_item_type_agreement = ''
                        

                        var_inside_doc_item_validity = ''
                        try:    
                            list_var_inside_doc_item_validity = re.findall(r'\[&apos;(.*?)&apos;\]', xml_a["Файл"]["Документ"]["СвСчет"]["ИнфПол"]["@ТекстИнф"])
                            if len(list_var_inside_doc_item_validity) > 0:
                                var_inside_doc_item_validity = str(list_var_inside_doc_item_validity)                    
                        except:
                            pass

                        
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
                     
                            
                        try:
                            var_inside_doc_item_name = k["@НаимТов"]
                        except:
                            var_inside_doc_item_name = np.nan
                        try:
                            var_inside_doc_item_quantity =  k["@КолТов"]
                        except:
                            var_inside_doc_item_quantity = np.nan
                        try:
                            var_inside_doc_item_unit = k["@ЕдИзм"]
                        except: 
                            var_inside_doc_item_unit = np.nan
                        try:
                            try:                                
                                var_inside_doc_item_price = k["@ЦенаТовСНДС"]
                            except:
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
                            # var_inside_doc_item_note,
                            var_inside_doc_item_code,
                            var_inside_doc_item_article,
                            var_inside_doc_item_name,
                            var_inside_doc_item_quantity,
                            var_inside_doc_item_unit,
                            var_inside_doc_item_price,
                            var_inside_doc_item_full_item_price,

                            var_inside_doc_item_type_agreement,
                            var_inside_doc_item_validity,

                            )
    
    
        def_edo_invoice_type2_set_variable(var_inside_doc_author, var_inside_doc_type)
   
    
 
         
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
    
    doc_notes = []
    doc_revision_of_notes = []

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
    
    # inside_doc_item_note = []
    
    inside_doc_item_code = []
    inside_doc_item_article = []
    inside_doc_item_name = []
    
    inside_doc_item_quantity = []
    inside_doc_item_unit = []
    
    inside_doc_item_price = []
    inside_doc_item_full_item_price = []
    
    inside_doc_item_type_agreement = []
    inside_doc_item_validity = []

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
            "Тип": "СчетИсх",
            # "Тип": "ДокОтгрИсх",
            # "Регламент": {
            #     "Название": "Реализация"
            # },
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
            if (re.findall("счет", i["Регламент"]["Название"].lower())[-1] == "счет"):
            # if (re.findall("реал", i["Регламент"]["Название"].lower())[-1] == "счет") and (i["Расширение"]["Проведен"].lower() == 'да'):
                
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
                    
                try:
                    var_doc_notes = i["Примечание"]
                except:
                    var_doc_notes = ''
                
                
                lst_revision_of_notes = []
                var_doc_revision_of_notes = ''
                
                if len(i["Редакция"]) > 0:
                    for i_n in i["Редакция"]:
                        try:
                            lst_revision_of_notes.append(i_n["ПримечаниеИС"])
                        except: 
                            pass
                    var_doc_revision_of_notes = ', '.join(lst_revision_of_notes)
                            
                
                
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
                            if i_b["Документ"]["Тип"].lower() == 'лид' or i_b["Тип"].lower() == 'лид':
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
                            if i_c["Документ"]["Тип"].lower() == 'докотгрисх' or i_c["Тип"].lower() == 'докотгрисх':
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
                
             
                try:
                    attachments_id = {}
                    try:
                        for l in range(len(str_to_dict_points["result"]["ВложениеУчета"])):
                            if str_to_dict_points["result"]["ВложениеУчета"][l]["Тип"].lower() in ("эдосч", "эдоcч"):
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
                            send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                            print('_____________')   
                
                except:
                    
                    attachments_id = {}
                    try:
                        for l in range(len(str_to_dict_points["result"]["Вложение"])):
                            if str_to_dict_points["result"]["Вложение"][l]["Тип"].lower() in ("эдосч", "эдоcч"):
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
                                            
                        if  b == "эдосч":
                    
                            a = requests.get(attachments_id["эдосч"], headers=headers)
                            a.encoding = "cp1251"
                            
                            try:
                                xml_a = xmltodict.parse(a.text)
                                var_inside_doc_type = "эдосч"
                                
                                var_l_try_0 = False
                                var_l_try_1 = False
                                
                                try:
                                    try_0 = xml_a["Файл"]["Документ"]["СодСч"]
                                    if try_0:
                                        var_l_try_0 = True 
                                except:
                                    pass
                                
                                try:
                                    try_1 = xml_a["Файл"]["Документ"]["ТаблСчет"]
                                    if try_1:
                                        var_l_try_1 = True 
                                except:
                                    pass
                                
                                if var_l_try_0 == True:
                                    try:
                                        def_edo_invoice(xml_a, var_inside_doc_author, var_inside_doc_type)
                                    except Exception as e:
                                        print('func def_edo_invoice:', e)
                                        print(var_link, var_doc_number)
                                        print('_____________') 
                                
                                elif var_l_try_1 == True:
                                    try:
                                        def_edo_invoice_type2(xml_a, var_inside_doc_author, var_inside_doc_type)
                                    except Exception as e:
                                        print('func def_edo_invoice_type2:', e)
                                        print(var_link, var_doc_number)
                                        print('_____________') 
                                
                                else:
                                    print('New type of invoce:')
                                    print(var_link, var_doc_number)
                                    print('_____________') 
                                
                            except:
                                print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                                try:                                
                                    send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                                    print('Error in XML. Sent in sbis')
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
    
    doc_notes,
    doc_revision_of_notes,

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
    
    # inside_doc_item_note,
    
    inside_doc_item_code,
    inside_doc_item_article,
    inside_doc_item_name,
    
    inside_doc_item_quantity,
    inside_doc_item_unit,
    
    inside_doc_item_price,
    inside_doc_item_full_item_price,
    
    inside_doc_item_type_agreement,
    inside_doc_item_validity,

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
                  
        "doc_notes",
        "doc_revision_of_notes",

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
        
        # "inside_doc_item_note",
        
        "inside_doc_item_code",
        "inside_doc_item_article",
        "inside_doc_item_name",
        
        "inside_doc_item_quantity",
        "inside_doc_item_unit",
        
        "inside_doc_item_price",
        "inside_doc_item_full_item_price",
        
        "inside_doc_item_type_agreement",
        "inside_doc_item_validity",

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
    
    doc_notes,
    doc_revision_of_notes,

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
    
    # inside_doc_item_note,
    
    inside_doc_item_code,
    inside_doc_item_article,
    inside_doc_item_name,
    
    inside_doc_item_quantity,
    inside_doc_item_unit,
    
    inside_doc_item_price,
    inside_doc_item_full_item_price,

    inside_doc_item_type_agreement,
    inside_doc_item_validity,

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
        df.to_sql(name=f'{name_unloading}', con=my_conn, schema="invoices", if_exists="replace")
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
        df_exc.to_sql(name=f'{name_unloading_exc}', con=my_conn, schema="invoices", if_exists="replace")
        print("df_exc.sent()")
        my_conn.close()
        print("my_conn.close()")
    except:
        print('failed')
        print('my_conn.failed()') 


# sbis_real_processing_0('31.03.2026', '31.03.2026', 'TEST_INVOCE_new_api_sbis', 'TEST_INVOCE_new_api_sbis_exc')