


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
        
        inside_doc_author.append(var_inside_doc_author)
        doc_term.append(var_doc_term)
        doc_sum.append(var_doc_sum)
        
        inside_doc_type.append(var_type)
    
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
    
    inside_doc_type = []

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
   
    doc_term = []
    doc_sum = []
    
    var_doc_type = []

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
            "Тип": "Лид",
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
            if (re.findall("лид", i["Тип"].lower())[-1] == "лид"):
            # if (re.findall("счет", i["Регламент"]["Название"].lower())[-1] == "счет"):
            # if (re.findall("реал", i["Регламент"]["Название"].lower())[-1] == "счет") and (i["Расширение"]["Проведен"].lower() == 'да'):
                
                try:
                    var_link = i["Идентификатор"]
                except:
                    var_link = ''

                try:
                    var_type = i["Тип"]
                except:
                    var_type = ''
                    
    
    
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
                    doc_manager_name = ''
    
                try:
                    var_doc_type = i["Регламент"]["Название"]
                except:
                    var_doc_type = ''
    
                try:
                    var_doc_number = str(i["Номер"])
                except:
                    var_doc_number = ''
    
                try:
                    var_doc_full_name = i["Название"]
                except:
                    var_doc_full_name = ''
    
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
                    var_doc_assigned_manager = ''
                try:
                    var_doc_department = i["Подразделение"]["Название"]
                except:
                    var_doc_department = ''  
                    
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
                            if i_c["Документ"]["Тип"].lower() == 'счетисх' or i_c["Тип"].lower() == 'cчетисх':
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
                    var_doc_term = str_to_dict_points["result"]["Срок"]
                except:
                    var_doc_term = ""                

                try:
                    var_doc_sum = str_to_dict_points["result"]["Сумма"]
                except:
                    var_doc_sum = ""                
                
                
             
                try:
                    doc_append()
                        
                except Exception as e:
                    doc_append_exc()  
                    print(e) 
                    send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                    print('_______________________')    
                                        
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
    doc_term,
    doc_sum,
    inside_doc_type,

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
        "doc_term",
        "doc_sum",
        
        "inside_doc_type",
        

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
    doc_term,
    doc_sum,
    
    inside_doc_type,
    

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
        df.to_sql(name=f'{name_unloading}', con=my_conn, schema="deals", if_exists="replace")
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
        df_exc.to_sql(name=f'{name_unloading_exc}', con=my_conn, schema="deals", if_exists="replace")
        print("df_exc.sent()")
        my_conn.close()
        print("my_conn.close()")
    except:
        print('failed')
        print('my_conn.failed()') 


sbis_real_processing_0('31.03.2026', '31.03.2026', 'TEST_DEAL_new_api_sbis', 'TEST_DEAL_new_api_sbis_exc')