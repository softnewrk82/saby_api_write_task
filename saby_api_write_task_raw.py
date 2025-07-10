
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

import warnings
warnings.simplefilter("ignore")

import numpy as np 

import pendulum

date_now = datetime.datetime.now().date()

# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ____________________________________________________________________________
# ____________________________________________________________________________
# ____________________________________________________________________________


import pandas as pd

import requests
import json

from functools import lru_cache

import warnings
warnings.simplefilter("ignore")

import importlib
import requests

import re

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, insert, update, Date, Numeric

from dateutil.relativedelta import relativedelta
import random
import string as st

import uuid

import base64

import datetime

import modules.api_info
importlib.reload(modules.api_info)

from modules.api_info import var_encryptvar_API_sbis
from modules.api_info import var_encrypt_API_sbis_pass
from modules.api_info import var_encrypt_url_sbis
from modules.api_info import var_encrypt_url_sbis_unloading

from modules.api_info import f_decrypt, load_key_external


API_sbis = f_decrypt(var_encryptvar_API_sbis, load_key_external()).decode("utf-8")
API_sbis_pass = f_decrypt(var_encrypt_API_sbis_pass, load_key_external()).decode("utf-8")
url_sbis = f_decrypt(var_encrypt_url_sbis, load_key_external()).decode("utf-8")
url_sbis_unloading = f_decrypt(var_encrypt_url_sbis_unloading, load_key_external()).decode("utf-8")

# _____________________________________
# _____________________________________
# _____________________________________
responsible_face = 'Таран'
main_face = 'Козлова'
# _____________________________________
# _____________________________________
# _____________________________________



local_tz = pendulum.timezone("Europe/Moscow")


default_arguments = {
    'owner': 'evgenijgrinev',
}


with DAG(
    'SBIS_buh_set_and_update_dept_task',
    schedule_interval='0 9 * * *',
    # schedule_interval='@once',
    catchup=False,
    default_args=default_arguments,
    start_date=pendulum.datetime(2024,7,1, tz=local_tz),
) as dag:

    def py_upl_script():


        try:
            def get_random_cyrillic_letter():
            """Возвращает случайную букву кириллицы."""
            # cyrillic_alphabet = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"
            cyrillic_alphabet = "абвгдежзийклмнопрстуфхцчшэюя"
            return random.choice(cyrillic_alphabet)

            random_letter = get_random_cyrillic_letter()
            # print(random_letter.upper())

            @lru_cache
            def auth(url_sbis, API_sbis, API_sbis_pass):
                
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
                # print("access_token:", access_token)
                
                headers = {
                "X-SBISSessionID": access_token,
                "Content-Type": "application/json",
                }  

                return headers

            headers = str(auth(url_sbis, API_sbis, API_sbis_pass))

            @lru_cache
            def common_info(var_last_name, var_first_name, var_surname, var_external_id, var_describe):

                # ______________________________________________________
                str_myuuid = str(uuid.uuid4())
                str_myuuid_attached = str(uuid.uuid4())
                # ______________________________________________________
                @lru_cache
                def generate_random_alphanumeric(length_l, length_d):
                    characters = st.ascii_letters
                    digits = st.digits
                    var_l = length_l

                    cyrillic_alphabet = "абвгдежзийклмнопрстуфхцчшэюя"
                    var_set = random.choice(cyrillic_alphabet.upper()), random.choice(cyrillic_alphabet.upper())
                    random_string = ''.join(var_set)          
                    
                    random_digits = ''.join(random.choice(digits) for _ in range(length_d))
                    num_task = f"{random_string}-{random_digits}"
                    return random_string, random_digits, num_task
            
                random_alphanumeric_string, random_digits, num_task = generate_random_alphanumeric(2, 6)
                # ______________________________________________________
                
                var_date_from = datetime.datetime.now().date()
                days = 1

            # ______________________________________________
                def add_business_days(start_date, days):
                    """
                    Добавляет указанное количество рабочих дней к дате, учитывая выходные.
                
                    Args:
                        start_date: Начальная дата (datetime.date или datetime.datetime).
                        days: Количество рабочих дней для добавления.
                
                    Returns:
                        Дата после добавления рабочих дней.
                    """
                    business_days = 0
                    current_date = start_date
                    while business_days < days:
                        current_date += relativedelta(days=1)
                        if current_date.weekday() < 5:  # 0-4 это будние дни (пн-пт)
                            business_days += 1
                    return current_date

                def subtract_business_days(start_date, days):
                    """
                    Вычитает указанное количество рабочих дней из даты, учитывая выходные.
                
                    Args:
                        start_date: Начальная дата (datetime.date или datetime.datetime).
                        days: Количество рабочих дней для вычитания.
                
                    Returns:
                        Дата после вычитания рабочих дней.
                    """
                    business_days = 0
                    current_date = start_date
                    while business_days < days:
                        current_date -= relativedelta(days=1)
                        if current_date.weekday() < 5:  # 0-4 это будние дни (пн-пт)
                            business_days += 1
                    return current_date
                
                # Добавляем n рабочих дней
                var_date_to = add_business_days(var_date_from, days)
                # print(f"Через {days} рабочих дней: {var_date_to}")
                
                # Вычитаем n рабочих дня
                var_date_prev = subtract_business_days(var_date_from, days)
                # print(f"{days} рабочих дня назад: {var_date_prev}")

                date_from = var_date_from.strftime("%d.%m.%Y")
                date_to = var_date_to.strftime("%d.%m.%Y")   
            # ______________________________________________

                
                var_describe = var_describe
                
                var_last_name = var_last_name
                var_first_name = var_first_name
                var_surname = var_surname
                
                var_external_id = var_external_id
                
                var_sluzhzap = f"ON_SLUZHZAP_{str(var_date_from).replace('-', '')}_{str_myuuid_attached}"
                
                # ___________________________________________________
                
                with open("./test_xml_saby_task.xml", "rb") as file:
                    encoded = base64.encodebytes(file.read()).decode("utf-8")
                
                decoded = base64.decodebytes(encoded.encode('utf-8'))
                str_decoded = decoded.decode('utf-8')
                
                str_decoded = str_decoded.replace("date_from", date_from)
                str_decoded = str_decoded.replace("var_sluzhzap", var_sluzhzap)
                str_decoded = str_decoded.replace("num_task", num_task)
                str_decoded = str_decoded.replace("date_to", date_to)
                str_decoded = str_decoded.replace("var_describe", var_describe)
                str_decoded = str_decoded.replace("var_first_name", var_first_name)
                str_decoded = str_decoded.replace("var_external_id", var_external_id)
                str_decoded = str_decoded.replace("var_surname", var_surname)
                str_decoded = str_decoded.replace("var_last_name", var_last_name)
                
                str_encode = str_decoded.encode('utf-8')
                
                with open(f"./temp/{str_myuuid_attached} {var_last_name} {var_first_name} {var_surname}.xml", "wb") as file:
                    file.write(str_encode)
                
                with open(f"./temp/{str_myuuid_attached} {var_last_name} {var_first_name} {var_surname}.xml", "rb") as file:
                    encoded_attached = base64.encodebytes(file.read()).decode("utf-8")

                return str_myuuid, str_myuuid_attached, num_task, date_from, var_sluzhzap, date_to, var_describe, encoded_attached



            @lru_cache
            def write(var_external_id, reverse_var_float, date_from, num_task, str_myuuid, date_to, var_last_name, var_last_name_responsible, var_first_name, var_first_name_responsible, var_surname, var_surname_responsible, str_myuuid_attached, var_sluzhzap, encoded_attached, headers):
                
                parameters_real = {
                "jsonrpc": "2.0",
                "method": "СБИС.ЗаписатьДокумент",
                "params": {
                    "Документ": {
                    "Дата": f"{date_from}",
                    "Номер": num_task,
                    "Идентификатор": str_myuuid,
                    "Тип": "СлужЗап",
                    "Этап": [
                                {
                                "Действие": [
                                    {
                                    "Комментарий": "",
                                    "Название": "Выполнение",
                                    }
                                ],
                    'Название': 'Выполнение',
                                }
                    ],
                    "Срок": f"{date_to}",
                    "Регламент": {
                        "Название": "Задача"
                    },
                    "Ответственный": {
                        "Фамилия": f"{var_last_name_responsible}",
                        "Имя": f"{var_first_name_responsible}",
                        "Отчество": f"{var_surname_responsible}"
                    },
                    "Вложение": [
                        {
                        "Идентификатор": str_myuuid_attached,
                        "Файл": {
                            "Имя": f"{var_sluzhzap}.xml",
                            "ДвоичныеДанные": f"{encoded_attached}"
                        }
                        }
                    ]
                    }
                },
                "id": 0
                }
                url_real = 'https://online.sbis.ru/service/?srv=1'
                
                response_points = requests.post(url_real, json=parameters_real, headers=json.loads(headers))
                str_to_dict_points = json.loads(response_points.text)

                # print(str_to_dict_points)
                
                
                if str_to_dict_points["result"]["Автор"]:
                    # print(True)
                    var_date_report = datetime.datetime.now().date() - relativedelta(months=1)
                    var_date_report_clear = datetime.date(var_date_report.year, var_date_report.month, 1)

                    engine = create_engine("postgresql+psycopg2://da:qa123@10.82.2.30:5432/warehouse")
                    metadata = MetaData()
                    user_table = Table(
                        'saby_api_task_send', metadata,
                        Column('period', Date),
                        Column('ext_id', String),
                        Column('num', String),
                        Column('var_external_id', String),
                        Column('var_last_name', String),
                        Column('var_first_name', String),
                        Column('var_surname', String),
                        Column('cash', Numeric),
                        schema = 'saby_api',
                    )
                    metadata.create_all(engine) # Create the table if it doesn't exist
                    
                    # 2. Construct the INSERT statement
                    stmt = insert(user_table).values(var_external_id=var_external_id, cash=reverse_var_float, period=var_date_report_clear, ext_id=str_myuuid, num=num_task, var_last_name=var_last_name, var_first_name=var_first_name, var_surname=var_surname)
                    
                    # 3. Execute the statement
                    with engine.connect() as conn:
                        result = conn.execute(stmt)
                        conn.commit()
                        print(f"inserted {result}")

                else: 
                    pass

            @lru_cache
            def execute(str_myuuid, headers, num_task):
                parameters_real = {
                "jsonrpc": "2.0",
                "method": "СБИС.ВыполнитьДействие",
                "params": {
                    "Документ": {
                    "Идентификатор":str_myuuid,
                    "Этап": {
                        "Название": "На выполнение"
                    }
                    }
                },
                "id": 0
                }
                url_real = 'https://online.sbis.ru/service/?srv=1'
            
                response_points = requests.post(url_real, json=parameters_real, headers=json.loads(headers))
                str_to_dict_points = json.loads(response_points.text)
                # print(str_to_dict_points)

                # _______________________________________
                if str_to_dict_points["result"]["Идентификатор"]:
                    engine = create_engine("postgresql+psycopg2://da:qa123@10.82.2.30:5432/warehouse")
                    metadata = MetaData()
                    user_table = Table(
                        'saby_api_task_send', metadata,
                        Column('period', Date),
                        Column('ext_id', String),
                        Column('num', String),
                        Column('var_external_id', String),
                        Column('var_last_name', String),
                        Column('var_first_name', String),
                        Column('var_surname', String),
                        Column('send', String),
                        Column('check_execute', String),
                        Column('cash', Numeric),
                        schema = 'saby_api',
                    )
                    
                    # 2. Construct the UPDATE statement
                    stmt = update(user_table).where(user_table.c.ext_id == str_myuuid).values(check_execute="true")

                    # 3. Execute the statement
                    with engine.connect() as conn:
                        result = conn.execute(stmt)
                        conn.commit()
                        print(f"updated {result}")
                        print('_________________')

            var_date_report = datetime.datetime.now().date() - relativedelta(months=1)
            var_date_report_clear = datetime.date(var_date_report.year, var_date_report.month, 1)

            if len(str(var_date_report.month)) == 1:
                var_month = '0' + str(var_date_report.month)
            else: 
                var_month = var_date_report.month

            df_debt = pd.read_excel(f'./Выгрузки/Долг сотрудник {var_date_report_clear.year}-{var_month}.xlsx', header=7)
            df_debt_red = df_debt[[
                "Unnamed: 3",
                "Незавершенные операции на конец периода"
            ]]

            df_debt_red_fil = df_debt_red[2:].reset_index(drop=True)
            df_debt_red_fil = df_debt_red_fil.rename(columns={"Unnamed: 3": "ФИО"})
            df_debt_red_fil["var_last_name"] = df_debt_red_fil["ФИО"].apply(lambda x: x.split(' ')[0])
            df_debt_red_fil["var_first_name"] = df_debt_red_fil["ФИО"].apply(lambda x: x.split(' ')[1])


            sql_query = f"""
                select * from common_info.sbis_list_emp
            """

            my_conn = create_engine("postgresql+psycopg2://da:qa123@10.82.2.30:5432/softum")
            try: 
                my_conn.connect()
                # print('connect')
                my_conn = my_conn.connect()
                sbis_list_emp = pd.read_sql(sql=sql_query, con=my_conn)
                print('common_info.sbis_list_emp success!')
                my_conn.close()
            except:
                print('common_info.sbis_list_emp failed')

            sql_query = f"""
                select * from saby_api.saby_api_task_send
            """

            my_conn = create_engine("postgresql+psycopg2://da:qa123@10.82.2.30:5432/warehouse")
            try: 
                my_conn.connect()
                # print('connect')
                my_conn = my_conn.connect()
                saby_api_task_summary = pd.read_sql(sql=sql_query, con=my_conn)
                print('saby_api.saby_api_task_send success!')
                my_conn.close()
            except:
                print('saby_api.saby_api_task_send failed')

            df_merge = df_debt_red_fil.merge(sbis_list_emp[[
                "var_last_name",
                "var_first_name",
                "var_surname",
                "var_full_name",
                'Ссылка на карточку',
            ]], how='left', left_on=["var_last_name", "var_first_name"], right_on=["var_last_name", "var_first_name"])

            var_date_report = datetime.datetime.now().date() - relativedelta(months=1)
            var_date_report_clear = datetime.date(var_date_report.year, var_date_report.month, 1)
            df_merge_na = df_merge[~df_merge["Ссылка на карточку"].isna()]
            df_merge_na["var_external_id"] = df_merge_na["Ссылка на карточку"].apply(lambda x: x.split('/')[-1])
            df_merge_na["Незавершенные операции на конец периода"] = df_merge_na["Незавершенные операции на конец периода"].astype(float)
            df_merge_na["Период"] = var_date_report_clear
            df_merge_na.head()

            df_merge = df_merge_na

            select_responsible_user = sbis_list_emp[(sbis_list_emp["var_last_name"] == responsible_face) & (sbis_list_emp["var_first_name"] != 'Сотрудник')]
            var_r_u_last_name = select_responsible_user["var_last_name"].iloc[-1]
            var_r_u_first_name = select_responsible_user["var_first_name"].iloc[-1]
            var_r_u_surname = select_responsible_user["var_surname"].iloc[-1]

            select_main_user = sbis_list_emp[(sbis_list_emp["var_last_name"] == main_face) & (sbis_list_emp["var_first_name"] != 'Сотрудник')]
            var_m_u_last_name = select_main_user["var_last_name"].iloc[-1]
            var_m_u_first_name = select_main_user["var_first_name"].iloc[-1]
            var_m_u_surname = select_main_user["var_surname"].iloc[-1]

            var_last_name_responsible = var_r_u_last_name
            var_first_name_responsible = var_r_u_first_name
            var_surname_responsible = var_r_u_surname

            df_merge_fil = df_merge[~df_merge["Ссылка на карточку"].isna()]
            df_merge_na = df_merge[df_merge["Ссылка на карточку"].isna()]

            for i in range(len(df_merge_fil["var_external_id"])):

                var_float = df_merge_fil["Незавершенные операции на конец периода"].iloc[i]
                reverse_var_float = -var_float
                
                var_last_name = df_merge_fil["var_last_name"].iloc[i]
                var_first_name = df_merge_fil["var_first_name"].iloc[i]
                var_surname = df_merge_fil["var_surname"].iloc[i]
                var_external_id = df_merge_fil["var_external_id"].iloc[i]
                var_period = df_merge_fil["Период"].iloc[i]


                if len(saby_api_task_summary[(saby_api_task_summary["var_external_id"] == var_external_id) & (saby_api_task_summary["period"] == var_date_report_clear) & (saby_api_task_summary["cash"] == reverse_var_float)]) > 0:
                    pass
                else:
                    if var_float > 0:
                        var_describe = f"Зайти в бухгалтерию. Занести деньги в кассу: {abs(reverse_var_float)} руб. за отчетный период {var_date_report_clear.year}-{var_month}. \n В случае возникновения вопросов по сальдо - уточняйте информацию у кассира"
                    elif var_float < 0:
                        var_describe = f"Зайти в бухгалтерию. Забрать деньги из кассы: {abs(reverse_var_float)} руб. за отчетный период {var_date_report_clear.year}-{var_month}.  \n В случае возникновения вопросов по сальдо - уточняйте информацию у кассира"
                
                    headers = str(auth(url_sbis, API_sbis, API_sbis_pass)).replace("'", '"')
                    str_myuuid, str_myuuid_attached, num_task, date_from, var_sluzhzap, date_to, var_describe, encoded_attached = common_info(var_last_name, var_first_name, var_surname, var_external_id, var_describe)
                    write(var_external_id, reverse_var_float, date_from, num_task, str_myuuid, date_to, var_last_name, var_last_name_responsible, var_first_name, var_first_name_responsible, var_surname, var_surname_responsible, str_myuuid_attached, var_sluzhzap, encoded_attached, headers)
                    # headers = str(auth(url_sbis, API_sbis, API_sbis_pass)).replace("'", '"')
                    # try:
                    execute(str_myuuid, headers, num_task)
                    # except:
                        # pass

            print(var_r_u_last_name)
            print(var_r_u_first_name)
            print(var_r_u_surname)

            print(var_m_u_last_name)
            print(var_m_u_first_name)
            print(var_m_u_surname)

            @lru_cache
            def common_info_main_task(date_from, date_to, str_myuuid, str_myuuid_attached, var_sluzhzap, num_task, var_r_u_last_name, var_r_u_first_name, var_r_u_surname, var_external_id, var_describe):

                # ______________________________________________________
                # if str_myuuid == '':
                str_myuuid = str(uuid.uuid4())
                # else:
                    # pass
                    
                if str_myuuid_attached == '':
                    str_myuuid_attached = str(uuid.uuid4())
                else: 
                    pass
                # ______________________________________________________
                # @lru_cache
                # def generate_random_alphanumeric(length_l, length_d):
                #     characters = st.ascii_letters
                #     digits = st.digits
                #     random_string = ''.join(random.choice(characters) for _ in range(length_l)).upper()
                #     random_digits = ''.join(random.choice(digits) for _ in range(length_d))
                #     num_task = f"{random_string}-{random_digits}"
                #     return random_string, random_digits, num_task

                # ______________________________________________________
                @lru_cache
                def generate_random_alphanumeric(length_l, length_d):
                    characters = st.ascii_letters
                    digits = st.digits
                    var_l = length_l

                    cyrillic_alphabet = "абвгдежзийклмнопрстуфхцчшэюя"
                    var_set = random.choice(cyrillic_alphabet.upper()), random.choice(cyrillic_alphabet.upper())
                    random_string = ''.join(var_set)          
                    
                    random_digits = ''.join(random.choice(digits) for _ in range(length_d))
                    num_task = f"{random_string}-{random_digits}"
                    return random_string, random_digits, num_task

                if num_task == '':
                    random_alphanumeric_string, random_digits, num_task = generate_random_alphanumeric(2, 6)
                else: 
                    pass

                
                # ______________________________________________________
                
                var_date_from = datetime.datetime.now().date()
                days = 1
                # var_date_to = var_date_from + relativedelta(days=1)
                # date_from = var_date_from.strftime("%d.%m.%Y")
                # date_to = var_date_to.strftime("%d.%m.%Y")


            # ______________________________________________
                def add_business_days(start_date, days):
                    """
                    Добавляет указанное количество рабочих дней к дате, учитывая выходные.
                
                    Args:
                        start_date: Начальная дата (datetime.date или datetime.datetime).
                        days: Количество рабочих дней для добавления.
                
                    Returns:
                        Дата после добавления рабочих дней.
                    """
                    business_days = 0
                    current_date = start_date
                    while business_days < days:
                        current_date += relativedelta(days=1)
                        if current_date.weekday() < 5:  # 0-4 это будние дни (пн-пт)
                            business_days += 1
                    return current_date

                def subtract_business_days(start_date, days):
                    """
                    Вычитает указанное количество рабочих дней из даты, учитывая выходные.
                
                    Args:
                        start_date: Начальная дата (datetime.date или datetime.datetime).
                        days: Количество рабочих дней для вычитания.
                
                    Returns:
                        Дата после вычитания рабочих дней.
                    """
                    business_days = 0
                    current_date = start_date
                    while business_days < days:
                        current_date -= relativedelta(days=1)
                        if current_date.weekday() < 5:  # 0-4 это будние дни (пн-пт)
                            business_days += 1
                    return current_date
                
                # Пример использования
                # print(f"Сегодня: {var_date_from}")
                # print(f"Дней: {days}")

                # Добавляем n рабочих дней
                var_date_to = add_business_days(var_date_from, days)
                # print(f"Через {days} рабочих дней: {var_date_to}")
                
                # Вычитаем n рабочих дня
                var_date_prev = subtract_business_days(var_date_from, days)
                # print(f"{days} рабочих дня назад: {var_date_prev}")

                if date_from == '':
                    date_from = var_date_from.strftime("%d.%m.%Y")
                else:
                    var_date_from = date_from

                if date_to == '':
                    date_to = var_date_to.strftime("%d.%m.%Y")   
                else:
                    date_to = var_date_to.strftime("%d.%m.%Y")
            # ______________________________________________

                
                var_describe = var_describe
                
                var_last_name = var_r_u_last_name
                var_first_name = var_r_u_first_name
                var_surname = var_r_u_surname
                
                # var_external_id = 'c943a420-f494-4a38-8975-d9db61c3dba7'
                # https://online.sbis.ru/person/c943a420-f494-4a38-8975-d9db61c3dba7
                # var_external_id = 'c943a420-f494-4a38-8975-d9db61c3dba7'
                var_external_id = var_external_id
                if var_sluzhzap == '':
                    var_sluzhzap = f"ON_SLUZHZAP_{str(var_date_from).replace('-', '')}_{str_myuuid_attached}"
                else:
                    pass
                
                # ___________________________________________________
                
                with open("./test_xml_saby_task_main.xml", "rb") as file:
                    encoded = base64.encodebytes(file.read()).decode("utf-8")
                
                decoded = base64.decodebytes(encoded.encode('utf-8'))
                str_decoded = decoded.decode('utf-8')

                
                str_decoded = str_decoded.replace("date_from", date_from)
                str_decoded = str_decoded.replace("var_sluzhzap", var_sluzhzap)
                str_decoded = str_decoded.replace("num_task", num_task)
                str_decoded = str_decoded.replace("date_to", date_to)
                str_decoded = str_decoded.replace("var_describe", var_describe)
                str_decoded = str_decoded.replace("var_first_name", var_first_name)
                str_decoded = str_decoded.replace("var_external_id", var_external_id)
                str_decoded = str_decoded.replace("var_surname", var_surname)
                str_decoded = str_decoded.replace("var_last_name", var_last_name)
                
                str_encode = str_decoded.encode('utf-8')
                
                with open(f"./temp/{str_myuuid_attached} {var_last_name} {var_first_name} {var_surname}.xml", "wb") as file:
                    file.write(str_encode)
                
                with open(f"./temp/{str_myuuid_attached} {var_last_name} {var_first_name} {var_surname}.xml", "rb") as file:
                    encoded_attached = base64.encodebytes(file.read()).decode("utf-8")

                return str_myuuid, str_myuuid_attached, num_task, date_from, var_sluzhzap, date_to, var_describe, encoded_attached


            @lru_cache
            def write_check_task(var_status_main_cycle, var_external_id, date_from, num_task, str_myuuid, date_to, var_r_u_last_name, var_m_u_last_name, var_r_u_first_name, var_m_u_first_name, var_r_u_surname, var_m_u_surname, str_myuuid_attached, var_sluzhzap, encoded_attached, headers):
                
                parameters_real = {
                "jsonrpc": "2.0",
                "method": "СБИС.ЗаписатьДокумент",
                "params": {
                    "Документ": {
                    "Дата": f"{date_from}",
                    "Номер": num_task,
                    "Идентификатор": str_myuuid,
                    "Тип": "СлужЗап",
                    "Этап": [
                                {
                                "Действие": [
                                    {
                                    "Комментарий": "",
                                    "Название": "Выполнение",
                                    }
                                ],
                    'Название': 'Выполнение',
                                }
                    ],
                    "Срок": f"{date_to}",
                    "Регламент": {
                        "Название": "Задача"
                    },
                    "Ответственный": {
                        "Фамилия": f"{var_m_u_last_name}",
                        "Имя": f"{var_m_u_first_name}",
                        "Отчество": f"{var_m_u_surname}"
                    },
                    "Вложение": [
                        {
                        "Идентификатор": str_myuuid_attached,
                        "Файл": {
                            "Имя": f"{var_sluzhzap}.xml",
                            "ДвоичныеДанные": f"{encoded_attached}"
                        }
                        }
                    ]
                    }
                },
                "id": 0
                }
                url_real = 'https://online.sbis.ru/service/?srv=1'
                
                response_points = requests.post(url_real, json=parameters_real, headers=json.loads(headers))
                str_to_dict_points = json.loads(response_points.text)

                # print(str_to_dict_points)
                
                if var_status_main_cycle == True:

                    var_date_report = datetime.datetime.now().date() - relativedelta(months=1)
                    var_date_report_clear = datetime.date(var_date_report.year, var_date_report.month, 1)

                    engine = create_engine("postgresql+psycopg2://da:qa123@10.82.2.30:5432/warehouse")
                    metadata = MetaData()
                    user_table = Table(
                        'saby_api_task_common_info', metadata,
                        Column('period', Date),
                        Column('var_sluzhzap', String),
                        Column('str_myuuid_attached', String),
                        Column('ext_id', String),
                        Column('num', String),
                        Column('date_to', String),
                        Column('date_from', String),
                        Column('var_external_id', String),
                        Column('var_last_name', String),
                        Column('var_first_name', String),
                        Column('var_surname', String),
                        schema = 'saby_api',
                    )
                    metadata.create_all(engine) # Create the table if it doesn't exist
                    
                    # 2. Construct the INSERT statement
                    # stmt = update(user_table).values(date_to = date_to, date_from = date_from, var_sluzhzap = var_sluzhzap, str_myuuid_attached = str_myuuid_attached, var_external_id=var_external_id, period=var_date_report_clear, ext_id=str_myuuid, num=num_task, var_last_name=var_last_name, var_first_name=var_first_name, var_surname=var_surname)
                    stmt = update(user_table).where(user_table.c.period == var_date_report_clear).values(ext_id=str_myuuid, date_to=date_to)
                    # 3. Execute the statement
                    with engine.connect() as conn:
                        result = conn.execute(stmt)
                        conn.commit()
                        print(f"inserted {result}")
                
                else:
                    if str_to_dict_points["result"]["Автор"]:
                        # print(True)
                        var_date_report = datetime.datetime.now().date() - relativedelta(months=1)
                        var_date_report_clear = datetime.date(var_date_report.year, var_date_report.month, 1)
                
                        engine = create_engine("postgresql+psycopg2://da:qa123@10.82.2.30:5432/warehouse")
                        metadata = MetaData()
                        user_table = Table(
                            'saby_api_task_common_info', metadata,
                            Column('period', Date),
                            Column('var_sluzhzap', String),
                            Column('str_myuuid_attached', String),
                            Column('ext_id', String),
                            Column('num', String),
                            Column('date_to', String),
                            Column('date_from', String),
                            Column('var_external_id', String),
                            Column('var_last_name', String),
                            Column('var_first_name', String),
                            Column('var_surname', String),
                            schema = 'saby_api',
                        )
                        metadata.create_all(engine) # Create the table if it doesn't exist
                        
                        # 2. Construct the INSERT statement
                        stmt = insert(user_table).values(date_to = date_to, date_from = date_from, var_sluzhzap = var_sluzhzap, str_myuuid_attached = str_myuuid_attached, var_external_id=var_external_id, period=var_date_report_clear, ext_id=str_myuuid, num=num_task, var_last_name=var_m_u_last_name, var_first_name=var_m_u_first_name, var_surname=var_m_u_surname)
                        
                        # 3. Execute the statement
                        with engine.connect() as conn:
                            result = conn.execute(stmt)
                            conn.commit()
                            print(f"inserted {result}")
                
                    else: 
                        pass

            @lru_cache
            def execute_main_task(var_status_main_cycle, str_myuuid, headers, num_task):
                parameters_real = {
                "jsonrpc": "2.0",
                "method": "СБИС.ВыполнитьДействие",
                "params": {
                    "Документ": {
                    "Идентификатор":str_myuuid,
                    "Этап": {
                        "Название": "На выполнение"
                    }
                    }
                },
                "id": 0
                }
                url_real = 'https://online.sbis.ru/service/?srv=1'
            
                response_points = requests.post(url_real, json=parameters_real, headers=json.loads(headers))
                str_to_dict_points = json.loads(response_points.text)
                # print(str_to_dict_points)

                # _______________________________________
                if var_status_main_cycle == True:
                    pass
                else:
                    if str_to_dict_points["result"]["Идентификатор"]:
                        engine = create_engine("postgresql+psycopg2://da:qa123@10.82.2.30:5432/warehouse")
                        metadata = MetaData()
                        user_table = Table(
                            'saby_api_task_common_info', metadata,
                            Column('period', Date),
                            Column('var_sluzhzap', String),
                            Column('ext_id', String),
                            Column('num', String),
                            Column('var_external_id', String),
                            Column('var_last_name', String),
                            Column('var_first_name', String),
                            Column('var_surname', String),
                            Column('send', String),
                            Column('check_execute', String),
                            Column('cash', Numeric),
                            schema = 'saby_api',
                        )
                        
                        # 2. Construct the UPDATE statement
                        stmt = update(user_table).where(user_table.c.ext_id == str_myuuid).values(check_execute="true")
                
                        # 3. Execute the statement
                        with engine.connect() as conn:
                            result = conn.execute(stmt)
                            conn.commit()
                            print(f"updated {result}")
                            print('_________________')

            sql_query = f"""
                select * from saby_api.saby_api_task_send
            """

            my_conn = create_engine("postgresql+psycopg2://da:qa123@10.82.2.30:5432/warehouse")
            try: 
                my_conn.connect()
                # print('connect')
                my_conn = my_conn.connect()
                saby_api_task_summary = pd.read_sql(sql=sql_query, con=my_conn)
                print('saby_api.saby_api_task_send success!')
                my_conn.close()
            except:
                print('saby_api.saby_api_task_send failed')

            lst_unique_tasks_id_by_report_period = saby_api_task_summary[saby_api_task_summary["period"] == var_date_report_clear]["ext_id"].unique()
            lst_unique_tasks_id_by_report_period

            lst_success_tasks = []
            lst_in_process_tasks = []
            lst_links_in_process_tasks = []

            for i_link in lst_unique_tasks_id_by_report_period:
                temp_dict = {}
                
                headers = auth(url_sbis, API_sbis, API_sbis_pass)
                
                var_link = i_link
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
                
                response_points = requests.post(url_sbis_unloading, json=parameters_real, headers=headers)
                str_to_dict_points = json.loads(response_points.text)

                
                var_doc_link = str_to_dict_points["result"]["СсылкаДляНашаОрганизация"]
                var_doc_id = str_to_dict_points["result"]["Идентификатор"]
                var_doc_num = str_to_dict_points["result"]["Номер"]
                var_doc_code = str_to_dict_points["result"]["Состояние"]["Код"]
                var_doc_status = str_to_dict_points["result"]["Состояние"]["Название"]
                var_doc_removed = str_to_dict_points["result"]["Удален"]

                temp_dict["СсылкаДляНашаОрганизация"] = var_doc_link
                temp_dict["var_doc_id"] = var_doc_id
                temp_dict["var_doc_num"] = var_doc_num
                temp_dict["var_doc_code"] = var_doc_code
                temp_dict["var_doc_status"] = var_doc_status
                temp_dict["var_doc_removed"] = var_doc_removed

                if var_doc_code == '7':
                    lst_success_tasks.append(temp_dict)
                else:
                    lst_links_in_process_tasks.append(' ' + var_doc_link + ' ')
                    # lst_links_in_process_tasks.append('<Ссылка ' + var_doc_link + f'></Ссылка>')
                    lst_in_process_tasks.append(temp_dict)

            # clear_lst_links = f"; \n ".join(lst_links_in_process_tasks)
            clear_lst_links = f" \n ".join(lst_links_in_process_tasks).replace('&', '&amp;')
            # clear_lst_links = lst_links_in_process_tasks

            var_describe = f"""
            Кол-во задач с состоянием 'Выполнение завершено успешно': {len(lst_success_tasks)}
            __________________________________________________________________________
            Кол-во задач с состоянием 'В обработке': {len(lst_in_process_tasks)}
            Список задач:
            {clear_lst_links}
            """

            sql_query = f"""
                select * from saby_api.saby_api_task_common_info
            """

            my_conn = create_engine("postgresql+psycopg2://da:qa123@10.82.2.30:5432/warehouse")
            try: 
                my_conn.connect()
                # print('connect')
                my_conn = my_conn.connect()
                saby_api_task_common = pd.read_sql(sql=sql_query, con=my_conn)
                print('saby_api.saby_api_task_common_info success!')
                my_conn.close()
            except:
                print('saby_api.saby_api_task_common_info failed')

            check_main_task = saby_api_task_common[saby_api_task_common["period"] == var_date_report_clear]
            check_main_task

            if len(check_main_task) != 0:
                var_status_main_cycle = True
                print(var_status_main_cycle)
                
                headers = str(auth(url_sbis, API_sbis, API_sbis_pass)).replace("'", '"')
                str_myuuid = check_main_task["ext_id"].iloc[0] 
                print(str_myuuid)
                str_myuuid_attached = check_main_task["str_myuuid_attached"].iloc[0] 
                num_task = check_main_task["num"].iloc[0] 
                var_sluzhzap = check_main_task["var_sluzhzap"].iloc[0] 
                date_from = check_main_task["date_to"].iloc[0] 
                date_to = check_main_task["date_from"].iloc[0] 

                @lru_cache
                def del_task(str_myuuid, headers):
                    headers = str(auth(url_sbis, API_sbis, API_sbis_pass)).replace("'", '"')
                    parameters_real = {
                    "jsonrpc": "2.0",
                    "method": "СБИС.УдалитьДокумент",
                    "params": {
                        "Документ": {
                            "Идентификатор": str_myuuid
                        }
                    },
                    "id": 0
                    }
                    
                    url_real = 'https://online.sbis.ru/service/?srv=1'
                        
                    response_points = requests.post(url_real, json=parameters_real, headers=json.loads(headers))
                    str_to_dict_points = json.loads(response_points.text)
                    # print(str_to_dict_points)
                
                del_task(str_myuuid, headers)
                
                str_myuuid, str_myuuid_attached, num_task, date_from, var_sluzhzap, date_to, var_describe, encoded_attached = common_info_main_task(date_from, date_to, str_myuuid, str_myuuid_attached, var_sluzhzap, num_task, var_r_u_last_name, var_r_u_first_name, var_r_u_surname, var_external_id, var_describe)

                write_check_task(var_status_main_cycle, var_external_id, date_from, num_task, str_myuuid, date_to, var_r_u_last_name, var_m_u_last_name, var_r_u_first_name, var_m_u_first_name, var_r_u_surname, var_m_u_surname, str_myuuid_attached, var_sluzhzap, encoded_attached, headers)
                execute_main_task(var_status_main_cycle, str_myuuid, headers, num_task)  
            else:
                var_status_main_cycle = False
                print(var_status_main_cycle)
                headers = str(auth(url_sbis, API_sbis, API_sbis_pass)).replace("'", '"')

                str_myuuid = ''
                str_myuuid_attached = ''
                num_task = ''
                var_sluzhzap = ''
                date_from = ''
                date_to = ''
                
                str_myuuid, str_myuuid_attached, num_task, date_from, var_sluzhzap, date_to, var_describe, encoded_attached = common_info_main_task(date_from, date_to, str_myuuid, str_myuuid_attached, var_sluzhzap, num_task, var_r_u_last_name, var_r_u_first_name, var_r_u_surname, var_external_id, var_describe)
                write_check_task(var_status_main_cycle, var_external_id, date_from, num_task, str_myuuid, date_to, var_r_u_last_name, var_m_u_last_name, var_r_u_first_name, var_m_u_first_name, var_r_u_surname, var_m_u_surname, str_myuuid_attached, var_sluzhzap, encoded_attached, headers)
                # headers = str(auth(url_sbis, API_sbis, API_sbis_pass)).replace("'", '"')
                execute_main_task(var_status_main_cycle, str_myuuid, headers, num_task) 
        except: 
            print('error')  

            
    python_upl_script = PythonOperator(
        task_id='python_upl_script',
        python_callable=py_upl_script
    )

python_upl_script