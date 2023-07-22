import csv
import random
import datetime
import tkinter
import uuid
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
import logging
import pymysql
import json
from tkinter import *
from tkinter.ttk import Combobox
import psycopg2


res_path = 'E:\\bigdata\\school\\csvfiles\\'
orig_path = 'E:\\bigdata\\school\\csvfiles_orig\\'
engine = create_engine(url=f"postgresql+psycopg2://alex:0209@192.168.1.75/bet_project", echo=False)
subj_cfg = 'C:\\Users\\alex\\PycharmProjects\\school\\subj_cfg.csv'
connection = engine.raw_connection()

logfile = f'{res_path}process.log'
log = logging.getLogger("my_log")
log.setLevel(logging.INFO)
FH = logging.FileHandler(logfile, encoding='utf-8')
basic_formater = logging.Formatter('%(asctime)s : [%(levelname)s] : %(message)s')
FH.setFormatter(basic_formater)
log.addHandler(FH)

log.info('start program---------------------------------------------------------------------------')


def global_drop():
    cursor = connection.cursor()
    cursor.execute('''DROP TABLE IF EXISTS mnames''')
    cursor.execute('''DROP TABLE IF EXISTS fnames''')
    cursor.execute('''DROP TABLE IF EXISTS users''')
    connection.commit()


def generate_spr():
    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE mnames
                      (name varchar(20), surname varchar(20));''')
    cursor.execute('''CREATE TABLE fnames
                      (name varchar(20), surname varchar(20));''')
    connection.commit()
    with open('msurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            name = row.split(' ')[0]
            sname = row.split(' ')[1].replace('\n', '')
            cursor.execute(f'''INSERT INTO mnames 
                               VALUES('{name}', '{sname}')''')
    with open('fsurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            name = row.split(' ')[0]
            sname = row.split(' ')[1].replace('\n', '')
            cursor.execute(f'''INSERT INTO fnames 
                               VALUES('{name}', '{sname}')''')
    connection.commit()

def generate_users():
    cursor = connection.cursor()
    cursor.execute('''
                      CREATE TABLE users 
                          (
                          name varchar(20),
                          surname varchar(20),
                          sex varchar(1)
                          );''')
    connection.commit()
    cursor.execute('''
                   INSERT INTO users (name, surname)
                   SELECT name, surname 
                   FROM mnames
                   ORDER BY random()
                   LIMIT 500;
                   
                   UPDATE users
                   set sex = 'М';
                   
                   INSERT INTO users (name, surname)
                   SELECT name, surname 
                   FROM fnames
                   ORDER BY random()
                   LIMIT 500;
                   
                   UPDATE users
                   set sex = 'Ж'
                   where sex is null''')

    # cursor.execute('''INSERT INTO users (id, email, bookmaker_id,sex)
    #                   SET
    #                   id = "N/A",
    #                   email = "N/A",
    #                   bookmaker_id = "N/A",
    #                   sex = "N/A";''')
    connection.commit()

global_drop()
generate_spr()
generate_users()