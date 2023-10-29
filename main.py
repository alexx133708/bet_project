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
from transliterate import translit
from random import randint, choice
import datetime

res_path = 'E:\\bigdata\\bet_project\\'
rep_path = 'E:\\bigdata\\bet_project\\reports\\'
log_path = 'E:\\bigdata\\bet_project\\logs\\'
orig_path = 'E:\\bigdata\\original\\'
while True:
    srv = input('выберите сервер \n1. 192.168.1.75\n2. 188.120.250.101\n3. 91.144.153.132\n: ')
    if srv == '1':
        engine = create_engine(url=f"postgresql+psycopg2://postgres@192.168.1.75/bet_project", echo=False)
        break
    elif srv == '2':
        engine = create_engine(url=f"postgresql+psycopg2://postgres@188.120.250.101/postgres", echo=False)
        break
    elif srv == '3':
        engine = create_engine(url=f"postgresql+psycopg2://postgres@91.144.153.132/bet_project", echo=False)
        break
    print('циферку введи')
while True:
    try:
        user_quan = int(input('введите количество пользователей: '))
        break
    except:
        print('циферку введи')
subj_cfg = 'C:\\Users\\alex\\PycharmProjects\\school\\subj_cfg.csv'
connection = engine.raw_connection()
print("канекшон саксессфул")
ct = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
logname = f"{ct}.log"
logfile = open(log_path + logname, 'w+')
logfile.close()
logging.basicConfig(
    level=logging.INFO,
    filename=log_path + logname,
    format="%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s",
    datefmt='%H:%M:%S',
)
logging.info('start program -----------------------------------------')


def global_drop():
    logging.info('drop database start')
    cursor = connection.cursor()
    cursor.execute('''DROP TABLE IF EXISTS sprnames''')
    cursor.execute('''DROP TABLE IF EXISTS users CASCADE''')
    cursor.execute('''DROP TABLE IF EXISTS emails''')
    cursor.execute('''DROP TABLE IF EXISTS bookmaker CASCADE''')
    cursor.execute('''DROP TABLE IF EXISTS events CASCADE''')
    cursor.execute('''DROP TABLE IF EXISTS sprteams''')
    cursor.execute('''DROP TABLE IF EXISTS team_score''')
    cursor.execute('''DROP TABLE IF EXISTS teams''')
    cursor.execute('''DROP TABLE IF EXISTS bets CASCADE''')
    cursor.execute('''DROP TABLE IF EXISTS matches''')
    cursor.execute('''DROP TABLE IF EXISTS wins''')
    connection.commit()
    logging.info('drop database end')


def percentage(part, whole):
    return 100 * float(part) / float(whole)


def generate_spr():
    logging.info('generate references start')
    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE sprnames
                      (name varchar(20), surname varchar(20), sex varchar(1));''')
    connection.commit()
    with open('surnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            name = row.split(' ')[0]
            sname = row.split(' ')[1].replace('\n', '')
            sex = row.split(' ')[2].replace('\n', '')
            cursor.execute(f'''INSERT INTO sprnames 
                               VALUES('{name}', '{sname}', '{sex}')''')
    connection.commit()
    cursor.execute('''CREATE TABLE sprteams
                          (id serial, name varchar(50));''')
    connection.commit()
    with open('teams.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            help = '\n'
            cursor.execute(f'''INSERT INTO sprteams(name)
                                   VALUES('{row.replace(help, "")}')''')
    connection.commit()
    cursor.execute('''
CREATE OR REPLACE FUNCTION public.add_users(n integer)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare counter int = 0;
begin
	while counter <= n
	loop
		insert into users(name, surname, sex, bookmaker_id, age)
		values((select name from sprnames where sex = 'М' order by random() limit 1),
	           (select surname from sprnames where sex = 'М' order by random() limit 1),
	           'М',
	           floor(random()*n*10),
	           floor(random()*62+18));
		counter = counter + 1;
	end loop;
	counter = 0;
	while counter <= n
	loop
		insert into users(name, surname, sex, bookmaker_id, age)
		values((select name from sprnames where sex = 'Ж' order by random() limit 1),
	           (select surname from sprnames where sex = 'Ж' order by random() limit 1),
	           'Ж',
	           floor(random()*n*10),
	           floor(random()*62+18));
		counter = counter + 1;
	end loop;
	return 0;
end

$function$
;''')
    connection.commit()
    logging.info('generate references end')
def generate_tables():
    logging.info('generate bookmaker start')
    cursor = connection.cursor()
    cursor.execute('''
                          CREATE TABLE bookmaker
                          (
                          id serial primary key,
                          district text,
                          region text,
                          city text,
                          address text,
                          size varchar(2)
                          );''')
    connection.commit()
    with open(orig_path + 'Stores.csv', 'r', encoding='utf-8') as f:
        i = 0
        for row in f.readlines():
            i += 1
            if i > user_quan*10:
                break
            row = row.split(';')
            help = '\n'
            cursor.execute(f'''
                              INSERT INTO bookmaker ("district", "region", "city", "address", "size")
                              VALUES('{row[1]}', '{row[2]}', '{row[3]}', '{row[4].replace("'", "")}', '{row[5].replace(help, "")}')
                              ''')
            print('-')
    # cursor.execute('create table bookmaker as table stores;')
    connection.commit()
    logging.info('generate bookmaker end')
    logging.info('generate users emails start')
    cursor.execute('''
                      CREATE TABLE users 
                          (
                          id serial primary key,
                          name varchar(20),
                          surname varchar(20),
                          sex varchar(1),
                          bookmaker_id integer REFERENCES bookmaker(id),
                          age int
                          );''')
    connection.commit()
    cursor.execute('''
                          CREATE TABLE emails
                              (
                              id serial primary key,
                              user_id integer REFERENCES users(id),
                              email text
                              );''')
    connection.commit()
    cursor.execute(f'select add_users({user_quan})')
    connection.commit()
    cursor.execute('''
                          SELECT name, surname, id 
                          FROM users
                          ''')
    users = cursor.fetchall()
    emailist = []
# генерация ящиков
    for user in users:
        email = translit(user[0] + '.' + user[1], language_code='ru', reversed=True).lower().replace("'", "") \
                + random.choice(["@gmail.com", "@mail.ru", "@ya.ru"])
        while True:
            if email in emailist:
                email = email + str(randint(1, 9))
            else:
                emailist.append([email, user[2]])
                break
#
    connection.commit()
    for i, email in enumerate(emailist):
        cursor.execute(f'''
                          INSERT INTO emails 
                          VALUES({i + 1}, {email[1]}, '{email[0]}')''')
    logging.info('generate users emails end')
    logging.info('generate events start')
    cursor.execute('''CREATE TABLE events
                   (id serial primary key, name varchar(50), datestart date)''')
    events = ['Футбол. Чемпионат России',
              'Футбол. Премьер лига России',
              'Футбол. Молодёжный Чемпионат']
    for event_name in events:
        month = randint(1, 12)
        day = randint(1, 28)
        date = datetime.date(2023, month, day)
        cursor.execute(f'''INSERT INTO events (name, datestart)
                          VALUES('{event_name}', '{date}')
                          ''')
    connection.commit()
    logging.info('generate events end')
    logging.info('generate teams start')
    cursor.execute('''CREATE TABLE teams
                      (id serial primary key, event_id integer , name varchar(50),
                      CONSTRAINT FK_teams_events FOREIGN KEY (event_id) REFERENCES events(id))''')
    connection.commit()
    event_id = 1
    count = 0
    cursor.execute('''SELECT * FROM sprteams order by random()''')
    for team_name in cursor.fetchall():
        cursor.execute(f'''INSERT INTO teams(name, event_id)
                           VALUES('{team_name[1]}',{event_id})''')
        count += 1
        if count == 32 or count == 64:
            event_id += 1
        if count == 96:
            break
    connection.commit()
    logging.info('generate teams end')
    logging.info('generate matches start')
    cursor.execute('''CREATE TABLE matches
                          (id serial primary key,
                           event_id int references events(id),
                           year int,
                           month int,
                           day int,
                           t1_name varchar(50), 
                           t1_res integer,
                           t1_pts integer,
                           t2_name varchar(50), 
                           t2_res integer, 
                           t2_pts integer)''')
    cursor.execute('''SELECT * FROM events''')
    events = cursor.fetchall()
# генерация данных матчей
    for event in events:
        cursor.execute(f'''SELECT name FROM teams WHERE event_id = {event[0]}''')
        team_names = cursor.fetchall()
        for teami in range(0, 32):
            teams = team_names
            cur_team = teams[teami]
            for team in teams:
                t1_res = randint(0, 6)
                t2_res = randint(0, 6)
                if t1_res == t2_res:
                    t1_pts = 1
                    t2_pts = 1
                elif t1_res < t2_res:
                    t1_pts = 0
                    t2_pts = 2
                elif t1_res > t2_res:
                    t1_pts = 2
                    t2_pts = 0
#
                cursor.execute(f'''INSERT INTO matches(event_id, year, month, day, t1_name, t1_res, t1_pts, t2_name, t2_res, t2_pts) 
                                   VALUES({event[0]}, {randint(2018, 2022)}, {randint(1, 12)}, {randint(1, 28)}, '{cur_team[0]}', {t1_res}, {t1_pts}, '{team[0]}', {t2_res}, {t2_pts})''')
    connection.commit()
    logging.info('generate matches end')
    logging.info('generate bets start')
    cursor.execute('''CREATE TABLE bets
                              (id serial primary key,
                               client_id integer,
                               bet_size integer,
                               match_id integer,
                               event_id integer,
                               bet_on boolean)''')
    cursor.execute('''SELECT id from users''')
    users_id = cursor.fetchall()
    cursor.execute('''SELECT id from matches''')
    matches_id = cursor.fetchall()
    connection.commit()
    for match_id in matches_id:
        for id in users_id:
            if randint(1, 10) in [1, 2, 3]:
                cursor.execute(f'''INSERT INTO bets(client_id, bet_size, match_id, event_id, bet_on) 
                                   VALUES('{id[0]}', {randint(100, 10000)}, {match_id[0]}, '{randint(1, 3)}', {choice([True, False])})''')
    connection.commit()
    logging.info('generate bets end')
    logging.info('generate wins start')
    cursor.execute('''CREATE TABLE wins
                          (id serial primary key,
                           client_id integer references users(id),
                           bet_id integer references bets(id),
                           win integer,
                           book_win integer,
                           team text)''')

    connection.commit()
    cursor.execute('''SELECT * from matches''')
    matches = cursor.fetchall()
    cursor.execute('''SELECT * from bets''')
    bets = cursor.fetchall()
# расчёт выигрышей
    winsum = 0
    allsum = 0
    for bet in bets:
        try:
            if bet[5] == True and matches[bet[3]][7] == 2 or bet[5] == False and matches[bet[3]][10] == 2:
                winsum += bet[2]
        except:
            pass
        # allsum += bet[2]
    cursor.execute('SELECT SUM(bet_size) FROM bets')
    allsum = cursor.fetchall()[0][0]
    cursor.execute('SELECT SUM(bet_size) - SUM(bet_size) / 20 FROM bets')
    allsumclear = cursor.fetchall()[0][0]
    for bet in bets:
        try:
            if bet[5]:
                bet_team = matches[bet[3]][5]
            else:
                bet_team = matches[bet[3]][8]
        except:
            print(len(matches))
        try:
            if bet[5] == True and matches[bet[3]][7] == 2 or bet[5] == False and matches[bet[3]][10] == 2:
                # win = percentage(bet[2], allsum) * allsumclear
                cursor.execute('SELECT (100 * bet_size / SUM(bet_size)) * (SUM(bet_size) - SUM(bet_size) / 20) FROM bets')
                cursor.execute(f'''INSERT INTO wins(client_id, bet_id, win, book_win, team)
                                   VALUES({bet[1]}, {bet[0]}, {win}, {bet[2]/20}, '{bet_team}')''')
            else:
                cursor.execute(f'''INSERT INTO wins(client_id, bet_id, win, book_win, team)
                                                   VALUES({bet[1]}, {bet[0]}, 0, {bet[2]}, '{bet_team}')''')
        except:
            pass
#
    connection.commit()
    logging.info('generate wins end')

def raschet(ct):
    logging.info('create team_score start')
    age_groups = [(18, 30), (30, 40), (40, 50), (50, 60), (60, 80)]
    repname = f"report{ct}.txt"
    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE team_score(team text, score int, goals int, year int, event_id int)''')
    connection.commit()
    cursor.execute('select * from teams')
    teams = cursor.fetchall()
    team_check = []
    for year in range(2018, 2023):
        for team in teams:
            cursor.execute(f'''SELECT SUM(t1_pts)
                               FROM matches 
                               WHERE "t1_name" = '{team[2]}' AND year = {year};''')
            t1 = cursor.fetchall()[0][0]
            t1 = t1 if t1 != None else 0
            cursor.execute(f'''SELECT SUM(t1_res)
                               FROM matches 
                               WHERE "t1_name" = '{team[2]}' AND year = {year};''')
            goal1 = cursor.fetchall()[0][0]
            goal1 = goal1 if goal1 != None else 0
            cursor.execute(f'''SELECT SUM(t2_pts)
                               FROM matches 
                               WHERE "t2_name" = '{team[2]}' AND year = {year};''')
            t2 = cursor.fetchall()[0][0]
            t2 = t2 if t2 != None else 0
            cursor.execute(f'''SELECT SUM(t2_res)
                               FROM matches 
                               WHERE "t1_name" = '{team[2]}' AND year = {year};''')
            goal2 = cursor.fetchall()[0][0]
            goal2 = goal2 if goal2 != None else 0
            cursor.execute(f'''INSERT INTO team_score
                               VALUES ('{team[2]}', {t1 + t2}, {goal1+goal2}, {year}, {team[1]})''')
    connection.commit()
    logging.info('create team_score end')
    cursor.execute('''SELECT * FROM events''')
    events = cursor.fetchall()
    logging.info('queries start')
    with open(rep_path + repname, 'w', encoding='utf-8') as f:
        f.write(f"Отчёт по базе данных bet_project\n")
        f.write(f"Параметры:\n")
        f.write(f"\tБаза данных - bet_project:\n")
        for event in events:
            for year in range(2018, 2023):
                cursor.execute(
                    f'''SELECT DISTINCT team, score FROM team_score WHERE year = {year} AND event_id = {event[0]} ORDER BY score DESC LIMIT 3 ''')
                query1 = cursor.fetchall()
                cursor.execute(
                    f'''SELECT DISTINCT team, score FROM team_score WHERE year = {year} AND event_id = {event[0]} ORDER BY score ASC LIMIT 3''')
                query2 = cursor.fetchall()
                f.write(f"\nТоп 3 самых успешных команд в рамках чемпионата {event[1]} в {year} году:\n")
                for row in query1:
                    f.write(f"\t{row[0]} - {row[1]}\n")
                f.write(f"\nТоп 3 самых унылых команд в рамках чемпионата {event[1]} в {year} году:\n")
                for row in query2:
                    f.write(f"\t{row[0]} - {row[1]}\n")
        cursor.execute('''select users.name, users.surname, wins.win from users join wins on users.id = wins.client_id order by win desc limit 100''')
        wins = cursor.fetchall()
        f.write(f'\nТоп 100 пользователей по выигрышу\n')
        for i, win in enumerate(wins):
            f.write(f'\t{i+1}. {win[0]} {win[1]} - {win[2]}\n')
        for year in range(2018, 2023):
            f.write(f'\nТоп 20 выигрышей за {year} год:\n')
            cursor.execute(f'''select users.name, users.surname, wins.win, matches."year" from users join wins on users.id = wins.client_id join bets on bets.id = wins.bet_id join matches on bets.match_id = matches.id where year = {year} order by win desc limit 20''')
            query1 = cursor.fetchall()
            for i, row in enumerate(query1):
                f.write(f'\t{i+1}. {row[0]} {row[1]} - {row[2]}\n')
        for year in range(2018, 2023):
            f.write(f'\nТоп 10 выигрышей среди мужчин за {year} год:\n')
            cursor.execute(f'''select users.name, users.surname, wins.win, matches."year", users.sex from users join wins on users.id = wins.client_id join bets on bets.id = wins.bet_id join matches on bets.match_id = matches.id where year = {year} and sex = 'М' order by win desc limit 10''')
            query1 = cursor.fetchall()
            for i, row in enumerate(query1):
                f.write(f'\t{i+1}. {row[0]} {row[1]} - {row[2]}\n')
        for year in range(2018, 2023):
            f.write(f'\nТоп 10 выигрышей среди женщин за {year} год:\n')
            cursor.execute(f'''select users.name, users.surname, wins.win, matches."year", users.sex from users join wins on users.id = wins.client_id join bets on bets.id = wins.bet_id join matches on bets.match_id = matches.id where year = {year} and sex = 'Ж' order by win desc limit 10''')
            query1 = cursor.fetchall()
            for i, row in enumerate(query1):
                f.write(f'\t{i+1}. {row[0]} {row[1]} - {row[2]}\n')
        cursor.execute('''select sum(bets.bet_size), matches."year" from bets join matches on matches.id = bets.match_id join users on bets.client_id = users.id where users.sex = 'М' group by year''')
        year_sums = cursor.fetchall()
        f.write('\nСумма ставок по годам среди мужчин:\n')
        for year in year_sums:
            f.write(f'\t{year[1]} - {year[0]}\n')
        cursor.execute('''select sum(bets.bet_size), matches."year" from bets join matches on matches.id = bets.match_id join users on bets.client_id = users.id where users.sex = 'Ж' group by year''')
        year_sums = cursor.fetchall()
        f.write('\nСумма ставок по годам среди женщин:\n')
        for year in year_sums:
            f.write(f'\t{year[1]} - {year[0]}\n')
        cursor.execute('''select distinct team, goals from team_score order by goals desc limit 5''')
        goals = cursor.fetchall()
        f.write('\nТоп 5 команд по большему забиванию голов:\n')
        for i, team in enumerate(goals):
            f.write(f'\t{i+1}. {team[0]} - {team[1]}\n')
        cursor.execute('''select distinct team, goals from team_score order by goals asc limit 5''')
        goals = cursor.fetchall()
        f.write('\nТоп 5 команд по меньшему забиванию голов:\n')
        for i, team in enumerate(goals):
            f.write(f'\t{i+1}. {team[0]} - {team[1]}\n')
        f.write('\nСумма выигрышей по возрастным группам:\n')
        for age_group in age_groups:
            cursor.execute(f'''select sum(wins.win) from wins join users on wins.client_id = users.id where users.age > {age_group[0]} and users.age < {age_group[1]}''')
            f.write(f'\t{age_group[0]}-{age_group[1]}: {cursor.fetchall()[0][0]}\n')
        cursor.execute('''select users.bookmaker_id, sum(wins.book_win) from users join wins on users.id = wins.client_id group by bookmaker_id order by sum(book_win) desc limit 10 ''')
        book_sums = cursor.fetchall()
        f.write('\nТоп 10 контор по большему заработку:\n')
        for i, row in enumerate(book_sums):
            f.write(f'\t{i}. {row[0]} - {row[1]}\n')
        cursor.execute('''select users.bookmaker_id, sum(wins.book_win) from users join wins on users.id = wins.client_id group by bookmaker_id order by sum(book_win) asc limit 10 ''')
        book_sums = cursor.fetchall()
        f.write('\nТоп 10 контор по меньшему заработку:\n')
        for i, row in enumerate(book_sums):
            f.write(f'\t{i}. {row[0]} - {row[1]}\n')
        cursor.execute('''select sum(win), team from wins group by team order by sum(win) desc limit 1''')
        team = cursor.fetchall()
        f.write(f'\nКоманда на которой больше всех выиграли пользователи:\n')
        f.write(f'\t{team[0][1]} - {team[0][0]}\n')
        cursor.execute('''select sum(book_win), team from wins group by team order by sum(book_win) desc limit 1''')
        team = cursor.fetchall()
        f.write(f'\nКоманда на которой больше всех выиграла контора:\n')
        f.write(f'\t{team[0][1]} - {team[0][0]}\n')
        cursor.execute('''select sum(wins.book_win), events.name from wins join bets on wins.bet_id = bets.id join events on bets.event_id = events.id group by name order by sum(wins.book_win) desc limit 1''')
        event = cursor.fetchall()
        f.write(f'\nСамый прибыльный чемпионат для конторы в целом:\n')
        f.write(f'\t{event[0][1]} - {event[0][0]}\n')
        for year in range(2018, 2023):
            cursor.execute(f'''select sum(wins.book_win), events.name from wins join bets on wins.bet_id = bets.id join events on bets.event_id = events.id join matches on bets.match_id = matches.id where year = {year} group by name order by sum(wins.book_win) desc limit 1''')
            event = cursor.fetchall()
            f.write(f'\nСамый прибыльный чемпионат для конторы за {year} год:\n')
            f.write(f'\t{event[0][1]} - {event[0][0]}\n')
        for year in range(2018, 2023):
            cursor.execute(f'''select bookmaker.region, sum(wins.book_win) from wins join bets on wins.bet_id = bets.id join users on users.id = bets.client_id join bookmaker on bookmaker.id = users.bookmaker_id join matches on matches.id = bets.match_id where matches.year = {year} group by region order by sum(book_win) desc''')
            book_region = cursor.fetchall()
            f.write(f'\nТоп регионов по прибыли для конторы за {year} год\n')
            for i, region in enumerate(book_region):
                f.write(f'\t{i+1}. {region[0]} - {region[1]}\n')
        for year in range(2018, 2023):
            cursor.execute(f'''select users.name, users.surname, sum(bets.bet_size) from users join bets on users.id = bets.client_id join matches on matches.id = bets.match_id where matches.year = {year} group by users.name, users.surname order by sum limit 10''')
            user_bets = cursor.fetchall()
            f.write(f'\nТоп пользователей по количеству ставок за {year} год\n')
            for i, bet in enumerate(user_bets):
                f.write(f'\t{i+1}. {bet[0]} - {bet[1]}\n')
    logging.info('queries end')
connection.commit()
global_drop()
generate_spr()
generate_tables()
raschet(ct)
connection.commit()
logging.info('end program')
