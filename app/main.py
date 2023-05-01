import csv
import time
import psycopg2


def variant():
    '''Функція виводу автора роботи'''
    print('Лабораторна робота №1\nШаповалов Гліб, КМ-03\nВаріант 14\n\
Порівняти середній бал з Англійської мови у кожному регіоні у 2020 та 2019 роках \
серед тих кому було зараховано тест')


def to_float(string):
    '''Функція заміни коми на крапку для уникнення помилки типізації'''
    if string != 'null' and string is not None:
        return float(string.replace(',', '.'))


def fill_table(columns, values):
    '''Функція створення строки для вставки в БД'''
    return f'INSERT INTO zno_table ({columns}) VALUES ({values})'


create = '''
CREATE TABLE IF NOT EXISTS zno_table (
    OUTID uuid,
    Birth int NULL,
    SEXTYPENAME varchar NULL,
    REGNAME varchar NULL,
    AREANAME varchar NULL,
    TERNAME varchar NULL,
    REGTYPENAME varchar NULL,
    TerTypeName varchar NULL,
    ClassProfileNAME varchar NULL,
    ClassLangName varchar NULL,
    EONAME varchar NULL,
    EOTYPENAME varchar NULL,
    EORegName varchar NULL,
    EOAreaName varchar NULL,
    EOTerName varchar NULL,
    EOParent varchar NULL,
    UkrTest varchar NULL,
    UkrTestStatus varchar NULL,
    UkrBall100 float NULL,
    UkrBall12 int NULL,
    UkrBall int NULL,
    UkrAdaptScale int NULL,
    UkrPTName varchar NULL,
    UkrPTRegName varchar NULL,
    UkrPTAreaName varchar NULL,
    UkrPTTerName varchar NULL,
    histTest varchar NULL,
    HistLang varchar NULL,
    histTestStatus varchar NULL,
    histBall100 float NULL,
    histBall12 int NULL,
    histBall int NULL,
    histPTName varchar NULL,
    histPTRegName varchar NULL,
    histPTAreaName varchar NULL,
    histPTTerName varchar NULL,
    mathTest varchar NULL,
    mathLang varchar NULL,
    mathTestStatus varchar NULL,
    mathBall100 float NULL,
    mathBall12 int NULL,
    mathBall int NULL,
    mathPTName varchar NULL,
    mathPTRegName varchar NULL,
    mathPTAreaName varchar NULL,
    mathPTTerName varchar NULL,
    physTest varchar NULL,
    physLang varchar NULL,
    physTestStatus varchar NULL,
    physBall100 float NULL,
    physBall12 int NULL,
    physBall int NULL,
    physPTName varchar NULL,
    physPTRegName varchar NULL,
    physPTAreaName varchar NULL,
    physPTTerName varchar NULL,
    chemTest varchar NULL,
    chemLang varchar NULL,
    chemTestStatus varchar NULL,
    chemBall100 float NULL,
    chemBall12 int NULL,
    chemBall int NULL,
    chemPTName varchar NULL,
    chemPTRegName varchar NULL,
    chemPTAreaName varchar NULL,
    chemPTTerName varchar NULL,
    bioTest varchar NULL,
    bioLang varchar NULL,
    bioTestStatus varchar NULL,
    bioBall100 float NULL,
    bioBall12 int NULL,
    bioBall int NULL,
    bioPTName varchar NULL,
    bioPTRegName varchar NULL,
    bioPTAreaName varchar NULL,
    bioPTTerName varchar NULL,
    geoTest varchar NULL,
    geoLang varchar NULL,
    geoTestStatus varchar NULL,
    geoBall100 float NULL,
    geoBall12 int NULL,
    geoBall int NULL,
    geoPTName varchar NULL,
    geoPTRegName varchar NULL,
    geoPTAreaName varchar NULL,
    geoPTTerName varchar NULL,
    engTest varchar NULL,
    engTestStatus varchar NULL,
    engBall100 float NULL,
    engBall12 int NULL,
    engDPALevel varchar NULL,
    engBall int NULL,
    engPTName varchar NULL,
    engPTRegName varchar NULL,
    engPTAreaName varchar NULL,
    engPTTerName varchar NULL,
    fraTest varchar NULL,
    fraTestStatus varchar NULL,
    fraBall100 float NULL,
    fraBall12 int NULL,
    fraDPALevel varchar NULL,
    fraBall int NULL,
    fraPTName varchar NULL,
    fraPTRegName varchar NULL,
    fraPTAreaName varchar NULL,
    fraPTTerName varchar NULL,
    deuTest varchar NULL,
    deuTestStatus varchar NULL,
    deuBall100 float NULL,
    deuBall12 int NULL,
    deuDPALevel varchar NULL,
    deuBall int NULL,
    deuPTName varchar NULL,
    deuPTRegName varchar NULL,
    deuPTAreaName varchar NULL,
    deuPTTerName varchar NULL,
    spaTest varchar NULL,
    spaTestStatus varchar NULL,
    spaBall100 float NULL,
    spaBall12 int NULL,
    spaDPALevel varchar NULL,
    spaBall int NULL,
    spaPTName varchar NULL,
    spaPTRegName varchar NULL,
    spaPTAreaName varchar NULL,
    spaPTTerName varchar NULL,
    YEAR int NULL,
    CONSTRAINT zno_pk PRIMARY KEY (outid)
);
'''

final_query = '''
    SELECT RegName, YEAR, AVG(EngBall100) AS avg_score
    FROM zno_table
    WHERE EngTestStatus = 'Зараховано'
    GROUP BY RegName, YEAR;
    '''


if __name__ == '__main__':
    #Команда для створення і запуску: docker-compose build --no-cache && docker-compose up -d --force-recreate
    
    variant()

    RUN_FLAG = True
    while RUN_FLAG:
        try:
            connect = psycopg2.connect(dbname='znodb', user='postgres', password='postgres', host='db')

            with connect:
                start = time.time()  #  Старт таймера
                cursor = connect.cursor()
                cursor.execute(create)

                # ------------------------------------------------------------------------------------
                
                sql_str_count_2019 = '''SELECT COUNT(outID) FROM zno_table WHERE year=2019'''
                cursor.execute(sql_str_count_2019)
                count_2019 = cursor.fetchone()[0]
                print(f'\nRows in the table for 2019 - {count_2019}\n')

                with open(r'./Odata2019File.csv', 'r', encoding='windows-1251') as csvfile:
                    reader = csv.reader(csvfile, delimiter=';')
                    headers = next(reader)
                    headers.append('YEAR')  #  Список назв колонок
                    str_headers = ', '.join(headers)  #  Строка назв колонок
                    idx_float = [headers.index(i) for i in headers if 'Ball100' in i]  #  Індекси усіх колонок з типом флоат
                    for row_id, row in enumerate(reader):
                        if row_id >= int(count_2019):
                            tmp_row = []
                            for el_id, el in enumerate(row):
                                if el_id in idx_float:
                                    el = to_float(el)
                                    tmp_row.append(el)
                                else:
                                    tmp_row.append(el)
                            lst_row = list(map(str, tmp_row))
                            lst_row.append('2019')
                            lst_row = [x.replace("\'", "`") for x in lst_row]
                            tmp_str = ''
                            for i in lst_row:
                                tmp_str += "\'" + i + "\', "
                            str_row = tmp_str[:-2]
                            str_row = str_row.replace("\'null\'", "null")
                            str_row = str_row.replace("\'None\'", "null")
                            str_insert = fill_table(str_headers, str_row)
                            cursor.execute(str_insert)
                            if (row_id+1) % 1000 == 0 and row_id != 0:
                                print(f'{row_id+1} `s rows in the table')
                                connect.commit()

                    connect.commit()

                # ------------------------------------------------------------------------------------

                sql_str_count_2020 = '''SELECT COUNT(outID) FROM zno_table WHERE year=2020'''
                cursor.execute(sql_str_count_2020)
                count_2020 = cursor.fetchone()[0]
                print(f'\nRows in the table for 2020 - {count_2020}\n')

                with open(r'./Odata2020File.csv', 'r', encoding='windows-1251') as csvfile:
                    reader = csv.reader(csvfile, delimiter=';')
                    headers = next(reader)
                    headers.append('YEAR')  #  Список назв колонок
                    str_headers = ', '.join(headers)  #  Строка з усіх назв колонок -> 'OUTID, Birth, ...'
                    idx_float = [headers.index(i) for i in headers if 'Ball100' in i]
                    #  Індекси усіх колонок з типом флоат
                    for row_id, row in enumerate(reader):
                        if row_id >= int(count_2020):
                            tmp_row = []
                            for el_id, el in enumerate(row):
                                if el_id in idx_float:
                                    el = to_float(el)
                                    tmp_row.append(el)
                                else:
                                    tmp_row.append(el)
                            lst_row = list(map(str, tmp_row))
                            lst_row.append('2020')
                            lst_row = [x.replace("\'", "`") for x in lst_row]
                            tmp_str = ''
                            for i in lst_row:
                                tmp_str += "\'" + i + "\', "
                            str_row = tmp_str[:-2]
                            str_row = str_row.replace("\'null\'", "null")
                            str_row = str_row.replace("\'None\'", "null")
                            str_insert = fill_table(str_headers, str_row)
                            cursor.execute(str_insert)
                            if (row_id+1) % 1000 == 0 and row_id != 0:
                                print(f'{row_id+1} `s rows in the table')
                                connect.commit()

                    connect.commit()
                
                # ------------------------------------------------------------------------------------

                with open(r'Timer.txt', 'w') as timefile:
                    timefile.write(f'Execution time: {round(time.time() - start, 2)} s')
                print(f'Timer: {round(time.time() - start, 0)} s\n')

                # ------------------------------------------------------------------------------------

                with open(r'Final_query.csv', 'w', newline='', encoding='utf-8') as csvfile:
                    cursor.execute(final_query)
                    writer = csv.writer(csvfile, delimiter=',')
                    writer.writerow([col[0] for col in cursor.description])

                    for row in cursor:
                        writer.writerow([str(el) for el in row])
        
                # ------------------------------------------------------------------------------------

                RUN_FLAG = False


        except psycopg2.OperationalError as err:
            print(f'\nERROR: {err}\n')
            time.sleep(15)
        except FileNotFoundError as err:
            RUN_FLAG = False
            print(f'\nERROR: {err}\n')
            print(f'\nFile {err.filename} does not exist\n')
        except psycopg2.errors.AdminShutdown as err:
            print(f'\nERROR: {err}\n')
        except psycopg2.InterfaceError as err:
            print(f'\nERROR: {err}\n')