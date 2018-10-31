import psycopg2
import glob
import pandas as pd
import csv


def change_format():
    files = glob.glob("/Users/zhihao/Desktop/isi/FAOSTAT/*.csv")
    for f in files:
        with open(f, encoding="ISO-8859-1") as csv_file:
            reader = csv.DictReader(csv_file)
            keys = reader.fieldnames
            if len(keys) == 11:
                old_f = pd.read_csv(f, encoding="ISO-8859-1")
                keys = list(old_f)
                keep_col = [keys[1], keys[3], keys[5], keys[7], keys[9]]
                new_f = old_f[keep_col]
                print(new_f)
                new_f.to_csv(f, index=False)
            elif len(keys) > 11:
                continue


if __name__ == '__main__':
    # change_format()
    files = glob.glob("/Users/zhihao/Desktop/isi/FAOSTAT/*.csv")
    conn = None
    try:
        # read connection parameters
        params = dict()
        params["dbname"] = 'faostat'
        params["user"] = 'postgres'
        params["password"] = '123123'
        params["host"] = 'dsbox02.isi.edu'

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        i = 0;
        cur = conn.cursor()
        for f in files:
            tmp = f.split("/")
            words = tmp[len(tmp) - 1].split(".")[0].split("_")
            name = ""
            for word in words:
                if word.find("(") == -1:
                    name = name + word + "_"
            name = name[:-1]
            name = name.lower()
            name = name.replace("-", "_")
            with open(f, encoding="ISO-8859-1") as csv_file:
                reader = csv.DictReader(csv_file)
                keys = reader.fieldnames
                area_or_country = keys[0].replace(" ", "_")
                item = keys[1].replace(" ", "_")
                element = keys[2].replace(" ", "_")
                year = keys[3]
                i = i + 1
                if year.find("-") != -1:
                    year = year[:year.find("-")]
                value = keys[4]
                try:
                    create_table = '''CREATE TABLE {0}
                                 ({1}         TEXT     ,
                                  {2}         TEXT    ,
                                  {3}         TEXT      ,
                                  {4}         INT     ,
                                  {5}         REAL      ); '''.format(name, area_or_country, item, element,
                                                                      year, value)
                    cur.execute(create_table)
                    conn.commit()
                    f = f[f.rfind('/') + 1:]
                    f = "/nfs1/dsbox-repo/zhihao/FAOSTAT/" + f
                    postgres_copy_query = "COPY {0} FROM '{1}' DELIMITER ',' CSV HEADER;".format(name, f)
                    cur.execute(postgres_copy_query)
                    conn.commit()
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)
                    conn.commit()
                    pass

            # for row in reader:
                #     if row[area_or_country].find("'") != -1:
                #         continue
                # postgres_insert_query = """ INSERT INTO {0} ({1}, {2}, {3}, {4}, {5})
                # VALUES ('{6}','{7}','{8}',{9},{10})""" \
                #     .format(name, area_or_country.lower(), item.lower(), element.lower(), year.lower(), value.lower(), row[area_or_country], row[item],
                #             row[element], row[year], row[value])

        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

# ConsumerPriceIndices_E_All_Data_(Normalized).csv
# 12
# Area Code	Area	Item Code	Item	Months Code	Months	Year Code	Year	Unit	Value	Flag	Note

# Investment_CapitalStock_E_All_Data.csv
# 51

# Indicators_from_Household_Surveys_E_All_Data_(Normalized).csv
# 13
# Survey Code	Survey	Breakdown Variable Code	Breakdown Variable	Breadown by Sex of the Household Head Code	Breadown by Sex of the Household Head	Indicator Code	Indicator	Measure Code	Measure	Unit	Value	Flag


# Forestry_Trade_Flows_E_All_Data_(Normalized).csv
# 13
# Reporter Country Code	Reporter Countries	Partner Country Code	Partner Countries	Item Code	Item	Element Code	Element	Year Code	Year	Unit	Value	Flag


# Prices_Monthly_E_All_Data_(Normalized).csv
# 13
# Area Code	Area	Item Code	Item	Element Code	Element	Months Code	Months	Year Code	Year	Unit	Value	Flag
