'''
GDP per capita, PPP

http://api.worldbank.org/v2/countries/sa/indicators/NY.GDP.PCAP.PP.CD?format=json&date=2017
'''
import requests
import json
import sys
from pprint import pprint
import mysql.connector as mc

import Gdp
import Country


class MyApp:
    countries_gdp_lp = [] # This will contain latest db data to compare/avoid double insert

    def dispatch(self, environ):
        query = environ['QUERY_STRING']
        method = environ['REQUEST_METHOD']
        path = environ['PATH_INFO']
        address = environ['REMOTE_ADDR']
        #fileinfo = self.filereader()

         #Use special function parse_url (see function docstring end of the page)
        info_dict = self.parse_url(path, query)

        if method == 'GET':
            if 'api_name' not in info_dict:
                return f"Bad GET request! API does not Exist. Use (/worldbank/sa?date=2015:2017) for Saudi Arabia 2015 to 2017 ."
            elif 'country' not in info_dict:
                #inven = json.dumps(f"{info_dict['category']}: {fileinfo[info_dict['category']]}")
                return f"Bad GET request! Please specify country in format (/worldbank/sa?date=2015:2017) for Saudi Arabia 2015 to 2017"
            else:
                final_url = self.build_url(info_dict)
                print(final_url)
                self.retrieve_data(final_url)
                return "Loaded Okay!"



        return "Your request is invalid, please try new URL. Remember you can view /worldbank/"

    def retrieve_data(self, final_url):

        r = requests.get(final_url)
        response_list = r.json() #This WB World Bank response, read by .json() and returns a list
        list_of_yearly_Gdp = response_list[1] #Inside WB, element at 1 is the info we need.
        for item in list_of_yearly_Gdp:
            country_code = item['country']['id']
            country_name = item['country']['value']

            gdp_date = item['date']
            gdp_value = item['value']

            country_obj = Country.Country(country_code, country_name)
            gdp_obj = Gdp.Gdp(country_code, gdp_date, gdp_value)


            self.data_insert(country_obj, gdp_obj)



            # If country exists in list, then do not add country
            # else create object country,
            # to later insert into database.
            # If GDP year exists in GDP list, then do not add
            # else create gdp object, later insert in db
            # re-load from db again updated lists for next comparison


    def data_insert(self, country_obj, gdp_obj):
        mydb = self.open_db()
        try:
            mycursor = mydb.cursor()

            if country_obj.country_code not in db_data_lp:

                query = "INSERT INTO countries(country_code, country_name) VALUES (%s, %s)"
                _tuple_of_values = (country_obj.code, country_obj.name)

                mycursor.execute(query, _tuple_of_values)
                mydb.commit()



            if country_gdp not in #If country and the gdp at that year not in db, then write following
            query = "INSERT INTO gdps(country_code, date, value) VALUES (%s, %s, %s)"
            _tuple_of_values = (gdp_obj.country_code, gdp_obj.date, gdp_obj.value)

            mycursor.execute(query, _tuple_of_values)
            mydb.commit()
        except Exception as exc:
            print(exc)
        finally:

            mycursor.close()
            self.close_db(mydb)
            self.check_db_data()



    def check_db_data(self):
        '''
        This executes at the beginning of the program only to check if
        data already exists before inserting or udpating db.
        '''
        mydb = self.open_db()# open connection
        mycursor = mydb.cursor() # access table required
        query = "SELECT * FROM gdps"
        mycursor.execute(query)
        db_data_lp = mycursor.fetchall()
        print(f"db_data_lp: {db_data_lp}")

        mycursor.close()
        self.close_db(mydb)




    def open_db(self):
        mydb = mc.connect(
            host="localhost",
            user=os.environ['USER'],
            password=os.environ['PASSWORD'],
            database=os.environ['NAME'],
            )
        print("Inside open_db")
        return mydb

    def close_db(self, mydb):
        print("inside close_db")
        return mydb.close()



    def filewriter(self, fileinfo):
        json.dump(fileinfo, open("GDP_DB.txt", 'w'))
        return "Your inventory has been updated"

    def filereader(self):
        _dict = {}
        with open('data_api.txt', 'r') as f:
            _dict = json.loads(f.read())
        return _dict



    def parse_url(self, path, query):
        '''
            Function parses path and query and return new dictionary
            with useful information to execute any http method
            #/worldbank/sa?date=yyyy:yyyy   for Saudi Arabia GDP PPP
        '''
        info_dict = {}
        if path[0] == '/':  # Clean up leading '/'' or trailing '/''
            path = path[1:]
        if path[-1] == '/':
            path = path[:-1]
        path_list = path.split("/") #i.e.[worldbank", "sa"]
        if len(path_list) >0: #If path has items
            info_dict['api_name'] = path_list[0]
            info_dict['country'] = path_list[1]
            info_dict['indicator'] = 'NY.GDP.PCAP.PP.CD'


        if len(query) > 1:
            query_list= query.split('=') # amount=5 i.e. [amount, 5]
            info_dict['date'] = query_list[1] #Add amount in dictionary


        return info_dict


    def build_url(self, info_dict):
        pass
        prefix_uri = "http://api.worldbank.org/v2/countries"

        final_url = prefix_uri + f"/{info_dict['country']}/indicators/{info_dict['indicator']}?date={info_dict['date']}&format=json"
        print(f"Final URL:\n {final_url}")

        return final_url
