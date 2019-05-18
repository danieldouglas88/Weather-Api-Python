import pyodbc
import requests
import datetime
import time

#print time
datetime.time(15, 8, 24)
print("Script began at: ", datetime.datetime.now().time())

#connect to server and db 
cnxn = pyodbc.connect("Driver={SQL Server};"
                        "Server=xxxx;"
                        "Database=xxxx;"
                        "Trusted_Connection=yes;")

def weather_data(zipcode, restnum):

    #connect to API service    
    cursor = cnxn.cursor()
    api = requests.get('http://api.openweathermap.org/data/2.5/forecast/daily?q=' + str(zipcode) + ',us&cnt=15&APPID=xxxxxxxxxxxx')
    api= api.json()
    
    #
    i = 1
    while i < 15:
        #parse JSON values
        try:
            date_forecasted = time.strftime('%Y-%m-%d', time.localtime(api['list'][i]['dt']))
        except:
            date_forecasted = None

        try:
            main_descript = api['list'][i]['weather'][0]['main']
        except:
            main_descript = None

        try:
            description = api['list'][i]['weather'][0]['description']
        except:
            description = None

        try:
            tempday = (float(api['list'][i]['temp']['day']) - 273.15) * 9/5 + 32
        except:
            tempday = None

        try:
            tempnight = (float(api['list'][i]['temp']['night']) - 273.15) * 9/5 + 32
        except:
            tempnight = None

        try:
            tempeve = (float(api['list'][i]['temp']['eve']) - 273.15) * 9/5 + 32
        except:
            tempeve = None

        try:
            tempmorn = (float(api['list'][i]['temp']['morn']) - 273.15) * 9/5 + 32
        except:
            tempmorn = None

        try:
            tempmin = (float(api['list'][i]['temp']['min']) - 273.15) * 9/5 + 32
        except:
            tempmin = None

        try:
            tempmax = (float(api['list'][i]['temp']['max']) - 273.15) * 9/5 + 32
        except:
            tempmax = None

        try:
            pressure = api['list'][i]['pressure']
        except:
            pressure = None

        try:
            humidity = api['list'][i]['humidity']
        except:
            humidity = None

        try:
            windspeed = float(api["list"][i]['speed']) * 2.237
        except:
            windspeed = None

        try:
            winddeg = api["list"][i]['deg']
        except:
            winddeg = None

        try:
            cloudiness = api["list"][i]['clouds']
        except:
            cloudiness = None

        try:
            rain = float(api['list'][i]['rain']) /  25.4
        except:
            rain = None

        try:
            snow = float(api['list'][i]['snow']) /  25.4
        except:
            snow = None

        try:
            name = api['city']['name']
        except:
            name = None


        query = "INSERT INTO forecast_weather_data VALUES (?, getdate(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
        values = (restnum, date_forecasted, main_descript, description, tempday, tempnight, tempeve, tempmorn, tempmin, tempmax, pressure, humidity, windspeed, winddeg, cloudiness, rain, snow, name, zipcode)
        cursor.execute(query, values)            
        cnxn.commit()
        
        i = i + 1

#call function
weather_data(98026, 3)
weather_data(99201, 5)
weather_data(99501, 10)
weather_data(94577, 12)
weather_data(98199, 14)
weather_data(98404, 16)
weather_data(98121, 18)
weather_data(94710, 20)
weather_data(55437, 23)
weather_data(94010, 26)
weather_data(98101, 29)
weather_data(94607, 31)
weather_data(98119, 34)
weather_data(94105, 35)
weather_data(46204, 40)
weather_data(90277, 44)
weather_data(55102, 52)
weather_data(97223, 57)
weather_data(97035, 64)
weather_data(97216, 65)
weather_data(97232, 66)
weather_data(97124, 69)
weather_data(97217, 71)
weather_data(94596, 72)
weather_data(97303, 73)
weather_data(98188, 75)
weather_data(97204, 77)
weather_data(97015, 79)
weather_data(97034, 80)
weather_data(97209, 81)
weather_data(97218, 82)
weather_data(97232, 83)
weather_data(98004, 84)
weather_data(98125, 85)
weather_data(98134, 86)
weather_data(75024, 87)
weather_data(80202, 88)
weather_data(97218, 89)
weather_data(98004, 90)
weather_data(98109, 91)
weather_data(98335, 303)


#print time
print("Script finished running at: ", datetime.datetime.now().time())
