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

    #parse JSON values
    try:
        weather_main_description = api["weather"][0]["main"]
    except:
        weather_main_description = None

    try:
        weather_description = api["weather"][0]["description"]
    except:
        weather_description = None

    try:
        temp = (float(api["main"]["temp"]) - 273.15) * 9/5 + 32
    except:
        temp = None

    try:
        temp_min = (float(api["main"]["temp_min"]) - 273.15) * 9/5 + 32
    except:
        temp_min = None

    try:
        temp_max = (float(api["main"]["temp_max"]) - 273.15) * 9/5 + 32
    except:
        temp_max = None

    try:
        grnd_level = api["main"]["pressure"]
    except:
        grnd_level = None

    try:
        sea_level = api["main"]["sea_level"]
    except:
        sea_level = None

    try:
        humidity = api["main"]["humidity"]
    except:
        humidity = None

    try:
        visibility = float(api["visibility"]) / 1609.344
    except:
        visibility = None

    try:
        windspeed = float(api["wind"]['speed']) * 2.237
    except:
        windspeed = None

    try:
        winddeg = api["wind"]['deg']
    except:
        winddeg = None

    try:
        cloudiness = api["clouds"]['all']
    except:
        cloudiness = None

    try:
        rainlast3hour = float(api["rain"]['1h']) /  25.4
    except:
        rainlast3hour = None

    try:
        snowlast3hour = float(api["snow"]['3h']) /  25.4
    except:
        snowlast3hour = None

    try:
        dt = time.strftime('%H:%M:%S', time.localtime(api["dt"]))
    except:
        dt = None

    try:
        sunset = time.strftime('%H:%M:%S', time.localtime(api['sys']["sunset"]))

    except:
        sunset = None

    try:
        sunrise = time.strftime('%H:%M:%S', time.localtime(api['sys']["sunrise"]))

    except:
        sunrise = None

    try:
        name = api["name"]
    except:
        name = None

    query = "INSERT INTO actual_weather_data VALUES (?, getdate(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
    values = (restnum, weather_main_description, weather_description, temp, temp_min, temp_max, grnd_level, sea_level, humidity, visibility, windspeed, winddeg, cloudiness, rainlast3hour, snowlast3hour, dt, sunset, sunrise, name, zipcode)
    cursor.execute(query, values)            
    cnxn.commit()

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
