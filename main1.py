import time
import requests
import datetime
import pandas as pd
from datetime import timedelta

'''
PkRzeszPilsu-PM2.5-1 - 20277
PkRzeszPilsu-PM10-1g - 16344
PkRzeszPilsu-CO-1g - 16343
PkRzeszPilsu-NO2-1g - 16319
PkRzeszRejta-O3-1g -4385
PkRzeszRejta-SO2-1 - 4391
'''
api_key = "382BF27FHEE7GQ5Q2HNQJ83E5"

apiLinks = ["https://api.gios.gov.pl/pjp-api/v1/rest/data/getData/20277", "https://api.gios.gov.pl/pjp-api/v1/rest/data/getData/16344",
            "https://api.gios.gov.pl/pjp-api/v1/rest/data/getData/16343", "https://api.gios.gov.pl/pjp-api/v1/rest/data/getData/16319",
            "https://api.gios.gov.pl/pjp-api/v1/rest/data/getData/4385", "https://api.gios.gov.pl/pjp-api/v1/rest/data/getData/4391"]


historicApiLinks = ["https://api.gios.gov.pl/pjp-api/v1/rest/archivalData/getDataBySensor/20277",
                    "https://api.gios.gov.pl/pjp-api/v1/rest/archivalData/getDataBySensor/16343",
                    "https://api.gios.gov.pl/pjp-api/v1/rest/archivalData/getDataBySensor/4391",
                    "https://api.gios.gov.pl/pjp-api/v1/rest/archivalData/getDataBySensor/16344",
                    "https://api.gios.gov.pl/pjp-api/v1/rest/archivalData/getDataBySensor/4385",
                    "https://api.gios.gov.pl/pjp-api/v1/rest/archivalData/getDataBySensor/16319"]



def getTodaysData():
    todayData = {
        "Date": [],
    }
    for sensor in apiLinks:
        dateList = []
        valueList = []
        response = requests.get(sensor + "?size=24").json()
        data = response['Lista danych pomiarowych']
        for i in range(len(data)):
            if pd.to_datetime(data[i]['Data']).day == datetime.datetime.now().day:
                dateList.append(data[i]['Data'])
                valueList.append(data[i]['Wartość'])

            if len(todayData["Date"]) == 0:
                todayData.update({"Date": dateList})
            todayData.update({data[i]['Kod stanowiska']: valueList})

    return pd.DataFrame(todayData).sort_values(by=['Date'])

def getHistoricDataOnly20Days(dateFrom, dateTo):

    dateF = pd.to_datetime(dateFrom)
    dateT = pd.to_datetime(dateTo)
    dateFromHour = dateF.time().hour
    dateFromMinute = dateF.time().minute
    dateToHour = dateT.time().hour
    dateToMinute = dateT.time().minute


    if (dateT - dateF).days > 20:
        return "Date range must be less or equal 20 days!! Try again."

    if dateF.time().hour < 10:
        dateFromHour = "0" + str(dateF.time().hour)

    if dateF.time().minute < 10:
        dateFromMinute = "0" + str(dateF.time().minute)

    if dateT.time().hour < 10:
        dateToHour = "0" + str(dateT.time().hour)

    if dateF.time().minute < 10:
        dateToMinute = "0" + str(dateT.time().minute)

    historicData = {}

    dateList = []
    dateList.append(str(dateF))

    k = 0

    while k < (dateT - dateF).total_seconds() / 3600:
        dateList.insert(k + 1, str(pd.to_datetime(dateList[k]) + datetime.timedelta(hours=1)))
        k += 1

    historicData.update({"Date": dateList})

    for sensor in historicApiLinks:
        valueList = []
        time.sleep(15)
        response = requests.get(f'{sensor}?size=500&dateFrom={dateF.date()}%20{dateFromHour}%3A{dateFromMinute}'
                                f'&dateTo={dateT.date()}%20{dateToHour}%3A{dateToMinute}').json()

        data = response['Lista archiwalnych wyników pomiarów']

        for date in dateList:
            for i in range(len(data)):
                if date == (data[i]['Data']):
                    valueList.append(data[i]['Wartość'])
                    break
                elif i == len(data)-1:
                    valueList.append("")

        historicData.update({data[i]['Kod stanowiska']: valueList})

    return pd.DataFrame(historicData)

#getHistoricDataOnly20Days("2023-03-20 00:00", "2023-04-07 00:00").to_csv('HistoricDataOnly20Days.csv', index=False)
getTodaysData().to_csv('TodaysData.csv', index=False)

def getTodaysVisualCrossingData(api_key: str, location: str = "Rzeszow,PL"):
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/today"
    params = {
        "unitGroup": "metric",
        "include": "hours",
        "key": api_key,
        "contentType": "json"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    hours = [hour for hour in data["days"][0]["hours"] if hour.get("source") == "obs"] 

    df = pd.json_normalize(hours)
    date = data["days"][0]["datetime"]
    df["Date"] = pd.to_datetime(date + " " + df["datetime"])
    df.drop(columns=["datetime"], inplace=True)
    df = df[["Date"] + [col for col in df.columns if col != "Date"]]

    df.to_csv("visualCrossing_today_obs.csv", index=False)
    print("Saved only 'obs' data to visualCrossing_today_obs.csv")

def getHistoricDataOnly20DaysVisualCrossing(api_key: str, location: str = "Rzeszow,PL"):
    end_date = datetime.datetime.now().date()
    start_date = end_date - timedelta(days=19) 

    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{start_date}/{end_date}"
    params = {
        "unitGroup": "metric",
        "include": "hours",
        "key": api_key,
        "contentType": "json"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    all_obs_rows = []

    for day in data["days"]:
        date = day["datetime"] 
        for hour in day.get("hours", []):
            if hour.get("source") == "obs":
                full_datetime = f"{date} {hour['datetime']}"
                hour_data = hour.copy()
                hour_data["Date"] = pd.to_datetime(full_datetime)
                del hour_data["datetime"]
                all_obs_rows.append(hour_data)

    df = pd.DataFrame(all_obs_rows)
    df = df[["Date"] + [col for col in df.columns if col != "Date"]]

    df.to_csv("visualCrossing_past20days_obs.csv", index=False)
    print("Saved past 20 days of observed data to visualCrossing_past20days_obs.csv")

getTodaysVisualCrossingData(api_key)
getHistoricDataOnly20DaysVisualCrossing(api_key)