import requests
from bs4 import BeautifulSoup
import sqlite3
import json
import datetime
import pandas as pd

conn = sqlite3.connect("World_Economies.db")
        
c = conn.cursor()

c.execute("""create table if not exists economies (
            country text,
            gdp_usd integer
                  );""")

url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"

response = requests.get(url).text

doc = BeautifulSoup(response, "html.parser")

table = doc.find_all("table")

rows = table[2].find_all("tr")

all_data = []

for row in rows[3:]:
    country_dict = {}
    data = row.find_all("td")
    country = data[0].get_text().replace("\xa0", "")
    country_dict["country"] = country
    region = data[1].get_text()
    country_dict["region"] = region
    gdp = None
    try:
        gdp = int(data[2].get_text().replace(",", ""))
        country_dict["gdp"] = gdp
    except:
        country_dict["gdp"] = None
    try:
        year = data[3].get_text()
        year = year.replace("[n 0]", "")
        year = year.replace("[n 1]", "")
        year = year.replace("[n 2]", "")
        year = year.replace("[n 3]", "")
        year = year.replace("[n 4]", "")
        year = year.replace("[n 5]", "")
        year = year.replace("[n 6]", "")
        year = year.replace("[n 7]", "")
        year = year.replace("[n 8]", "")
        year = year.replace("[n 9]", "")
        country_dict["year"] = year
    except:
        country_dict["year"] = None

    all_data.append(country_dict)

    with open("etl_project_log.txt", "a") as file:
        file.write(f"Country {country} of Region {region} data entered at {datetime.datetime.now()}")
        file.close()

    c.execute("INSERT INTO economies (country, gdp_usd) VALUES (?, ?)", [country, gdp])

with open("Countries_by_GDP.json", "a") as file:
        json.dump(all_data, file)

csv_data = {"country": [],"region": [], "gdp": [], "year": []}

for i in all_data:
    csv_data["country"].append(i["country"])
    csv_data["region"].append(i["region"])
    csv_data["gdp"].append(i["gdp"])
    csv_data["year"].append(i["year"])

df = pd.DataFrame(csv_data)
df.to_csv("Countries_by_GDP.csv")

if __name__=="__main__":
    c.execute("SELECT * FROM economies WHERE gdp_usd > 100")
    rows = c.fetchall()

    for i in rows:
        print(i)


