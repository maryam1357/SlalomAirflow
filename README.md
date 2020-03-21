# SlalomAirflow
Slalom - Airflow Study Group Summary - Goals (WIP)

## Data sources
- I94 Immigration Data: This data comes from the US National Tourism and Trade Office [Source](https://travel.trade.gov/research/reports/i94/historical/2016.html). This data records immigration records partitioned by month of every year.
- World temperature Data: This dataset comes from [Kaggle Source](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data). Includes temperature recordings of cities around the world for a period of time
- US City Demographic Data: This dataset comes from [OpenSoft Source](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). Includes population formation of US states, like race and gender.
- Aiport Code table: [Source](https://datahub.io/core/airport-codes#data). Includes a collection of airport codes and their respective cities, countries around the world.

## Configuration
WIP
## Deployment
WIP
##ETL
WIP
## Table designs
1. Normalized us city: built on city code data from raw airport and demographics data
2. Normalized us airport: built on raw airport data, filtered for US airports, joined with ``city`` table to get ``city_id``
    - Each airport can either be identified by ``icao_code`` or ``iata_code`` or both
3. Normalized country: built on country codes from I94 immigration dictionary
4. Normalized us state code: built on state codes from I94 immigration dictionary
5. Normalized us weather: built on global weather data, filtered for US cities, joined with ``city`` table to get ``city_id``
    - Filtered for weather data in the US at the latest date
    - The raw weather data only has temperatures on a fraction of all the cities in the US
    - Some cities in the data are duplicates, but that means they're on different states of the US. However the state is not available in the data, but instead we have the latitude - longitude coordinates. This issue is currently NOT addressed in this project, but in a production setting,we should join the latitude - longitude from weather dataset with this [data](https://simplemaps.com/data/us-cities), which includes city coordinates and their respective states
6. Normalized us demographics: built on raw demographics data, not much transform is needed
7. Denormalized airport weather: Joining weather data with airport location, to get the respective weather information for each US airports
8. Normalized immigrant: Information about individual immigrants, like age, gender, occupation, visa type, built on I94 Immigration dataset
9. Normalized immigration table: Information about immigration information, such as date of arrival, visa expiry date, airport, means of travel
10. Denormalized immigration demographics: Joining immigration with demographics data, to get the population of places where immigrants go
