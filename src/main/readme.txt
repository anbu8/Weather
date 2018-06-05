Overview

-------------

This is a toy model of the environment that uses historical weather data to estimate the current weather conditions at various locations (ie weather stations), and then emits that data, as in the following: 

SYD|-33.86,151.21,39|2015-12-23T05:02:12Z|Rain|+12.5|1004.3|97
MEL|-37.83,144.98,7|2015-12-24T15:30:55Z|Snow|-5.3|998.4|55
ADL|-34.92,138.62,48|2016-01-03T12:35:37Z|Sunny|+39.4|1114.1|12
with a three letter IATA code used as a station label.



The source code could be run and test in both sbt console and eclipse-scala.



Implementation :



Since the data needs to include an IATA code to identify the city, I have chosen 10 major airports in Australia, distributed across various time zones and locations (Coastal, Desert, Islands etc).



For historical weather information, the National Climatic Data Center has the most comprehensive dataset with weather information available for the past 150+ years.

Refer to the https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/readme.txt for further details on the data set.



The hourly temperature has been estimated by using the daily maximum and minimum temperature from the previous year and fitting it to a simple cosine curve.



Output :



When a particular city is chosen, the program will output the data based on the current local time and date.

The data will be updated on an hourly basis