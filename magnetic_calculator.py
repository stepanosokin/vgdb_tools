import requests

lat_wgs84 = 51.557901
lon_wgs84 = 45.760180
result_file = 'magnetic_result.csv'
day = '23'
month = '1'
year = '2023'
key = 'zNEw7'   # key received to registered email at https://www.ngdc.noaa.gov/geomag
format = 'csv'
# format = 'json'
# format = 'html'

with requests.Session() as s:
    url = 'https://www.ngdc.noaa.gov/geomag-web/calculators/calculateDeclination' \
          '?browserRequest=true' \
          '&magneticComponent=d' \
          f'&key={key}' \
          f'&lat1={lat_wgs84}' \
          '&lat1Hemisphere=N' \
          f'&lon1={lon_wgs84}' \
          '&lon1Hemisphere=E' \
          '&model=WMM' \
          f'&startYear={year}' \
          f'&startMonth={month}' \
          f'&startDay={day}' \
          f'&resultFormat={format}'
    result = s.get(url)
    with open(result_file, 'w', encoding='utf8', newline='') as f:
        f.write(result.text)