import csv
import psycopg2
import requests
import re
import time
from os import environ

from opencage.geocoder import OpenCageGeocode, UnknownError

'''
Grab latlngs for properties with postcodes.
In the 2017 file, 72,473 out of 99,220 rows geocode by postcode.
Requires a local copy of Code-Point Open, and an OpenCage API key.
'''


def format_address(row):
    address = row['Property_Address'].lower()
    district = row['District'].lower()
    county = row['County'].lower()
    if district not in address:
        address += ', %s' % district
    if county not in address:
        address += ', %s' % county
    addr = re.sub('land .* of ', '', address)
    addr = re.sub('land at ', '', addr)
    return addr


OPENCAGE_KEY = environ.get('OPENCAGE_API_KEY')
opencage_geocoder = OpenCageGeocode(OPENCAGE_KEY)
def geocode_with_opencage(opencage_geocoder, address):
    results = opencage_geocoder.geocode(address, no_annotations=1, countrycode='gb')
    if not len(results):
        time.sleep(10)
        results = opencage_geocoder.geocode(address, no_annotations=1, countrycode='gb')
    r = None
    if results and len(results):
        r = {
            'lat': results[0]["geometry"]["lat"],
            'lng': results[0]['geometry']['lng'],
            'confidence': results[0]['confidence'],
            'formatted': results[0]['formatted'].encode('utf8')
        }
    else:
        r = {
            'lat': None,
            'lng': None,
            'confidence': None,
            'formatted': None
        }
    return r


conn_string = "host='localhost' dbname='codepoint' user='anna'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
def fetch_from_codepoint(postcode):
    lat = None
    lng = None
    q = "SELECT ST_X(ST_Transform(geom, 4326)), ST_Y(ST_Transform(geom, 4326))"
    q += " FROM postcodes WHERE postcode='%s'" % postcode
    cursor.execute(q)
    records = cursor.fetchall()
    if records:
        lat =  records[0][1]
        lng = records[0][0]  
    return lat, lng


reader = csv.DictReader(open('2017.csv', 'rU'))
header = reader.fieldnames
header += ['is_new', 'lat', 'lng', 'confidence', 'formatted']
writer = csv.DictWriter(
    open('2017_geocoded.csv', 'wb'), fieldnames=header)
writer.writeheader()

row_count = 0
postcode_count = 0
opencage_count = 0
for row in reader:
    row_count += 1
    if row_count < 70400:
        continue
    if not row_count % 100:
        print row_count
    postcode = row['Postcode'].replace(' ', '').upper()
    lat = None
    lng = None
    if postcode:
        lat, lng = fetch_from_codepoint(postcode)
        row['lat'] = lat
        row['lng'] = lng
        row['confidence'] = None
        row['formatted'] = None
        postcode_count += 1
    else:
        address = unicode(format_address(row).decode('latin-1'))
        print address
        try:
            r = geocode_with_opencage(opencage_geocoder, address)
        except (UnknownError, requests.exceptions.ConnectionError) as e:
            print 'Error!!!!', address
            r = {'lat': None, 'lng': None, 'confidence': None, 'formatted': None}
        z = row.copy()
        z.update(r) 
        row = z
        opencage_count += 1
    writer.writerow(row)

print 'Geocoded %s out of %s rows with CPO' % (postcode_count, row_count)
print 'Geocoded %s out of %s rows with OpenCage' % (opencage_count, row_count)
