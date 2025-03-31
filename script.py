from openaq import OpenAQ
from datetime import datetime
import pytz

client = OpenAQ(api_key="932148dc9fced6a1df5c6d006c2ab3ae249eb6076ad539c693487236ace264dc")

countries = client.countries.list()

for cry in countries.results:
    print(cry.name, cry.id)

notEmpty = True
page = 1
while notEmpty:
    locs = client.locations.list(limit=1000, page=page)
    print("Got locs:", len(locs.results))
    notEmpty = len(locs.results) > 0
    page += 1
cnt = 0
#for (i, loc) in enumerate(locs.results):
#    if loc.datetime_last is None:
#        continue
#    utc = pytz.UTC
#    print(datetime.fromisoformat(loc.datetime_last.local) > utc.localize(datetime.fromisoformat('2025-01-01')))
#    print(i, loc.name, loc.id)
#    if datetime.fromisoformat(loc.datetime_last.local) > utc.localize(datetime.fromisoformat('2025-01-01')):
#        cnt += 1
#print("cnt: ", cnt)

loc_resp = client.locations.get(10667)
#loc_resp.results[0].sensors[0].
print(loc_resp)
# sensor id: 23021
#id=23424
#id=22060
#id=22182

mnts = client.measurements.list(11438904, datetime_from='2022-02-27', limit=10)
print(mnts)
print("measurements made today", len(mnts.results))
for mnt in mnts.results:
    val = 0
    if mnt.summary is not None:
        val = mnt.summary.max

    print(mnt.parameter, mnt.value, val, mnt.coverage.datetime_from, mnt.coverage.datetime_to)