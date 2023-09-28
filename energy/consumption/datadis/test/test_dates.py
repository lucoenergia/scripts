from datetime import datetime
import pytz

date= "2023/07/01"
time = "13:00"


# "24:00" is not a valid time in the "HH:MM" format as required by the %H:%M format specifier in the strptime method.
# In the "HH:MM" format, the hour should be between 00 and 23.
# To represent midnight at the end of a day, we use "00:00" instead of "24:00".
if time == "24:00":
    time = "00:00"

# Concatenate date and time strings with a space in between
datetime_str = f"{date} {time}"

# Parse the concatenated string into a datetime object
datetime_obj = datetime.strptime(datetime_str, "%Y/%m/%d %H:%M")    

# Set Madrid timezone to date
localized_datetime:datetime = pytz.timezone('Europe/Madrid').localize(datetime_obj)

# Convert CEST to UTC
utc_datetime = localized_datetime.astimezone(pytz.utc)

# Format the datetime object as an InfluxDB-compatible timestamp string (RFC3339 format)
result = utc_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

print(result)