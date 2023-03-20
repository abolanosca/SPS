SELECT replace(cast(date(end_of_week)as string),'-','')  as week_ending_date
from @ds_stg.sps_week_ending_date ;