from os.path import join
from pathlib import Path

BASE_FOLDER = join(
       str(Path("~/data_engineer").expanduser()),
       "datalake/{stage}/twitter_datascience/{partition}",
   )

print(BASE_FOLDER.format(stage="Bronze", partition="PARTITION_FOLDER_EXTRACT"))