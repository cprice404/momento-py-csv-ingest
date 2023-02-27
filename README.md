# momento-py-csv-ingest

```bash
sudo pip3 install pipenv
pipenv install

# generate the data
pipenv run python ./datagen.py

export MOMENTO_AUTH_TOKEN=<YOUR_TOKEN_HERE>

# caveman approach to running 8 copies of the ingest code; multiprocessing would be cleaner
for i in {1..8}; do echo "Launching run $i"; pipenv run python ./ingest.py $i ./data/data_100k.csv & done

# this script will pull a random sampling of the ingested data (0.5% of all of the ingested values) just
# to do a sanity check that they were ingested successfully; will crash if any values are missing
pipenv run python ./verify.py 8 100000
```