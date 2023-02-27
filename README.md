# momento-py-csv-ingest

```bash
sudo pip3 install pipenv
pipenv install

# generate the data
pipenv run python ./datagen.py

export MOMENTO_AUTH_TOKEN=<YOUR_TOKEN_HERE>

# caveman approach to running 8 copies of the ingest code; multiprocessing would be cleaner
for i in {1..8}; do echo "Launching run $i"; pipenv run python ./ingest.py $i ./data/data_100k.csv & done
```