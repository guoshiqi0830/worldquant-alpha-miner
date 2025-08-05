# WorldQuant Alpha Miner
## initialize the environment
### install python requirements
```shell
pip install -r requirements.txt
```

### create local datadbase
```shell
sh scripts/create_db.sh
```

### add credential
add your credentail to config/credential.json

## prepare data
### refresh data fields
```shell
python main.py -d
```

### add templates
create yaml file templates under templates folder.

add paramters to the {} placeholder.

please refer to the example.yaml


### populate the simulation queue
```shell
python main.py -q [template_id]
```

## start simulation
```shell
python main.py -s [template_id] [--stats] [--shuffle]
```

## submit alpha
```shell
python main.py -S [alpha_id]
```