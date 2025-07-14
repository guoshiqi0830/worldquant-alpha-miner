# WorldQuant Alpha Miner
## initialize the environment
### python virtual environment
```shell
python -m venv worldquant_venv
source worldquant_venv/bin/activate
pip install -r requirements.txt
```

### create local datadbase
```shell
sh scripts/create_db.sh
```

### add credential
add your credentail to worldquant/credential.json

## prepare data
### refresh data fields
```shell
python main.py -d
```

### add templates
create templates under templates folder with file name format <template_id>-<template_name>.txt

add paramters to the {} placeholder, you can use the filter of data_field table. 

e.g. {dataset_id=fundamental2,type=MATRIX}


### populate alpha queue
```shell
python main.py -q <template_id>
```

## start simulation
```shell
python main.py -s <template_id>
```

## submit alpha
```shell
python main.py -S <alpha_id>
```