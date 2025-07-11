from worldquant.api import WorldQuantSession

from db.database import SessionLocal
from db.crud.data_field import upsert_data_field
from db.crud.simulation import upsert_simulation, get_simulations
from db.crud.data_field import get_data_fields_by_criteria
from db.crud.alpha_queue import delete_alpha_queue_by_template_id, insert_alpha_queue, delete_alpha_queue_by_id
from db.crud.alpha_template import upsert_alpha_template
from db.schema.data_field import DataFieldCreate
from db.schema.simulation import SimulationCreate, SimulationUpdate

from concurrent.futures import ThreadPoolExecutor, as_completed
from pydantic import ValidationError
from loguru import logger
from sqlalchemy import text
from itertools import product
from contextlib import contextmanager

import time
import json
import os
import re


class WorldQuantService():
    def __init__(self):
        self.session = WorldQuantSession()
        self.session_time = time.time()
        self.db = SessionLocal()
        self.check_metric_mapping = {
            'LOW_SHARPE': 'sharpe',
            'LOW_FITNESS': 'fitness',
            'LOW_TURNOVER': 'turnover',
            'LOW_SUB_UNIVERSE_SHARPE': 'sub_universe_sharpe',
            'CONCENTRATED_WEIGHT': 'concentrated_weight',
            'SELF_CORRELATION': 'self_correlation',
        }

    @contextmanager
    def session_scope(self):
        db = SessionLocal()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            SessionLocal.remove()

    def get_all_datafields(self, params):
        res = self.session.get_datafields(params).json()
        count = res.get('count')

        logger.info(f'found {count} datafields')
        result = []
        for i in range(0, count, 50):
            res = self.session.get_datafields(params, 50, i).json()
            result += res['results']
        return result
    

    def get_all_alphas(self, params):
        res = self.session.search_alpha(params)
        count = res['count']
        logger.info(f'found {count} alphas')
        result = []
        for i in range(0, count, 100):
            res = self.session.search_alpha(params, 100, i)
            result += res['results']
        return result


    def refresh_datafields(self, 
                           params = {'region': 'USA', 'delay': '1', 'universe': 'TOP3000', 'instrumentType': 'EQUITY'}):
        data_fields = self.get_all_datafields(params)
        for data_field in data_fields:
            try:
                dataFieldCreate = DataFieldCreate.model_validate({
                    "field_name": data_field["id"],
                    **{k: v for k, v in data_field.items() if k != "id"},
                    "dataset_id": data_field.get("dataset", {}).get("id"),
                    "dataset_name" : data_field.get('dataset', {}).get('name'),
                    "category_id" : data_field.get('category', {}).get('id'),
                    "category_name" : data_field.get('category', {}).get('name'),
                    "subcategory_id" : data_field.get('subcategory', {}).get('id'),
                    "subcategory_name" : data_field.get('subcategory', {}).get('name'),
                    "user_count" : data_field.get('userCount'),
                    "alpha_count" : data_field.get('alphaCount')
                })
                upsert_data_field(self.db, dataFieldCreate)
            except ValidationError as e:
                print(f"Validation failed: {e.errors()}")
        logger.info("All data fields are refreshed to local db!")


    def get_templates(self, template_id = None):
        templates = []
        for file in os.listdir('templates'):
            id, description = file[:-4].split('-')
            if template_id and template_id != id:
                continue
            with open(f'templates/{file}', 'r') as f:
                template = {
                    'id': id,
                    'template': f.read(),
                    'description': description
                }
                templates.append(template)
        return templates


    def populate_alpha_queue(self, template_id = None, append = False):
        if not template_id:
            result = self.get_templates(template_id)
            for template in result:
                self.populate_alpha_queue_by_template_id(template.id, append)
        else:
            self.populate_alpha_queue_by_template_id(template_id, append)


    def populate_alpha_queue_by_template_id(self, template_id, append):
        if not append:
            delete_alpha_queue_by_template_id(self.db, template_id)
        settings = {}
        with open('config/settings.json') as f:
            settings = json.dumps(json.load(f)[0]['settings'])
        template = self.get_templates(template_id)[0].get('template')
        params_list =  list(set(re.findall(r'{(.*?)}', template)))
        params = [
            {
                'filter': dict(pair.split("=") for pair in param.split(",") if '=' in pair),  
                'param': param
            } 
            for param in params_list
        ]
        param_data_field = []
        for p in params:
            data_fields = get_data_fields_by_criteria(self.db, **p['filter'])
            param_data_field.append({ 'data_fields': [ data_field.field_name for data_field in data_fields ],
                                    'param': p['param'] })
        param_values = [ d['param'] for d in param_data_field]
        data_field_lists = [d['data_fields'] for d in param_data_field]
        logger.info(f'param count, {[(param_values[i] ,len(data_field_lists[i])) for i in range(len(data_field_lists))]}')
        params_comb = []
        for b_combination in product(*data_field_lists):
            params_comb.append(dict(zip(param_values, b_combination)))

        logger.info(f'found {len(params_comb)} alphas')

        for params in params_comb:
            expression = template
            for key in params:
                expression = str.replace(expression, f'{{{key}}}' , params[key])
            insert_alpha_queue(self.db, expression, settings, template_id = template_id, template = template, 
                               params = json.dumps(params))

        logger.info(f'inserted {len(params_comb)} alphas')


    def check_alpha(self, alpha_id):
        wait_sec = 60
        retries = 0
        max_retries = 3
        while retries < max_retries:
            ac_response = self.session.check_alpha(alpha_id)
            if ac_response.status_code == 200:
                if ac_response.text:
                    break
                else:
                    logger.warning(f'alpha_id, {alpha_id}, {ac_response.status_code}, wait for {wait_sec} s and try again')
                    time.sleep(wait_sec)
                    wait_sec *= 2
                    max_retries += 1
            elif ac_response.status_code == 429:
                logger.warning(f'alpha_id, {alpha_id}, {ac_response.status_code}, {ac_response.text}, wait for 30 s and try again')
                time.sleep(30)
            else:
                logger.error(f'alpha_id, {alpha_id}, error {ac_response.status_code}, {ac_response.text}')
                return
        
        try:
            alpha_check_list = ac_response.json().get('is').get('checks')
        except Exception as e:
            logger.error(f'{ac_response.status_code, ac_response.text}')
            return
        
        alpha_check_dict = {'alpha_id': alpha_id}
        check_status = 'PASS'
        for check in alpha_check_list:
            result = check.get('result')
            check_name = check['name']
            if self.check_metric_mapping.get(check_name):
                alpha_check_dict[self.check_metric_mapping.get(check_name)] = check.get('value')
            if check_name == 'ALREADY_SUBMITTED':
                check_status = 'SUBMITTED'
                break
            elif result == 'FAIL':
                check_status = 'FAIL'

        alpha_check_dict['status'] = check_status

        while True:
            alpha_response = self.session.get_alpha(alpha_id)
            if alpha_response.status_code == 200:
                break
            else:
                logger.warning(
                    f'fail to call get_alpha api, {alpha_response.status_code}, wait for 5 s and try again')
                time.sleep(5)

        alpha_metrics = alpha_response.json().get('is')
        if alpha_metrics:
            alpha_check_dict['drawdown'] = alpha_metrics.get('drawdown')
            alpha_check_dict['long_count'] = alpha_metrics.get('longCount')
            alpha_check_dict['margin'] = alpha_metrics.get('margin')
            alpha_check_dict['pnl'] = alpha_metrics.get('pnl')
            alpha_check_dict['returns'] = alpha_metrics.get('returns')
            alpha_check_dict['short_count'] = alpha_metrics.get('shortCount')

        simulationUpdate = SimulationUpdate.model_validate(alpha_check_dict)
        upsert_simulation(self.db, simulationUpdate)
        logger.info(f'simulation table updated for alpha check, alpha_id {alpha_id}, status {check_status}')
        return check_status


    def check_all_alpha(self):
        result = get_simulations(self.db, status= ['COMPLETE'])
        for row in result:
            self.check_alpha(row.alpha_id)


    def simulate_one(self, alpha):
        with self.session_scope() as db:
            while True:
                response = self.session.post_simulation(alpha)
                if response.status_code < 300:
                    sim_progress_url = response.headers['Location']
                    simulate_id = sim_progress_url.split('/')[-1]
                    logger.info(f'Simulation submitted, simulate_id {simulate_id}')
                    break
                elif response.status_code in (429, 401):
                    logger.info(f'{response.status_code}, {response.text}, wait for 30s')
                    time.sleep(30)
                    continue
                else:
                    logger.error(f'{response.status_code}, {response.text}')
                    return None
            
            while True:
                res = self.session.get_simulation_status(simulate_id).json()
                if res.get('progress'):
                    time.sleep(10)
                else:
                    break

            if res.get('status') == 'COMPLETE':
                alpha_id = res.get('alpha')
                logger.info(f'Simulation completed, alpha_id {alpha_id}')
                # upsert simulation
                simulationCreate = SimulationCreate.model_validate({
                    'type': res.get('type'),
                    'instrument_type': res.get('settings').get('instrumentType'),
                    'region': res.get('settings').get('region'),
                    'universe': res.get('settings').get('universe'),
                    'delay': res.get('settings').get('delay'),
                    'decay': res.get('settings').get('decay'),
                    'neutralization': res.get('settings').get('neutralization'),
                    'truncation': res.get('settings').get('truncation'),
                    'pasteurization': res.get('settings').get('pasteurization'),
                    'unit_handling': res.get('settings').get('unitHandling'),
                    'nan_handling': res.get('settings').get('nanHandling'),
                    'max_trade': res.get('settings').get('maxTrade'),
                    'language': res.get('settings').get('language'),
                    'visualization': res.get('settings').get('visualization'),
                    'alpha': res.get('regular'),
                    'alpha_id': alpha_id,
                    'status': res.get('status'),
                    'simulation_id': res.get('id')
                })
                upsert_simulation(db, simulationCreate)
                logger.debug(f'simulation table updated, alpha_id {alpha_id}')

                # update alpha check and status for simulation table
                self.check_alpha(alpha_id)

                return alpha_id
            else:
                logger.warning(f'Fail to complete simulation for {simulate_id}, status is {res.get('status')}')
                logger.warning(f'{res}')
                return None


    def simulate_from_alpha_queue(self, template_id = None, parallelism = 3):
        where_condition = ''
        if template_id:
            logger.info(f"start to simulate template {template_id}, parallelism {parallelism}")
            where_condition = f"where template_id = {template_id}"
        else:
            logger.info(f"start to simulate all alphas from alpha queue")

        while True:
            if time.time() - 3600 > self.session_time:
                self.session._sign_in()
                self.session_time = time.time()
            time.sleep(1)
            
            result = self.db.execute(
                text(f"select id, regular, settings, type, template, params \
                     from alpha_queue {where_condition} limit {100}")
            ).all()
            if not result:
                logger.info("no alpha found in the queue, wait for 1 min")
                time.sleep(60)
                continue

            def process_row(row):
                alpha = row._asdict()
                queue_id = alpha['id']
                template = alpha['template']
                params = alpha['params']
                alpha.pop('id')
                alpha.pop('template')
                alpha.pop('params')
                alpha['settings'] = json.loads(alpha['settings'])
                alpha_id = self.simulate_one(alpha)
                if alpha_id:
                    upsert_alpha_template(self.db, alpha_id, template, params)
                    logger.info(f"alpha_template updated for {alpha_id}")
                    delete_alpha_queue_by_id(self.db, queue_id)
                    self.db.commit()

            with ThreadPoolExecutor(max_workers = parallelism) as executor:
                futures = [executor.submit(process_row, row) for row in result]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"task failed: {e}")
                # executor.map(process_row, result)

            # single thread test
            # for row in result:
            #     process_row(row)

            time.sleep(1)


    def submit_alpha(self, alpha_id):
        check_result = self.check_alpha(alpha_id)
        if check_result != 'PASS':
            return False

        status = 'FAIL'
        while True:
            response = self.session.submit_alpha(alpha_id)
            if response.status_code < 300:
                logger.info(f'submitted alpha {alpha_id} successfully, {response.status_code}')
                status = 'SUBMITTED'
                break
            elif response.status_code in (400, 403):
                logger.error(f"fail to sumit alpha {alpha_id}, {response.status_code}, {response.text}")
                if response.status_code == 403 and \
                    response.json().get('is').get('checks')[0].get('name') == 'ALREADY_SUBMITTED':
                    status = 'SUBMITTED'
                else:
                    status = 'FAIL'
                break
            elif response.status_code == 429:
                logger.warning(f"{response.status_code}, {response.text}, wait for 30 s and try again")
                time.sleep(30)
                continue
            else:
                logger.warning(f"unkown status {response.status_code}, {response.text}")
                return False

        data = {
            "alpha_id": alpha_id,
            "status": status
        }
        simulation = SimulationUpdate.model_validate(data)
        upsert_simulation(self.db, simulation)
        logger.info(f"simulation status updated")
        return True if response.status_code < 300 else False


    def find_and_sumbit_alpha(self, count = 1, order_by = 'sharpe', direction = 'asc'):
        result = self.db.execute(
                text(f"select alpha_id from simulation where status  = 'PASS' order by {order_by} {direction}")
            ).all()
        
        submitted_count = 0
        for row in result:
            logger.info(f'find a submittable alpha {row.alpha_id}')
            res = self.submit_alpha(row.alpha_id)
            if res:
                logger.info(f'alpha {row.alpha_id} submitted')
                submitted_count += 1
            else:
                logger.info(f'fail to submit alpha {row.alpha_id}')

            if submitted_count >= count:
                logger.info(f'Submitted {count} alphas')
                break
