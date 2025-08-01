from worldquant.api import WorldQuantSession
from worldquant.constants import CHECK_METRIC_MAPPING, Status
from worldquant.utils import load_config

from db.database import SessionLocal
from db.crud.data_field import upsert_data_field
from db.crud.alpha import upsert_alpha, get_alphas, delete_alpha
from db.crud.simulation_queue import delete_queue_by_id
from db.schema.data_field import DataFieldBase
from db.schema.alpha import AlphaBase

from concurrent.futures import ThreadPoolExecutor, as_completed
from pydantic import ValidationError
from loguru import logger
from sqlalchemy import text
from contextlib import contextmanager
from types import SimpleNamespace

import time
import json
import random
import threading


class WorldQuantService():
    def __init__(self):
        self.session = WorldQuantSession()
        self.session_time = time.time()
        self.db = SessionLocal()
        self.db.execute(text('PRAGMA journal_mode=WAL;'))
        self.config = SimpleNamespace(**load_config('simulation'))
        
        self.simulation_cnt = 0
        self.pass_cnt = 0
        self.fail_cnt = 0
        self.avg_sharpe = 0
        self.avg_fitness = 0
        

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
        if res.status_code != 200:
            logger.error(f'fail to fetch alphas {res.code} {res.text}')
            return None
        count = res.json()['count']
        logger.debug(f'search_alpha api found {count} alphas')
        result = []
        batch_size = 100
        for i in range(0, count, batch_size):
            res = self.session.search_alpha(params, limit = batch_size, offset = i).json()
            result += res['results']
            logger.debug(f'search_alpha api fetched {len(result)}')
        return result


    def refresh_datafields(self, params = {
        'region': 'USA',
        'delay': '1',
        'universe': 'TOP3000',
        'instrumentType': 'EQUITY'
    }):
        data_fields = self.get_all_datafields(params)
        for data_field in data_fields:
            try:
                df = DataFieldBase.model_validate({
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
                upsert_data_field(self.db, df)
            except ValidationError as e:
                logger.error(f"Validation failed: {e.errors()}")
                return
        logger.info("All data fields are refreshed to local db!")


    def refresh_alphas(self, start_date = None, end_date = None, status = None):
        positive_params = {
            'is.sharpe>': f'{self.config.sharpe_low}',
            'is.fitness>': f'{self.config.fitness_low}'
        }
        negative_params = {
            'is.sharpe<': f'-{self.config.sharpe_low}',
            'is.fitness<': f'-{self.config.fitness_low}'
        }
        date_params = {}
        if start_date:
            date_params['dateCreated>'] = f'{start_date}T00:00:00+08:00'
        if end_date:
            date_params['dateCreated<'] = f'{end_date}T00:00:00+08:00'
        
        status_param = {}
        if status == Status.ACTIVE.value:
            status_param = { 'status!': 'UNSUBMITTEDIS-FAIL' }
        else:
            status_param = { 'status': 'UNSUBMITTEDIS-FAIL' }
            
        alphas = []
        for params in (positive_params, negative_params):
            alphas += self.get_all_alphas({
                'is.turnover>': f'{self.config.turnover_low}',
                'is.turnover<': f'{self.config.turnover_high}',
                'hidden': 'false',
                **params,
                **date_params,
                **status_param
            })
        logger.info(f'found {len(alphas)} alphas')
        
        for alpha in alphas:
            try:
                settings = alpha.get('settings', {})
                metrics = alpha.get('is', {})
                data = AlphaBase.model_validate({
                    'alpha_id': alpha.get('id'),
                    'expression': alpha.get('regular', {}).get('code'),
                    'type': alpha.get('type'),
                    'instrument_type': settings.get('instrumentType'),
                    'region': settings.get('region'),
                    'universe': settings.get('universe'),
                    'delay': settings.get('delay'),
                    'decay': settings.get('decay'),
                    'neutralization': settings.get('neutralization'),
                    'truncation': settings.get('truncation'),
                    'pasteurization': settings.get('pasteurization'),
                    'unit_handling': settings.get('unitHandling'),
                    'nan_handling': settings.get('nanHandling'),
                    'max_trade': settings.get('maxTrade'),
                    'language': settings.get('language'),
                    'visualization': settings.get('visualization'),
                    'status': alpha.get('status'),
                    'sharpe': metrics.get('sharpe'),
                    'fitness': metrics.get('fitness'),
                    'turnover': metrics.get('turnover'),
                    'drawdown': metrics.get('drawdown'),
                    'long_count': metrics.get('longCount'),
                    'short_count': metrics.get('shortCount'),
                    'returns': metrics.get('returns'),
                    'margin': metrics.get('margin'),
                    'pnl': metrics.get('pnl')
                })
                upsert_alpha(self.db, data, False)
            except ValidationError as e:
                logger.error(f"Validation failed: {e.errors()}")
        logger.info("All alphas are refreshed to local db!")


    def _check_alpha(self, alpha_id):
        try:
            ac_response = self.session.check_alpha(alpha_id)
            if ac_response.status_code == 200:
                if not ac_response.text:
                    return Status.PENDING.value
            elif ac_response.status_code == 429:
                return Status.WAITING.value
            elif ac_response.status_code == 401:
                self.session._sign_in()
                return Status.EXPIRED.value
            else:
                logger.error(f'alpha_id, {alpha_id}, error {ac_response.status_code}, {ac_response.text}')
                return Status.ERROR.value
            alpha_check_list = ac_response.json().get('is').get('checks')
        except Exception as e:
            logger.error(f'fail to check alpha {e}')
            return Status.ERROR.value
        
        alpha_check_dict = {'alpha_id': alpha_id}
        check_status = Status.PASS.value
        for check in alpha_check_list:
            result = check.get('result')
            check_name = check['name']
            if CHECK_METRIC_MAPPING.get(check_name):
                alpha_check_dict[CHECK_METRIC_MAPPING.get(check_name)] = check.get('value')
            if check_name == 'ALREADY_SUBMITTED':
                check_status = Status.ACTIVE.value
                break
            elif result == Status.FAIL.value:
                check_status = Status.FAIL.value

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
        self.simulation_cnt += 1
        sharpe = alpha_metrics.get("sharpe") or 0
        fitness = alpha_metrics.get("fitness") or 0
        if self.avg_sharpe == 0:
            self.avg_sharpe = abs(sharpe)
        else:
            self.avg_sharpe = (self.avg_sharpe * (self.simulation_cnt - 1) + abs(sharpe) ) / self.simulation_cnt
        
        if self.avg_fitness == 0:
            self.avg_fitness = abs(fitness) 
        else:
            self.avg_fitness = (self.avg_fitness * (self.simulation_cnt - 1) + abs(fitness) ) / self.simulation_cnt
        
        if check_status == Status.PASS.value:
            self.pass_cnt += 1
        else:
            self.fail_cnt += 1
        
        logger.debug(f'alpha_id {alpha_id}, expression: {alpha_response.json().get("regular").get("code")}')
        logger.info(f'alpha_id {alpha_id}, sharpe: {sharpe}, fitness: {fitness}')
        if alpha_metrics:
            alpha_check_dict['drawdown'] = alpha_metrics.get('drawdown')
            alpha_check_dict['long_count'] = alpha_metrics.get('longCount')
            alpha_check_dict['margin'] = alpha_metrics.get('margin')
            alpha_check_dict['pnl'] = alpha_metrics.get('pnl')
            alpha_check_dict['returns'] = alpha_metrics.get('returns')
            alpha_check_dict['short_count'] = alpha_metrics.get('shortCount')
        alpha = AlphaBase.model_validate(alpha_check_dict)
        if check_status ==  Status.PASS.value or \
            (sharpe >= self.config.sharpe_low and fitness >= self.config.fitness_low) or \
            (sharpe <=  -self.config.sharpe_low and fitness <= -self.config.fitness_low):
            logger.debug(f"alpha_id {alpha_id}, keep this alpha in local db")
            upsert_alpha(self.db, alpha)
        else:
            logger.debug(f"alpha_id {alpha_id}, discard this alpha")
            delete_alpha(self.db, alpha_id)

        return check_status


    def check_all_alphas(self):
        result = get_alphas(self.db, status= [Status.UNSUBMITTED.value])
        alphas = []
        for row in result:
            alphas.append(row.alpha_id)
        buffer = []
        buffer_size = 2

        while len(alphas) > 0 or len(buffer) > 0:
            current_time = time.time()
            if len(buffer) < buffer_size and len(alphas) > 0:
                alpha = {
                    'alpha_id' : alphas.pop(),
                    'wait_sec': 0,
                    'wait_until': current_time
                }
                buffer.append(alpha)
            buffer.sort(key = lambda x: x.get('wait_until'))
            
            alpha = buffer[0]
            wait_sec = alpha.get('wait_sec')
            wait_until = alpha.get('wait_until')
            alpha_id = alpha.get('alpha_id')
            if wait_until <= current_time:
                buffer.pop(0)
                status = self._check_alpha(alpha_id)
                logger.info(f'alpha_id {alpha_id}, check status {status}')
                if status == Status.PENDING.value:
                    wait_sec = 30 if wait_sec == 0 else wait_sec * 2
                elif status in (Status.WAITING.value, Status.ERROR.value, Status.EXPIRED.value):
                    wait_sec = 30
                else:
                    continue
                new_alpha = {
                    'wait_sec': wait_sec,
                    'wait_until': current_time + wait_sec,
                    'alpha_id': alpha_id
                }
                buffer.append(new_alpha)
            else:
                wait_time = round(wait_until - current_time + 1)
                logger.info(f'alpha {alpha_id} wait for {wait_time} s')
                time.sleep(wait_time)


    def check_one_alpha(self, alpha_id):
        wait_sec = 0
        while True:
            status = self._check_alpha(alpha_id)
            logger.info(f'alpha_id {alpha_id}, check status {status}')
            if status == Status.PENDING.value:
                wait_sec = 30 if wait_sec == 0 else wait_sec * 2
            elif status in (Status.WAITING.value, Status.ERROR.value):
                wait_sec = 30
            else:
                return status
            logger.info(f'alpha_id {alpha_id} wait for {wait_sec} s')
            time.sleep(wait_sec)
        

    def simulate_one(self, simulation):
        with self.session_scope() as db:
            while True:
                response = self.session.post_simulation(simulation)
                if response.status_code < 300:
                    sim_progress_url = response.headers['Location']
                    simulate_id = sim_progress_url.split('/')[-1]
                    logger.debug(f'simulation submitted, simulate_id {simulate_id}')
                    break
                elif response.status_code in (429, ):
                    logger.info(f'{response.status_code}, {response.text}, wait for 30s')
                    time.sleep(30)
                    continue
                elif response.status_code in (401,):
                    self.session._sign_in()
                    logger.info(f'{response.status_code}, {response.text}, sign in again, wait for 30s')
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

            if res.get('status') == Status.COMPLETE.value:
                alpha_id = res.get('alpha')
                logger.debug(f'simulation completed, alpha_id {alpha_id}')
                # upsert simulation
                alpha = AlphaBase.model_validate({
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
                    'expression': res.get('regular'),
                    'alpha_id': alpha_id,
                    'status': Status.UNSUBMITTED.value
                })
                upsert_alpha(db, alpha)
                logger.debug(f'simulation table updated, alpha_id {alpha_id}')

                # update alpha check and status for simulation table
                self.check_one_alpha(alpha_id)

                return alpha_id
            else:
                logger.error(f'Fail to complete simulation for {simulate_id}, status is {res.get("status")}')
                logger.error(f'{res}')
                return None

    
    def print_simulation_status(self, template_id = None):
        where_condition = ''
        if template_id != None:
            where_condition = f"where template_id = {template_id}"
        result = self.db.execute(text(f"select count(*) as cnt from simulation_queue {where_condition}")).first()
        if template_id != None:
            logger.info(f'{result.cnt} alphas for template_id {template_id} in the queue')
        else:
            logger.info(f'{result.cnt} alphas in the queue')
            
        result = self.db.execute(text(f"select count(*) as cnt, status from alpha group by status"))
        for row in result:
            logger.info(f'{row.cnt} {row.status}')

    
    def periodic_print(self, stop_event: threading.Event):
        while not stop_event.is_set():
            logger.info('### statistics for this session:')
            logger.info(f'# pass count {self.pass_cnt}')
            logger.info(f'# fail count {self.fail_cnt}')
            logger.info(f'# average sharpe {round(self.avg_sharpe, 2)}') 
            logger.info(f'# average fitness {round(self.avg_fitness, 2)}')
            time.sleep(self.config.print_interval)


    def simulate_from_alpha_queue(self, template_id = None, shuffle = False, stats = False):
        if template_id != None:
            where_condition = f'where template_id = {template_id}'
            template_info = f'simulate template {template_id}'
        else:
            where_condition  = ''
            template_info = 'all alphas from the queue'
        logger.info(f"start to simulate {template_info}, parallelism {self.config.parallelism}, shuffle {shuffle}")
        
        if stats:
            stop_event = threading.Event()
            t = threading.Thread(target = self.periodic_print, args = (stop_event, ) )
            t.daemon = True
            t.start()

        while True:
            if time.time() - 3600 > self.session_time:
                self.session._sign_in()
                self.session_time = time.time()
            time.sleep(1)
            
            result = self.db.execute(
                text(f"select id, regular, settings, type \
                     from simulation_queue {where_condition} order by id limit {1000}")
            ).all()
            if not result:
                logger.info("no alpha found in the queue, wait for 1 min")
                time.sleep(60)
                continue

            if shuffle:
                result = list(result)
                random.shuffle(result)

            def process_row(row):
                simulation = row._asdict()
                queue_id = simulation['id']
                simulation.pop('id')
                simulation['settings'] = json.loads(simulation['settings'])
                alpha_id = self.simulate_one(simulation)
                if alpha_id:
                    delete_queue_by_id(self.db, queue_id)
                    self.db.commit()

            with ThreadPoolExecutor(max_workers = self.config.parallelism) as executor:
                futures = [executor.submit(process_row, row) for row in result]
                if shuffle:
                    random.shuffle(futures)
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
        check_result = self.check_one_alpha(alpha_id)
        if check_result != Status.PASS.value:
            return False

        status = Status.FAIL.value
        while True:
            response = self.session.submit_alpha(alpha_id)
            if response.status_code < 300:
                logger.info(f'submitted alpha {alpha_id} successfully, {response.status_code}')
                status = Status.ACTIVE.value
                break
            elif response.status_code in (400, 403):
                logger.error(f"fail to sumit alpha {alpha_id}, {response.status_code}, {response.text}")
                if response.status_code == 403 and \
                    response.json().get('is').get('checks')[0].get('name') == 'ALREADY_SUBMITTED':
                    status = Status.ACTIVE.value
                else:
                    status = Status.FAIL.value
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
        alpha = AlphaBase.model_validate(data)
        upsert_alpha(self.db, alpha)
        logger.info(f"simulation status updated")
        return True if response.status_code < 300 else False


    def find_and_sumbit_alpha(self, count = 1, order_by = 'sharpe', direction = 'asc'):
        result = self.db.execute(
            text(f"select alpha_id from alpha where status  = '{Status.PASS.value}' order by {order_by} {direction}")
        ).all()
        submitted_count = 0
        for row in result:
            logger.info(f'found a submittable alpha {row.alpha_id}')
            res = self.submit_alpha(row.alpha_id)
            if res:
                logger.info(f'alpha {row.alpha_id} submitted')
                submitted_count += 1
            else:
                logger.info(f'fail to submit alpha {row.alpha_id}')

            if submitted_count >= count:
                logger.info(f'Submitted {count} alphas')
                break


    def check_negative_direction(self):
        result = self.db.execute(
            text(f"select * from alpha where sharpe <= -{self.config.sharpe_pass} and fitness <= -{self.config.fitness_pass}")
        )
        
        for row in result:
            old_expression = row.expression
            lines =  [line for line in old_expression.split('\n') if line]
            new_expression = '\n'.join(
                [ line if index != len(lines) - 1 else f'-{line}' for index, line in enumerate(lines) ])
            simulation = {
                'regular': new_expression,
                'settings': {
                    'instrumentType': row.instrument_type,
                    'region': row.region,
                    'universe': row.universe,
                    'delay': row.delay,
                    'decay': row.decay,
                    'neutralization': row.neutralization,
                    'truncation': row.truncation,
                    'pasteurization': row.pasteurization,
                    'unitHandling': row.unit_handling,
                    'nanHandling': row.nan_handling,
                    'language': row.language,
                    'visualization': row.visualization
                },
                'type': row.type
            }
            logger.debug(f'new_expression {new_expression}')
            alpha_id = self.simulate_one(simulation)
            if alpha_id:
                delete_alpha(self.db, row.alpha_id)
                self.db.commit()
                logger.debug(f'deleted negative alpha {row.alpha_id}')
