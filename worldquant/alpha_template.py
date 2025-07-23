import os
import yaml
import re
import json

from loguru import logger
from itertools import product

from db.crud.data_field import get_data_fields_by_criteria
from db.crud.simulation_queue import insert_queue, delete_queue_by_template_id
from db.database import SessionLocal

class AlphaTemplate():
    def __init__(self, template_id):
        self.template_id = None
        self.template_expression = ''
        self.template_parameters = {}
        self.template_settings = {}
        self.default_settings = {}
        self.settings = {}
        self.parameters = {}

        self.db = SessionLocal()
        self._get_default_settings()
        self._read_template(template_id)


    def _read_template(self, template_id):
        template = {}
        for file in os.listdir('templates'):
            m = re.match(r'(.*)\.yaml', file)
            if not m:
                continue
            with open(f'templates/{file}', 'r') as f:
                template = yaml.safe_load(f)
                if str(template.get('id')) == template_id:
                    self.template_id = template_id
                    self.template_expression = template.get('expression')
                    self.template_parameters = template.get('parameters')
                    self.template_settings = template.get('settings', {})
                    self.settings = {
                        **self.default_settings,
                        **(self.template_settings if self.template_settings else {})
                    }
                    self._get_parameters()
        if not self.template_id:
            logger.warning(f'template_id {template_id} not found')


    def _get_default_settings(self):
        with open('config/default_settings.json') as f:
            self.default_settings = json.load(f)


    def _get_parameters(self):
        for key in self.template_parameters:
            value = self.template_parameters.get(key)
            if type(value) == list:
                self.parameters[key] = value
            elif type(value) == dict:
                param_type = value.get('type')
                value_list = []
                if param_type == 'data_field':
                    data_fields = get_data_fields_by_criteria(self.db, **value.get('where'), 
                                                              order_by=value.get('order_by'), limit=value.get('limit'))
                    value_list = [ data_field.field_name for data_field in data_fields ]
                else:
                    pass
                    # TODO: other params
                self.parameters[key] = value_list


    def load_simulation_queue(self, template_id = None, append = False):
        if template_id:
            self._read_template(template_id)
        if not self.template_id:
            logger.warning(f'template not loaded')
            return
        if not append:
            delete_queue_by_template_id(self.db, self.template_id)
            logger.debug(f'deleted template_id {template_id} in the queue')
        keys = self.parameters.keys()
        values = self.parameters.values()
        params_list = [dict(zip(keys, combo)) for combo in product(*values)]
        for params in params_list:
            expression = self.template_expression.format(**params).strip()
            insert_queue(self.db, expression, json.dumps(self.settings), template_id = self.template_id)
        logger.info(f'inserted {len(params_list)} alphas')
