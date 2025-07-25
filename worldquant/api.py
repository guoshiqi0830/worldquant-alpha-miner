import requests
import json
from requests.auth import HTTPBasicAuth
from loguru import logger
from util.util import add_params_to_url


class WorldQuantSession():
    def __init__(self):
        self.base_url = 'https://api.worldquantbrain.com'
        self.session = None
        self._sign_in()
        

    def _sign_in(self):
        sess = requests.Session()
        cred = json.load(open('worldquant/credential.json'))
        sess.auth = HTTPBasicAuth(cred['username'], cred['password'])
        response = sess.post(f'{self.base_url}/authentication')
        if response.status_code == 201:
            logger.info(f'login status code {response.status_code}, user {response.json().get('user').get('id')}')
            self.session = sess
            return sess
        else:
            logger.error(f'fail to login status code {response.status_code}, {response.text}')
            exit()
        

    def get_datafields(self, params, limit = 50, offset = 0):
        url = add_params_to_url(f'{self.base_url}/data-fields?limit={limit}&offset={offset}', params)
        return self.session.get(url)
    

    def get_simulation_status(self, simulate_id):
        url = f'{self.base_url}/simulations/{simulate_id}'
        return self.session.get(url)


    def submit_alpha(self, alpha_id):
        url = f"{self.base_url}/alphas/{alpha_id}/submit"
        return self.session.post( url, json={}, timeout=(10, 30))
    

    def check_alpha(self, alpha_id):
        url = f"{self.base_url}/alphas/{alpha_id}/check"
        return self.session.get(url)
    

    def get_alpha(self, alpha_id):
        url = f"{self.base_url}/alphas/{alpha_id}"
        return self.session.get(url)
    

    def search_alpha(self, params, limit = 100, offset = 0):
        url = add_params_to_url(f"{self.base_url}/users/self/alphas?limit={limit}&offset={offset}", params)
        return self.session.get(url)
    

    def post_simulation(self, simulation):
        url = f'{self.base_url}/simulations'
        return self.session.post(url, json=simulation)
