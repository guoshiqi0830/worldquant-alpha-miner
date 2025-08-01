from urllib.parse import urlencode, urlparse, urlunparse, quote, parse_qs
import json

def add_params_to_url(base_url: str, params: dict) -> str:
    parsed = urlparse(base_url)
    existing_params = {}
    if parsed.query:
        existing_params = {k: v[0] if len(v) == 1 else v 
                          for k, v in parse_qs(parsed.query).items()}
    merged_params = {**existing_params, **params}
    new_query = urlencode(merged_params, doseq=True, quote_via=quote)
    new_parts = parsed._replace(query=new_query)
    return urlunparse(new_parts)

def load_config(config_name):
    with open(f'config/{config_name}.json') as f:
        return json.load(f)
