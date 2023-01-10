import json
import logging

class Config:
    def __init__(self , section) -> None:
        logging.info('config init..')
        with open('datacompare/config.json', 'r') as f:
            data = json.loads(f.read())
            self.cfg = data[section]

    def get_from_iiq(self, key):
        try:
            iiq = self.cfg['iiq']
            for item in iiq:
                if key == item:
                    return iiq[key]
        except Exception:
            raise Exception("No parameter [%s] found in config file" % key)

    def get_from_bq(self, key):
        try:
            bq = self.cfg['bq']
            for item in bq:
                if key == item:
                    return bq[key]
        except Exception:
            raise Exception("No parameter [%s] found in config file" % key)

    def get_from_compare(self, key):
        try:
            c = self.cfg['compare']
            for item in c:
                if key == item:
                    return c[key]
        except Exception:
            raise Exception("No parameter [%s] found in config file" % key)
