import json
import boto3
import logging

# Function to store data in s3
class FiveTranConnector:
    '''
    class to conform to fivetran connector. 

    Will sync using an s3_bucket
    '''
    agent = ''
    state = {}
    secrets = {}

    ###
    fivetran_state = {}
    fivetran_insert = {}
    fivetran_delete = {}
    fivetran_schema = {}
    
    def __init__(self, s3_bucket: str, s3_file: str):
        '''
        Specify s3_bucket and s3_file when initialising object
        '''
        self.bucket = s3_bucket
        self.file = s3_file

    def add_insert_data(self, table_name: str, data: list):
        '''
        add a list of data to insert into self.insert_data.

        data will be inserted into table_name as a list of rows.
        '''
        self.fivetran_insert[table_name] = data

    def add_delete_data(self, table_name: str, data: list):
        '''
        add a list of data to insert into self.insert_data.

        data will be inserted into table_name as a list of rows.
        '''
        self.fivetran_delete[table_name] = data

    def update_fivetran_state(self, updates: dict):
        '''
        update the new state for fivetran's next call.
        '''
        self.fivetran_state.update(updates)

    def _push_data_s3(self, fivetran_payload):
        '''
        Use s3 as connector to host data temporarily.
        '''

        json_str = bytes(json.dumps(fivetran_payload).encode("utf-8"))
        client = boto3.client('s3')
        client.put_object(Body=json_str, Bucket=self.bucket, Key=self.file)

    def main(self):
        '''
        fivetran connector, return the fivetran payload.
        Ensure that self.add_insert_data and self.update_fivetran_state is executed already.
        '''
        if not list(self.fivetran_insert.keys()) and not list(self.fivetran_delete.keys()):
            print('No data received. Do nothing.')
            return None

        #### FIVETRAN S3 SYNC
        fivetran_s3_payload = {
            "insert" : self.fivetran_insert,
            "delete" : self.fivetran_delete
        }

        self._push_data_s3(fivetran_s3_payload)

        fivetran_lambda_payload = {
            "state": self.fivetran_state,
            "schema": self.fivetran_schema
        }

        logging.info(f'Next update state: {self.fivetran_state}')

        return fivetran_lambda_payload
