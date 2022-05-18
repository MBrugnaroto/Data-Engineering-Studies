import json
from os.path import join
from pathlib import Path
from datetime import datetime
from airflow.models import DAG, TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.twitter_hook import TwitterHook

class TwitterOperator(BaseOperator):
    
    template_fields = [
        "query",
        "file_path",
        "start_time",
        "end_time"
    ]
    
    @apply_defaults
    def __init__(
        self,
        query,
        file_path, 
        conn_id = None, 
        start_time = None, 
        end_time = None,
        *args,
        **kwargs
    ):
        self.query = query
        self.file_path = file_path, 
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time
        super().__init__(*args, **kwargs)
    
    def create_parent_file(self):
        parent = Path(self.file_path[0]).parent
        Path(parent).mkdir(parents=True, exist_ok=True)
    
    def execute(self, context):
        hook = TwitterHook(
            self.query,
            self.conn_id,
            self.start_time,
            self.end_time
        )
        
        self.create_parent_file()
        with open(self.file_path[0], "w") as file:
            for pg in hook.run():
                json.dump(pg, file, ensure_ascii=False)
                file.write("\n")
