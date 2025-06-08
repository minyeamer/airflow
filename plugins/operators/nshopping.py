from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from typing import Dict, List

class NaverSearchToCsvOperator(BaseOperator):
    template_fields = ("file_path", "client_id", "client_secret")

    def __init__(self,
            search_type: str,
            file_path: str,
            client_id: str,
            client_secret: str,
            keyword: str,
            display: int = 10,
            start: int = 1,
            sort = "sim",
            **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = "openapi.naver.com"
        self.endpoint = f"/v1/search/{search_type}.json"
        self.method = "GET"
        self.file_path = file_path if file_path.startswith("/") else ("/opt/airflow/files/"+file_path)
        self.client_id = client_id
        self.client_secret = client_secret
        self.keyword = keyword
        self.display = min(display, 1000)
        self.start = min(start, 1000)
        self.sort = sort

    def execute(self, context):
        connection = BaseHook.get_connection(self.http_conn_id)
        url = connection.host + self.endpoint
        rows = list()
        for i, start in enumerate(range(self.start, self.display, 100)):
            display = min(self.display + self.start - start, 100)
            self.log.info(f"시작: {start}")
            self.log.info(f"끝: {start+display-1}")
            kwargs = dict(keyword=self.keyword, display=display, start=start, sort=self.sort)
            rows = rows + self._request_api(url, show_header=(i == 0), **kwargs)
        self._mkdir(self.file_path)
        self._to_csv(rows, self.file_path, encoding="utf-8", sep=',')

    def _request_api(self, url: str, keyword: str, display: int=10, start: int=1, sort="sim", show_header=True) -> List[List]:
        import requests
        params = self._get_params(keyword, display, start, sort)
        headers = self._get_headers(self.client_id, self.client_secret)
        with requests.request(self.method, url, params=params, headers=headers) as response:
            return self._parse_api(response, start, show_header)

    def _get_params(self, keyword: str, display: int=10, start: int=1, sort="sim") -> Dict:
        return {"query": keyword, "display": min(display, 100), "start": min(start, 1000), "sort": sort}

    def _get_headers(self, client_id: str, client_secret: str) -> Dict[str,str]:
        return {
            "Content-Type": "application/json",
            "X-Naver-Client-Id": client_id,
            "X-Naver-Client-Secret": client_secret
        }

    def _parse_api(self, response, start: int=1, show_header=True) -> List[List]:
        contents = response.json()["items"]
        if contents:
            header = ([["rank"] + list(contents[0].keys())]) if show_header else []
            return header + [[start+i]+list(content.values()) for i, content in enumerate(contents)]
        else: return list()

    def _mkdir(self, file_path: str):
        import os
        dir_path = os.path.split(file_path)[0]
        if not os.path.exists(dir_path):
            os.system(f"mkdir -p {dir_path}")

    def _to_csv(self, rows: List[List], file_path: str, encoding="utf-8", sep=','):
        def clean(value: str) -> str:
            return str(value).replace(sep, '')
        with open(file_path, 'w', encoding=encoding) as file:
            for row in rows:
                file.write(sep.join(map(clean, row))+'\n')