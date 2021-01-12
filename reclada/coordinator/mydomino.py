import requests


class DominoException(Exception):
    pass


class DominoBase:
    def __init__(self, token, *, session=None, base_url="https://try.dominodatalab.com/v1/"):
        self.token = token
        self.session = session or requests.Session()
        self.base_url = base_url

    def _request(self, path, method, params=None, json=None, stream=False, data=None):
        if not path.startswith("https://"):
            path = self.base_url + path
        try:
            headers = {
                "X-Domino-Api-Key": self.token,
            }
            resp = self.session.request(
                method,
                path,
                params=params, json=json, data=data,
                headers=headers, stream=stream,
            )
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            raise DominoException from e


class Domino(DominoBase):
    def files(self, user, project, commit, path=""):
        return self._request(f"projects/{user}/{project}/files/{commit}/{path}", "GET").json()

    def run(self, user, project, command, *, title="from api", commit="", is_direct=False):
        data = {
            "isDirect": is_direct,
            "command": command,
            "title": title,
        }
        if commit:
            data["commitId"] = commit
        return self._request(f"projects/{user}/{project}/runs", "POST", json=data).json()

    def run_status(self, user, project, run_id):
        return self._request(f"projects/{user}/{project}/runs/{run_id}/", "GET").json()

    def file(self, url):
        return self._request(url, method="GET", stream=True).raw

    def upload(self, user, project, path, data):
        return self._request(f"projects/{user}/{project}/{path}", "PUT", data=data).json()
