import requests
import settings

from os.path import exists
from requests.exceptions import HTTPError
from typing import Union

from helpers import decrypts_text
from tracers import endpoint_tracer, response_tracer


class MetabaseAPIClient:

    def __init__(self):
        # When running `sync` service in a development mode, it won't require Metabase access token
        # because we will use downloaded Metabase data from `.csv` files.
        if settings.DEV_MODE:
            return

        self._METABASE_URL_API = settings.METABASE_URL + '/api'

        self._FILE_SESSION_ID = '.metabase.session.tmp'
        self._SESSION_ID = self._get_session_id()

    def _get_session_id(self) -> str:
        """
        Reads the user's session ID from local file and validates it.
        If absent or invalid, it generates a new token and writes it to the temp file.

        Returns the session ID.
        """

        session_id = self._read_session_id()
        if session_id and self._validate_session_id(session_id):
            return session_id

        return self._login()

    def _login(self) -> str:
        """
        Login to the Metabase using predefine service account
        and writes the session ID to the temp file.
        """

        method = 'POST'
        path = '/session'

        payload = {
            'username': decrypts_text(settings.METABASE_SERVICE_ACCOUNT),
            'password': decrypts_text(settings.METABASE_SERVICE_ACCOUNT_PASSWORD)
        }

        response = self._auth_request(method, path, payload=payload)

        session_id = response.json().get('id')
        self._write_session_id(session_id)

        return session_id

    def _validate_session_id(self, session_id: str) -> bool:
        """
        Returns True if that `session_id` is valid.
        If we can retrieve the user profile belonging to that `session_id`,
        we assume that session is valid.

        Normally, the Metabase session ID is valid for up to 14 days.
        @see https://www.metabase.com/learn/administration/metabase-api
        """

        method = 'GET'
        path = '/user/current'

        try:
            self._api_request(method, path, headers={
                "X-Metabase-Session": session_id
            })
        except HTTPError:
            return False

        return True

    def _write_session_id(self, session_id: str) -> None:
        """
        Write that `session_id` into disk.
        """

        with open(self._FILE_SESSION_ID, "w") as file_session_id:
            file_session_id.write(session_id)

    def _read_session_id(self) -> Union[str, None]:
        """
        Read session ID from the temporary file.

        Returns the session ID or None.
        """

        if not exists(self._FILE_SESSION_ID):
            return None

        with open(self._FILE_SESSION_ID, "r") as file_session_id:
            try:
                return file_session_id.read()
            except Exception:
                return None

    def _auth_request(self, method, path, payload=None, headers=None) -> requests.Response:

        url = self._METABASE_URL_API + path
        req_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        if headers:
            req_headers.update(headers)

        hooks = {'response': response_tracer} if settings.DEBUG_MODE else None

        response = requests.request(method, url, json=payload, headers=req_headers, hooks=hooks)
        response.raise_for_status()

        return response

    def _api_request(self, method, path, payload=None, headers=None) -> requests.Response:

        url = self._METABASE_URL_API + path
        req_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Metabase-Session": self._SESSION_ID if hasattr(self, '_SESSION_ID') else None
        }

        if headers:
            req_headers.update(headers)

        hooks = {'response': response_tracer} if settings.DEBUG_MODE else None

        response = requests.request(method, url, json=payload, headers=req_headers, hooks=hooks)
        response.raise_for_status()

        return response

    def _download(self, download_as, path, payload=None, headers=None) -> None:

        url = self._METABASE_URL_API + path
        req_headers = {
            "Accept": "*/*",
            "X-Metabase-Session": self._SESSION_ID
        }

        if headers:
            req_headers.update(headers)

        chunk_size = 4096
        hooks = {'response': endpoint_tracer} if settings.DEBUG_MODE else None

        with requests.post(url, data=payload, headers=req_headers, stream=True, hooks=hooks) as req:
            with open(download_as, 'wb') as file:
                # Writes response data in chunk
                for chunk in req.iter_content(chunk_size):
                    if not chunk:
                        continue

                    file.write(chunk)


class TherapistAPI(MetabaseAPIClient):

    def __init__(self) -> None:
        super().__init__()

        self._THERS_JOINING_ND_CARD_ID = 2060
        self._THERS_JOINING_ND_FILE = '.thers_joining_nd.csv.tmp'

    def download_data(self, format='csv') -> None:
        """
        Downloads Therapist Joining Niceday data from Metabase in CSV format.
        """
        if format not in ['json', 'csv', 'xlsx', 'api']:
            raise ValueError(f'{format} is invalid format.')

        path = f'/card/{self._THERS_JOINING_ND_CARD_ID}/query/{format}'

        self._download(self._THERS_JOINING_ND_FILE, path)


class InteractionAPI(MetabaseAPIClient):

    def __init__(self) -> None:
        super().__init__()

        self._THER_INTERACTIONS_CARD_ID = 2061
        self._THER_INTERACTIONS_FILE = '.ther_interactions.csv.tmp'

    def download_data(self, format='csv') -> None:
        """
        Downloads Therapist Interaction data from Metabase in CSV format.
        """
        if format not in ['json', 'csv', 'xlsx', 'api']:
            raise ValueError(f'{format} is invalid format.')

        path = f'/card/{self._THER_INTERACTIONS_CARD_ID}/query/{format}'

        self._download(self._THER_INTERACTIONS_FILE, path)
