import requests

import settings

from os.path import exists
from requests.exceptions import HTTPError
from typing import Dict, Union

from tracers import request_tracer, response_tracer


class MetabaseAPIClient:

    def __init__(self):

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
            'username': settings.METABASE_SERVICE_ACCOUNT,
            'password': settings.METABASE_SERVICE_ACCOUNT_PASSWORD
        }

        response = self._auth_request(method, path, payload=payload)

        session_id = response.json().get('id')
        self._write_session_id(session_id)

        return session_id

    def _validate_session_id(self, session_id) -> bool:
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

    def _write_session_id(self, session_id) -> None:
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
        default_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        if headers:
            default_headers.update(headers)

        response = requests.request(
            method,
            url,
            json=payload,
            headers=headers,
            hooks=self._get_hook_requests()
        )

        response.raise_for_status()

        return response

    def _api_request(self, method, path, payload=None, headers=None) -> requests.Response:

        url = self._METABASE_URL_API + path
        default_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Metabase-Session": self._SESSION_ID if hasattr(self, '_SESSION_ID') else None
        }

        if headers:
            default_headers.update(headers)

        response = requests.request(
            method,
            url,
            json=payload,
            headers=default_headers,
            hooks=self._get_hook_requests()
        )

        response.raise_for_status()

        return response

    def _download(self, download_as, path, payload=None, headers=None) -> None:

        url = self._METABASE_URL_API + path
        default_headers = {
            "Accept": "*/*",
            "X-Metabase-Session": self._SESSION_ID
        }

        if headers:
            default_headers.update(headers)

        chunk_size = 4096

        with requests.post(
            url,
            data=payload,
            headers=default_headers,
            stream=True,
            hooks=self._get_hook_requests()
        ) as request:

            with open(download_as, 'wb') as file:

                for chunk in request.iter_content(chunk_size):
                    if not chunk:
                        continue

                    file.write(chunk)

    def _get_hook_requests(self) -> Union[Dict, None]:
        """
        Returns dictionary that contains predefined hook functions
        for printing out HTTP request and its response from the requests module.
        """

        if settings.DEBUG_MODE:
            return {'response': [request_tracer, response_tracer]}

        return None


class TherapistJoiningNicedayAPI(MetabaseAPIClient):

    def __init__(self) -> None:
        super().__init__()

        self._THER_JOINING_ND_CARD_ID = 2060
        self._THER_JOINING_ND_FILE = '.thers_joining_nd.csv.tmp'

    def download(self) -> None:
        """
        Download all Therapist Interaction data from Metabase in CSV format.
        """

        # Available download options ['json', 'csv', 'xlsx', 'api']
        download_type = 'csv'

        path = f'/card/{self._THER_JOINING_ND_CARD_ID}/{download_type}'

        self._download(self._THER_JOINING_ND_FILE, path)


class TherapistInteractionAPI(MetabaseAPIClient):

    def __init__(self) -> None:
        super().__init__()

        self._THER_INTRACTION_CARD_ID = 2061
        self._THER_INTERACTION_FILE = '.ther_interactions.csv.tmp'

    def download(self) -> None:
        """
        Download all Therapist Interaction data from Metabase in CSV format.
        """

        # Available download options ['json', 'csv', 'xlsx', 'api']
        download_type = 'csv'

        path = f'/card/{self._THER_INTRACTION_CARD_ID}/{download_type}'

        self._download(self._THER_INTERACTION_FILE, path)
