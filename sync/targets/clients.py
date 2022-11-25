import requests
import settings

from os.path import exists
from requests.exceptions import HTTPError
from typing import List, Dict, Union

from tracers import response_tracer


class BackendAPIClient:

    def __init__(self):

        self._FILE_ACCESS_TOKEN = '.backend.access_token.tmp'
        self._ACCESS_TOKEN = self._get_access_token()

    def _get_access_token(self) -> str:
        """
        Reads the user's access token from local file and validates it.
        If absent or invalid, it generates a new token and writes it to the temp file.

        Returns the user's access token.
        """

        token = self._read_access_token()
        if token and self._validate_access_token(token):
            return token

        return self._login()

    def _login(self) -> str:
        """
        Login to the Backend app using predefine service account
        and writes the access token to the temp file.
        """

        method = 'POST'
        path = '/auth/login/'

        payload = {
            'username': settings.HOLISTIC_SERVICE_ACCOUNT,
            'password': settings.HOLISTIC_SERVICE_ACCOUNT_PASSWORD
        }

        response = self._auth_request(method, path, payload=payload)

        token = response.json().get('token')
        self._write_access_token(token)

        return token

    def _validate_access_token(self, token) -> bool:
        """
        Returns True if that `token` is valid.
        If we can retrieve the user profile belonging to that access `token`,
        we assume that `token` is valid.

        Normally, the Backend access token is valid for 1 day.
        """

        method = 'GET'
        path = '/accounts/me/'

        try:
            self._api_request(method, path, headers={
                "Authorization": f"Token {token}"
            })
        except HTTPError:
            return False

        return True

    def _write_access_token(self, token) -> None:
        """
        Write that Backend access `token` into disk.
        """

        with open(self._FILE_ACCESS_TOKEN, "w") as file_access_token:
            file_access_token.write(token)

    def _read_access_token(self) -> Union[str, None]:
        """
        Read access token from the temporary file.

        Returns the access token or None.
        """

        if not exists(self._FILE_ACCESS_TOKEN):
            return None

        with open(self._FILE_ACCESS_TOKEN, "r") as file_access_token:
            try:
                return file_access_token.read()
            except Exception:
                return None

    def _auth_request(self, method, path, payload={}, headers=None) -> requests.Response:

        url = settings.HOLISTIC_BACKEND_URL + path

        req_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        if headers:
            req_headers.update(headers)

        hooks = {'response': response_tracer} if settings.DEBUG_MODE else None
        auth = (payload.get('username'), payload.get('password'))

        response = requests.request(method, url, auth=auth, headers=req_headers, hooks=hooks)
        response.raise_for_status()

        return response

    def _api_request(self, method, path, payload=None, headers=None) -> requests.Response:

        url = settings.HOLISTIC_BACKEND_URL + path
        req_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Token {self._ACCESS_TOKEN}" if hasattr(self, '_ACCESS_TOKEN') else None
        }

        if headers:
            req_headers.update(headers)

        hooks = {'response': response_tracer} if settings.DEBUG_MODE else None

        response = requests.request(method, url, json=payload, headers=req_headers, hooks=hooks)
        response.raise_for_status()

        return response


class OrganizationAPI(BackendAPIClient):

    def upsert(self, organizations: List[Dict]) -> Dict:
        """
        Create or update organizations in the Backend.
        """

        method = 'POST'
        path = '/sync/organizations/'

        response = self._api_request(method, path, organizations)

        return response.json()


class TherapistOrganizationAPI(BackendAPIClient):

    def upsert(self, org_id: int, therapists: List[Dict]) -> Dict:
        """
        Create or update therapists belonging to the specific organization in the Backend.
        """

        method = 'POST'
        path = f'/sync/organizations/{org_id}/therapists/'

        response = self._api_request(method, path, therapists)

        return response.json()


class TherapistInteractionAPI(BackendAPIClient):

    def upsert(self, ther_id: str, interactions: List[Dict]) -> Dict:
        """
        Create or update interactions of that specific therapist ID in the Backend.
        """

        method = 'POST'
        path = f'/sync/organizations/therapists/{ther_id}/interactions/'

        response = self._api_request(method, path, interactions)

        return response.json()
