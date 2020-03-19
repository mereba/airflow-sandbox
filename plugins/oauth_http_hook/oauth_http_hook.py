from typing import Any, Dict, Optional

import oauthlib.oauth2 as oauth2
import requests
import requests_oauthlib
from airflow.hooks.http_hook import HttpHook

OptionalDictAny = Optional[Dict[str, Any]]


class OAuthHttpHook(HttpHook):
    """
    Add OAuth support to the basic HttpHook
    """

    def __init__(self, token_url='', *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.token_url = token_url

    # headers may be passed through directly or in the "extra" field in the connection
    # definition

    def get_conn(self, headers: OptionalDictAny = None) -> requests_oauthlib.OAuth2Session:
        conn = self.get_connection(self.http_conn_id)

        # login and password are required
        assert conn.login and conn.password

        client = oauth2.BackendApplicationClient(client_id=conn.login)
        session = requests_oauthlib.OAuth2Session(client=client)

        # inject token to the session
        assert self.token_url
        session.fetch_token(
            token_url=self.token_url,
            client_id=conn.login,
            client_secret=conn.password,
            include_client_id=True
        )

        if conn.host and "://" in conn.host:
            self.base_url = conn.host
        else:
            # schema defaults to HTTPS
            schema = conn.schema if conn.schema else "https"
            host = conn.host if conn.host else ""
            self.base_url = schema + "://" + host

        if conn.port:
            self.base_url = self.base_url + ":" + str(conn.port)

        if conn.extra:
            try:
                session.headers.update(conn.extra_dejson)
            except TypeError:
                self.log.warn('Connection to %s has invalid extra field.', conn.host)
        if headers:
            session.headers.update(headers)

        return session

    def run(self,
            endpoint: str,
            data: OptionalDictAny = None,
            headers: OptionalDictAny = None,
            extra_options: OptionalDictAny = None) -> requests.Response:

        session = self.get_conn(headers)

        if self.base_url and not self.base_url.endswith('/') and \
           endpoint and not endpoint.startswith('/'):
            url = self.base_url + '/' + endpoint
        else:
            url = (self.base_url or '') + (endpoint or '')

        req_settings = dict(
            stream=extra_options.get("stream", False),
            verify=extra_options.get("verify", False),
            proxies=extra_options.get("proxies", {}),
            cert=extra_options.get("cert"),
            timeout=extra_options.get("timeout"),
            allow_redirects=extra_options.get("allow_redirects", True)
        )

        try:
            self.log.info("Sending '%s' to url: %s", self.method, url)
            if self.method == 'GET':
                # GET uses params
                response = session.request(self.method,
                                           url,
                                           params=data,
                                           headers=headers,
                                           **req_settings)
            elif self.method == 'HEAD':
                # HEAD doesn't use params
                response = session.request(self.method,
                                           url,
                                           headers=headers,
                                           **req_settings)
            else:
                # Others use data
                response = session.request(self.method,
                                           url,
                                           data=data,
                                           headers=headers,
                                           **req_settings)

        except requests.exceptions.ConnectionError as ex:
            self.log.warning(str(ex) + ' Tenacity will retry to execute the operation')
            raise ex

        if extra_options.get('check_response', True):
            self.check_response(response)

        return response

    def run_and_check(self, *args, **kwargs) -> None:
        """Use the `run` method instead
        """
        raise NotImplementedError
