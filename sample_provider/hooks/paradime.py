from __future__ import annotations
from dataclasses import dataclass

from typing import Any

from airflow.hooks.base import BaseHook
import requests


class ParadimeHook(BaseHook):
    conn_name_attr = "conn_id"
    default_conn_name = "paradime_conn_default" 
    conn_type = "paradime"
    hook_name = "Paradime"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "api_endpoint": StringField(lazy_gettext("API Endpoint"), widget=BS3TextFieldWidget()),
            "api_key": StringField(lazy_gettext("API Key"), widget=BS3TextFieldWidget()),
            "api_secret": PasswordField(lazy_gettext("API Secret"), widget=BS3PasswordFieldWidget()),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict:

        return {
            "hidden_fields": ["port", "password", "login", "schema", "extra", "host"],
            "relabeling": {},
            "placeholders": {
                "api_endpoint": "https://api.paradime.io/api/v1/....",
                "api_key": "You can generate the API key from the Paradime workspace settings...",
                "api_secret": "You can generate the API secret from the Paradime workspace settings...",
            },
        }

    def __init__(
        self,
        conn_id: str,
    ) -> None:
        super().__init__()
        self.conn_id = conn_id

    @dataclass
    class AuthConfig:
        api_endpoint: str
        api_key: str
        api_secret: str

    def get_auth_config(self) -> AuthConfig:
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson
        return self.AuthConfig(
            api_endpoint=extra["api_endpoint"],
            api_key=extra["api_key"],
            api_secret=extra["api_secret"],
        )
    
    def get_api_endpoint(self) -> str:
        return self.get_auth_config().api_endpoint
    
    def get_request_headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-API-KEY": self.get_auth_config().api_key,
            "X-API-SECRET": self.get_auth_config().api_secret,
        }

    def _extract_gql_response(
        request: requests.Response, query_name: str, field: str,
    ) -> str:
        response_json = request.json()
        if "errors" in response_json:
            raise Exception(f"{response_json['errors']}")

        try:
            return response_json["data"][query_name][field]
        except (TypeError, KeyError) as e:
            raise ValueError(f"{e}: {response_json}")

    def trigger_schedule_run(self, schedule_name: str) -> int:
        query = """
            mutation trigger($scheduleName: String!) {
                triggerBoltRun(scheduleName: $scheduleName){
                    runId
                }
            }
        """

        response = requests.post(
            url=self.get_api_endpoint(),
            json={"query": query, "variables": {"scheduleName": schedule_name}},
            headers=self.get_request_headers(),
        )
        response.raise_for_status()

        run_id = response.json()["data"]["triggerBoltRun"]["runId"]

        return run_id