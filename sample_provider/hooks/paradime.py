from __future__ import annotations
from dataclasses import dataclass

from typing import Any

from airflow.hooks.base import BaseHook


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

