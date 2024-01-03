from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.models import BaseOperator

from sample_provider.hooks.paradime import ParadimeHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

class ParadimeBoltDbtScheduleRunOperator(BaseOperator):
    """
    Triggers a Paradime Bolt dbt schedule run.

    :param conn_id: The Airflow connection id to use when connecting to Paradime.
    :param schedule_name: The name of the bolt schedule to run.
    """

    def __init__(
        self,
        *,
        conn_id: str,
        schedule_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.schedule_name = schedule_name
        self.hook = ParadimeHook(conn_id=conn_id)

    def execute(self, context: Context) -> int:
        run_id = self.hook.trigger_schedule_run(schedule_name=self.schedule_name)
        return run_id
