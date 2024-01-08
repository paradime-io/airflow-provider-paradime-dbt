from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.sensors.base import BaseSensorOperator  # type: ignore

from paradime_dbt_provider.hooks.paradime import BoltRunState, ParadimeException, ParadimeHook

if TYPE_CHECKING:
    from airflow.utils.context import Context  # type: ignore


class ParadimeBoltDbtScheduleRunSensor(BaseSensorOperator):
    """
    Waits for a Paradime Bolt dbt schedule run to complete.

    :param conn_id: The Airflow connection id to use when connecting to Paradime.
    :param run_id: The id of the schedule run to wait for.
    """

    template_fields = ["run_id"]

    def __init__(
        self,
        *,
        conn_id: str,
        run_id: int,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.hook = ParadimeHook(conn_id=conn_id)
        self.run_id = run_id

    def poke(self, context: Context) -> bool:
        state = self.hook.get_bolt_run_status(self.run_id)

        more_info = f"More details at: https://app.paradime.io/bolt/run_id/{self.run_id}"

        if state == BoltRunState.RUNNING.value:
            self.log.info(f"Run {self.run_id!r} is still running. {more_info}")
            return False
        elif state == BoltRunState.SUCCESS.value:
            self.log.info(f"Run {self.run_id!r} has succeeded. {more_info}")
            return True
        elif state == BoltRunState.FAILED.value:
            raise ParadimeException(f"Run {self.run_id!r} has failed. {more_info}")
        elif state == BoltRunState.ERROR.value:
            raise ParadimeException(f"Run {self.run_id!r} has error(s). {more_info}")
        elif state == BoltRunState.CANCELED.value:
            raise ParadimeException(f"Run {self.run_id!r} was canceled. {more_info}")
        elif state == BoltRunState.SKIPPED.value:
            raise ParadimeException(f"Run {self.run_id!r} was skipped. {more_info}")
        else:
            raise ParadimeException(f"Run {self.run_id!r} has an unknown status: {state!r}. {more_info}")
