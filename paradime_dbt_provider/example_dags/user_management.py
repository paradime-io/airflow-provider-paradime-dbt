# Third party modules
import logging

from airflow.decorators import dag  # type: ignore[import]
from airflow.operators.python import PythonOperator  # type: ignore[import]

# First party modules
from paradime_dbt_provider.hooks.paradime import ParadimeHook, UserAccountType

logger = logging.getLogger(__name__)

PARADIME_CONN_ID = "your_paradime_conn_id"  # Update this to your connection id


def manage_users(conn_id: str):
    paradime_hook = ParadimeHook(conn_id=conn_id)

    # Get the active users
    active_users = paradime_hook.get_active_users()

    # Print the active users' names
    logger.info(f"Active users: {', '.join([user.name for user in active_users])}")

    # Get the invited users
    invited_users = paradime_hook.get_invited_users()

    # Print the invited users' emails
    logger.info(f"Invited users: {', '.join([user.email for user in invited_users])}")

    # Get the workspaces
    workspaces = paradime_hook.get_workspaces()

    # Print the workspaces' names
    logger.info(f"Workspaces: {', '.join([workspace.name for workspace in workspaces])}")

    # Invite a user
    paradime_hook.invite_user(email="foo@bar.baz", account_type=UserAccountType.ADMIN)

    # Update a user's account type
    paradime_hook.update_user_account_type(uid="--user-uid--", account_type=UserAccountType.BUSINESS)

    # Disable a user
    paradime_hook.disable_user(uid="--user-uid--")


@dag()
def user_management():
    task_user_management = PythonOperator(task_id="user_management", python_callable=manage_users, op_kwargs={"conn_id": PARADIME_CONN_ID})

    task_user_management


user_management()
