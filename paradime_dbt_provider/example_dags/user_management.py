# Third party modules
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator

# First party modules
from paradime_dbt_provider.hooks.paradime import ParadimeHook, UserAccountType


def manage_users(self, conn_id: str):
    hook = ParadimeHook(conn_id=conn_id)

    # Get the active users
    active_users = hook.get_active_users()

    # Print the active users' names
    self.log.info(f"Active users: {','.join([user.name for user in active_users])}")

    # Get the invited users
    invited_users = hook.get_invited_users()

    # Print the invited users' emails
    self.log.info(f"Invited users: {','.join([user.email for user in invited_users])}")

    # Get the workspaces
    workspaces = hook.get_workspaces()

    # Print the workspaces' names
    self.log.info(f"Workspaces: {','.join([workspace.name for workspace in workspaces])}")

    # Invite a user
    hook.invite_user(email="foo@bar.baz", account_type=UserAccountType.ADMIN)

    # Update a user's account type
    hook.update_user_account_type(uid="--user-uid--", account_type=UserAccountType.BUSINESS)

    # Disable a user
    hook.disable_user(uid="--user-uid--")


@dag(
    start_date=datetime(2024, 1, 1),
    default_args={"conn_id": "paradime_conn_id"},  # Update this to your connection id
)
def user_management():
    task_user_management = PythonOperator(task_id="user_management", python_callable=manage_users)

    task_user_management


user_management()
