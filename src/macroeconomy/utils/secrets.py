import base64

from databricks.sdk import WorkspaceClient


class SecretManager:

    @staticmethod
    def get_secret(scope, secret_name):
        w = WorkspaceClient()
        secret = w.secrets.get_secret(scope=scope, key=secret_name)
        return base64.b64decode(secret.value).decode("utf-8")
