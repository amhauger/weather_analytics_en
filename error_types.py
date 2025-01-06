class ConfigError(Exception):
    body = {}

    def mask_weatherapi_secret(self, data: dict) -> dict:
        if data.get("yaml_ingested") != None:
            if data.get("yaml_ingested").get("weatherapi-key") != None:
                data["yaml_ingested"]["weatherapi-key"] = "****"
        return data

    def mask_db_fields(self, data: dict) -> dict:
        if data.get("yaml_ingested") != None:
            if data.get("yaml_ingested").get("db-host") != None:
                data["yaml_ingested"]["db-host"][:-13] = "*"

            if data.get("yaml_ingested").get("db-name") != None:
                data["yaml_ingested"]["db-name"] = "****"

            if data.get("yaml_ingested").get("db-password") != None:
                data["yaml_ingested"]["db-password"] = "****"
        return data

    def mask_fields(self, data: dict) -> dict:
        data = self.mask_weatherapi_secret(data)
        data = self.mask_db_fields(data)

        return data

    def __init__(self, data: dict, message: str):
        self.body["data"] = self.mask_fields(data)
        self.body["message"] = message


class ConnectionError(Exception):
    body = {}

    def mask_db_fields(self, data: dict) -> dict:
        if data.get("db-name") != None:
            data["db-name"] = "****"

        if data.get("db-password") != None:
            data["db-password"] = "****"
        return data

    def mask_fields(self, data: dict) -> dict:
        data = self.mask_db_fields(data)

        return data

    def __init__(self, data: dict, message: str):
        self.body["data"] = self.mask_fields(data)
        self.body["message"] = message


class RedshiftDWError(Exception):
    body = {}

    def mask_password(self, data: dict) -> dict:
        if data.get("password") != None:
            data["password"] = "****"
        return data

    def __init__(self, data: dict, message: str):
        self.body["data"] = self.mask_password(data)
        self.body["message"] = message
