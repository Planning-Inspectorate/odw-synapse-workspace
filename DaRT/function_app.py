import azure.functions as func
import logging
from azure.functions.decorators.core import DataType
import json

_SQL_QUERY = "dart_sql.sql"

app = func.FunctionApp()

@app.function_name(name="getCase")
@app.route(route="case/{AppealRefNumber}", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
@app.generic_input_binding(arg_name="case", type="sql",
                        CommandType="Text",
                        Parameters="@AppealRefNumber={AppealRefNumber}",
                        ConnectionStringSetting="SqlConnectionString",
                        data_type=DataType.STRING)
def get_case(req: func.HttpRequest, case: func.SqlRowList) -> func.HttpResponse:
    with open(_SQL_QUERY, 'r') as dart_query:
        dart_sql = dart_query.read()
        case.set('CommandText', dart_sql)
    rows = list(map(lambda r: json.loads(r.to_json()), case))
    return func.HttpResponse(
        json.dumps(rows),
        status_code=200,
        mimetype="application/json"
    )
