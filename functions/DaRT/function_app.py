import azure.functions as func
import logging
from azure.functions.decorators.core import DataType
import json

app = func.FunctionApp()

@app.function_name(name="GetCases")
@app.route(route="getcases", methods=["get"], auth_level=func.AuthLevel.FUNCTION)

@app.generic_input_binding(arg_name="cases", type="sql",
                        CommandText="SELECT TOP (10) * FROM [odw_harmonised_db].[dbo].[casework_case_info_dim]",
                        CommandType="Text",
                        ConnectionStringSetting="SqlConnectionString",
                        data_type=DataType.STRING)

def get_cases(req: func.HttpRequest, cases: func.SqlRowList) -> func.HttpResponse:
    rows = list(map(lambda r: json.loads(r.to_json()), cases))

    return func.HttpResponse(
        json.dumps(rows),
        status_code=200,
        mimetype="application/json"
    )