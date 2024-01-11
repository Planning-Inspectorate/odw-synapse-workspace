import csv
from collections import defaultdict
import pprint
import model_nsip_exam_timetable
from pydantic import BaseModel, ValidationError

# create a list of fields that sit in the events nested array. Taken directly from pydantic model Event class
_event_fields = dict(model_nsip_exam_timetable.Event.__annotations__.items())
# convert to a dictionary
_event_dict = {k.lower(): v for k, v in _event_fields.items()}
# list the keys that need to be excluded form the top level,
# i.e. if a field is in a nested array it shouldn't be in the top level as well
_excluded_fields = [k.lower() for k, v in _event_dict.items()]

# sample file for testing
_file_name = r"C:\Users\ChrisTopping\Downloads\nsip-exam-timetable-sample.csv"

_data = []

with open(_file_name, encoding="utf-8-sig") as csvfile:
    _reader = csv.DictReader(csvfile)
    _data.extend(_reader)

_merged_data = defaultdict(dict)

for row in _data:
    _lowercase_row = {k.lower(): v for k, v in row.items()}
    _caseReference = _lowercase_row["casereference"]
    if _caseReference not in _merged_data:
        # set casereference as the primary key and add all other fields to top level
        _merged_data[_caseReference] = {
            k: v for k, v in _lowercase_row.items() if k.lower() not in _excluded_fields
        }
        # create empty nested array for events
        _merged_data[_caseReference]["events"] = []
    # populate events array
    _events = _merged_data[_caseReference]["events"]
    _new_event = {k.lower(): _lowercase_row[k] for k, v in _event_dict.items()}
    if _new_event not in _events:
        _events.append(_new_event)

_final_data = list(_merged_data.values())


# validation of data against model

class ExaminationTimetableInstances(BaseModel):
    data: list[model_nsip_exam_timetable.ExaminationTimetable]

try:
    ExaminationTimetableInstances(data=_final_data)
except ValidationError as e:
    print(e)
    pprint.pprint(e.errors())
