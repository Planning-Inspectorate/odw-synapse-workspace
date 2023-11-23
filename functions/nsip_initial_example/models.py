from pydantic import BaseModel, ValidationError, ConfigDict
import json
import pprint
from datetime import datetime
from model_funcs import fill_nonexistent_properties, remove_undefined_properties, convert_to_lower

def model():

  class nsip (BaseModel):

      model_config = ConfigDict(
          validate_assignment=True,
          extra='forbid'
      )

      projectname: str
      decision: str = None
      publishstatus: str = None
      examtimetablepublishstatus: str = None
      sector: str = None
      projecttype: str = None
      stage: str = None
      casereference: str = None
      caseid: int = None
      projectlocation: str = None
      projectemailaddress: str = None
      region: str = None
      easting: int = None
      northing: int = None
      transboundary: bool
      welshlanguage: bool = None
      mapzoomlevel: str = None
      projectdescription: str = None
      secretaryofstate: str = None
      dateprojectappearsonwebsite: datetime = None
      dateofdcoacceptance: datetime = None
      anticipateddateofsubmission: datetime = None
      anticipatedsubmissiondatenonspecific: str = None
      dateofdcosubmission: datetime = None
      dateofrepresentationperiodopen: datetime = None
      dateofrelevantrepresentationclose: datetime = None
      daterrepappearonwebsite: datetime = None
      confirmedstartofexamination: datetime = None
      datetimeexaminationends: datetime = None
      stage4extensiontoexamclosedate: datetime = None
      stage5extensiontorecommendationdeadline: datetime = None
      dateofrecommendations: datetime = None
      confirmeddateofdecision: datetime = None
      stage5extensiontodecisiondeadline: datetime = None
      dateprojectwithdrawn: datetime = None
      section46notification: datetime = None
      datepinsfirstnotifiedofproject: datetime = None
      screeningopinionsought: datetime = None
      screeningopinionissued: datetime = None
      scopingopinionsought: datetime = None
      scopingopinionissued: datetime = None
      deadlineforacceptancedecision: datetime = None
      datesection58noticereceived: datetime = None
      preliminarymeetingstartdate: datetime = None
      deadlineforcloseofexamination: datetime = None
      deadlineforsubmissionofrecommendation: datetime = None
      deadlinefordecision: datetime = None
      jrperiodenddate: datetime = None
      extensiontodaterelevantrepresentationsclose: datetime = None
      operationsleadid: str = None
      operationsmanagerid: str = None
      casemanagerid: str = None
      nsipofficerids: list[str] = []
      nsipadministrationofficerids: list[str] = []
      leadinspectorid: str = None
      inspectorids: list[str] = []
      environmentalservicesofficerid: str = None
      legalofficerid: str = None
      sourcesystem: str = None
      dateofnonacceptance: datetime = None
      dateiapidue: datetime = None
      rule6letterpublishdate: datetime = None
      notificationdateforpmandeventsdirectlyfollowingpm: datetime = None
      notificationdateforeventsdeveloper: datetime = None
      rule8letterpublishdate: datetime = None
      regions: list[str] = []
      applicantid: str = None

  test_data = {
    "caseId": "string",
    "projectName": "AutoTest_1698666963287",
    "caseReference": "WW0110055",
    "projectDescription": "Libero assumenda quam accusamus iste quos.",
    "publishStatus": "unpublished",
    "sourceSystem": "back-office-applications",
    "stage": "pre_application",
    "projectLocation": "Occaecati sint sequi voluptate maiores quidem quas eos voluptatum.",
    "projectEmailAddress": "Rafaela.Greenholt@gmail.com",
    "regions": [
      "east_midlands",
      "eastern",
      "london",
      "north_east",
      "north_west",
      "south_east",
      "south_west",
      "west_midlands",
      "yorkshire_and_the_humber"
    ],
    "easting": 156490,
    "northing": 504346,
    "welshLanguage": False,
    "mapZoomLevel": "country",
    "anticipatedDateOfSubmission": "2025-09-05T00:00:00.000Z",
    "anticipatedSubmissionDateNonSpecific": "Q2 2032",
    "datePINSFirstNotifiedOfProject": "2025-09-05T00:00:00.000Z",
    "dateProjectAppearsOnWebsite": "2025-09-05T00:00:00.000Z",
    "screeningOpinionSought": "2025-09-05T00:00:00.000Z",
    "screeningOpinionIssued": "2025-09-05T00:00:00.000Z",
    "scopingOpinionSought": "2025-09-05T00:00:00.000Z",
    "scopingOpinionIssued": "2025-09-05T00:00:00.000Z",
    "section46Notification": "2025-09-05T00:00:00.000Z",
    "dateOfDCOSubmission": "2025-09-05T00:00:00.000Z",
    "deadlineForAcceptanceDecision": "2025-09-05T00:00:00.000Z",
    "dateOfDCOAcceptance": "2025-09-05T00:00:00.000Z",
    "dateOfNonAcceptance": "2025-09-05T00:00:00.000Z",
    "dateOfRepresentationPeriodOpen": "2025-09-05T00:00:00.000Z",
    "dateOfRelevantRepresentationClose": "2025-09-05T00:00:00.000Z",
    "extensionToDateRelevantRepresentationsClose": "2025-09-05T00:00:00.000Z",
    "dateRRepAppearOnWebsite": "2025-09-05T00:00:00.000Z",
    "dateIAPIDue": "2025-09-05T00:00:00.000Z",
    "rule6LetterPublishDate": "2025-09-05T00:00:00.000Z",
    "preliminaryMeetingStartDate": "2025-09-05T00:00:00.000Z",
    "notificationDateForPMAndEventsDirectlyFollowingPM": "2025-09-05T00:00:00.000Z",
    "notificationDateForEventsDeveloper": "2025-09-05T00:00:00.000Z",
    "dateSection58NoticeReceived": "2025-09-05T00:00:00.000Z",
    "confirmedStartOfExamination": "2025-09-05T00:00:00.000Z",
    "rule8LetterPublishDate": "2025-09-05T00:00:00.000Z",
    "deadlineForCloseOfExamination": "2025-09-05T00:00:00.000Z",
    "dateTimeExaminationEnds": "2025-09-05T00:00:00.000Z",
    "stage4ExtensionToExamCloseDate": "2025-09-05T00:00:00.000Z",
    "deadlineForSubmissionOfRecommendation": "2025-09-05T00:00:00.000Z",
    "dateOfRecommendations": "2025-09-05T00:00:00.000Z",
    "stage5ExtensionToRecommendationDeadline": "2025-09-05T00:00:00.000Z",
    "deadlineForDecision": "2025-09-05T00:00:00.000Z",
    "confirmedDateOfDecision": "2025-09-05T00:00:00.000Z",
    "stage5ExtensionToDecisionDeadline": "2025-09-05T00:00:00.000Z",
    "jRPeriodEndDate": "2025-09-05T00:00:00.000Z",
    "dateProjectWithdrawn": "2025-09-05T00:00:00.000Z",
    "sector": "WW - Waste Water",
    "projectType": "WW01 - Waste Water Treatment Plants",
    "applicantId": "100000005",
    "nsipOfficerIds": [],
    "nsipAdministrationOfficerIds": [],
    "inspectorIds": [],
    "extra_field": 123
  }

  # convert input data dicionary keys to lowercase for comparison with model
  test_data_lower = convert_to_lower(test_data)

  # pprint.pprint(test_data_lower)
  # print("*"*20)

  # define json schema for model
  json_schema = nsip.model_json_schema(by_alias=False)

  # pprint.pprint(json_schema)
  # print("*"*20)
  # output_dict = fill_nonexistent_properties(test_data_lower, json_schema)

  # pprint.pprint(output_dict)

  # nsip.model_rebuild()

  try:
      print(nsip(**test_data_lower).model_dump())
      print("Success!")
  except ValidationError as e:
      print(e)

model()