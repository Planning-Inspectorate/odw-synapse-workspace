"""
This module defines a pydantic model to be used for data validation. 
It defines the fields we expect to receive from the ODT Service Bus messages. 
The model does not accept extra fields and all fields are mandatory. 
Expected data types and constraints are also defined for each field.
"""
from pydantic import BaseModel, ValidationError
from servicebus_funcs import get_messages
import config

_NAMESPACE = config.ODT_NAMESPACE
_SUBSCRIPTION = config.NSIP_SUBSCRIPTION
_TOPIC = config.NSIP_TOPIC
_MAX_MESSAGE_COUNT = config.MAX_MESSAGE_COUNT
_CREDENTIAL = config.CREDENTIAL


def model() -> list[dict]:
    """
    The 'model' function defines a class 'Nsip' and performs some operations on test data.

    Returns:
        data - a list of dictionaries containng Service Bus message data
    """

    class Nsip(BaseModel):

        """
        Description of this class to go here
        """

        model_config = config.MODEL_CONFIG

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

    class MessageInstances(BaseModel):

        """
        Represents a pydantic model for a list of ServiceUser instances.

        Args:
            messagedata (list[ServiceUser]): The list of ServiceUser instances.

        Attributes:
            messagedata (list[ServiceUser]): The list of ServiceUser instances.
        """

        messagedata: list[Nsip]

    _data = get_messages(
        _NAMESPACE, _CREDENTIAL, _TOPIC, _SUBSCRIPTION, _MAX_MESSAGE_COUNT
    )

    _messages_lower = []
    for message in _data:
        _lowercase_message = {k.lower(): v for k, v in message.items()}
        _messages_lower.append(_lowercase_message)
    if not _messages_lower:
        print("NO MESSAGES TO PROCESS - VALIDATION OK")
        return _data
    else:
        try:
            MessageInstances(messagedata=_messages_lower)
            print("VALIDATION SUCCEEDED!")
            print(f"{len(_messages_lower)} MESSAGES PROCESSED")
            return _data
        except ValidationError as e:
            print(e)
            raise e
