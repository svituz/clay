# Builtins Imports
import os
import uuid
from datetime import datetime

# HL7apy Imports
from hl7apy import load_message_profile
from hl7apy.parser import parse_message
from hl7apy.v2_5 import get_base_datatypes
DTM = get_base_datatypes()['DTM']

# Communication Layer Imports
from . import Serializer
from ..exceptions import InvalidMessage, MissingParameters


class HL7Serializer(Serializer):

    def __init__(self, message_type):
        try:
            self.serializer = _HL7_SERIALIZERS[message_type]()
        except Exception as e:
            import traceback
            traceback.print_exc(e)
            raise InvalidMessage

    def serialize(self, datum):
        return self.serializer.serialize(datum)

    def deserialize(self, message, domain):
        return self.serializer.deserialize(message)


class PDQRequestHL7(object):
    def __init__(self):
        tpl = "MSH|^~\&|INPECO_TIH|IL_LAB_BRK|INPECO_TIH|IL_PDQ_SUPP|20110727180227||QBP^Q22^QBP_Q21|82989007558297150|D|2.5|||||ITA||EN\r" \
              "QPD|IHE PDQ Query|111069|@PID.3.1^111111||||\r" \
              "RCP|I|\r"

        mp_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hl7_profiles/pdq_request")
        message_profile = load_message_profile(mp_path)

        self.message = parse_message(tpl, message_profile=message_profile)
        del self.message.qpd.qpd_3
        self.message.msh.msh_7 = DTM(datetime.now(), format="%Y%m%d%H%M%S").to_er7()
        self.message.msh.msh_10 = uuid.uuid4().hex
        self.message.qpd.qpd_2 = uuid.uuid4().hex  # create an identifier for the query

        self._query_fields = {
            "patient_id"              : "@PID.3.1",
            "patient_surname"         : "@PID.5.1.1",
            "patient_name"            : "@PID.5.2",
            "patient_birth_date"      : "@PID.7.1",
            "patient_gender"          : "@PID.8",
            "patient_address"         : "@PID.11.4",
            "patient_account_number"  : "@PID.18.1",
            "patient_visit_number"    : "@PV1.19"
        }

    def serialize(self, datum):
        valid = False
        for search_param, pid_param in self._query_fields.iteritems():
            value = datum.get(search_param, None)
            if value is not None:
                self.message.qpd.add_field("QPD_3")
                self.message.qpd.qpd_3[-1] = "{}^{}".format(pid_param, value)
                valid = True
        if not valid:
            raise MissingParameters()

        return self.message.to_mllp()

    def deserialize(self, message):
        pass


class LAB62RequestHL7(object):
    def __init__(self):
        tpl = "MSH|^~\&|INPECO_TIH|IL_LAB_BRK|INPECO_TIH|IL_LAB_INF_PRV|20140101||QBP^SLI^QBP_Q11|1111111111|D|2.5|||||ITA||EN\r" \
              "QPD|SLI^Specimen Labeling Instructions^IHE_LABTF|11111|||||||\r" \
              "RCP|I||R"

        mp_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hl7_profiles/lab_62")
        message_profile = load_message_profile(mp_path)
        self.message = parse_message(tpl, message_profile=message_profile)
        self.message.msh.msh_7 = DTM(datetime.now(), format="%Y%m%d%H%M%S").to_er7()
        self.message.msh.msh_10 = uuid.uuid4().hex
        self.message.qpd.qpd_2 = uuid.uuid4().hex # create an identifier for the query

        self._query_fields = {
            "patient_id"            : "QPD_3",
            "patient_visit_number"  : "QPD_4",
            "placer_group_number"   : "QPD_5",
            "placer_order_number"   : "QPD_6",
            "filler_order_number"   : "QPD_7"
        }

    def serialize(self, datum):
        valid = False
        for search_param, field in self._query_fields.iteritems():
            value = datum.get(search_param, None)
            if value is not None:
                valid = True
                setattr(self.message.qpd, field, value)
        if not valid:
            raise MissingParameters()

        return self.message.to_mllp()

class PDQResponseHL7(object):
    def __init__(self):
        tpl = "MSH|^~\&|PDQ Server|Mirth|Label Broker|Label Broker|20100615094820||RSP^K22^RSP_K21|8799|P|2.5|||||ITA||IT" \
              "MSA|AA|2|" \
              "QAK|111069|OK||2" \
              "QPD|Q22^Find Candidates^HL70471|111069|@PID.8^F|80|MATCHWARE|1.2||METRO HOSPITAL~SOUTH LAB" \
              "PID|1||100000000||ROSSI^MARIO^^^^^B^||19810101|M|||xx, 23^VExx^VExx^Vxx^37xx0^1xx0^H^^023xxx~^^^^^^L||||||||||||23|||||"

        mp_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hl7_profiles/pdq_response")
        message_profile = load_message_profile(mp_path)
        self.message = parse_message(tpl, message_profile=message_profile)



_HL7_SERIALIZERS = {
    "PDQ": PDQRequestHL7,
    "PDQ_RES" : PDQResponseHL7,
    "LAB_62": LAB62RequestHL7
}


# vim:tabstop=4:expandtab