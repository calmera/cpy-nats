HEADER_CONSUMER_STALLED = "Nats-Consumer-Stalled"
HEADER_DESCRIPTION = "Description"
HEADER_EXPECTED_LAST_MSG_ID = "Nats-Expected-Last-Msg-Id"
HEADER_EXPECTED_LAST_SEQUENCE = "Nats-Expected-Last-Sequence"
HEADER_EXPECTED_LAST_SUBJECT_SEQUENCE = "Nats-Expected-Last-Subject-Sequence"
HEADER_EXPECTED_STREAM = "Nats-Expected-Stream"
HEADER_LAST_CONSUMER = "Nats-Last-Consumer"
HEADER_LAST_STREAM = "Nats-Last-Stream"
HEADER_MSG_ID = "Nats-Msg-Id"
HEADER_ROLLUP = "Nats-Rollup"
HEADER_STATUS = "Status"

import re
PATTERN_NATS_FULL_URL = re.compile('(.*)://(.*):(.*)')
PATTERN_NATS_HOST_PORT = re.compile('(.*):(.*)')
