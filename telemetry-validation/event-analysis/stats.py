import json
import os


MAIN_TOPICS = [
    'dev.telemetry.ingest',
    'dev.telemetry.raw',
    'dev.telemetry.unique',
    'dev.telemetry.unique.secondary',
    'dev.telemetry.unique.primary',
    'dev.telemetry.denorm',
    'dev.druid.events.telemetry',
    'dev.telemetry.extractor.duplicate',
    'dev.telemetry.extractor.failed',
    'dev.telemetry.failed',
    'dev.telemetry.error',
    'dev.telemetry.duplicate',
    'dev.druid.events.log',
    'dev.telemetry.audit',
    'dev.telemetry.assess.raw'
]

BATCH_EVENT_TOPICS = [
    'dev.telemetry.ingest',
    'dev.telemetry.extractor.duplicate',
    'dev.telemetry.extractor.failed'
]

INGESTION_TOPIC = 'dev.telemetry.ingest'


def topic_file_name(topic):
    return '%s.txt' % topic


def topic_file_path(dir_path, topic):
    return os.path.join(dir_path, topic_file_name(topic))


def read_events(file_path):
    file = open(file_path, 'rb')
    while True:
        line = file.readline()
        if not line:
            break
        yield json.loads(line)


def de_duplicate(events):
    seen = set()
    for event in events:
        if event['mid'] not in seen:
            seen.add(event['mid'])
            yield event


class Analyser(object):

    def __init__(self, dir_path):
        self.dir_path = dir_path

        self.topic_events = {}
        self.topic_event_counts = {}

    def read_all_topics(self):
        for topic in MAIN_TOPICS:
            self.topic_events[topic] = list(read_events(topic_file_path(self.dir_path, topic)))





def extract(batch_events, de_dup=True):
    """
    telemetry-extractor
    """
    batch_event_count = len(batch_events)
    if de_dup:
        batch_events = de_duplicate(batch_events)
    unique_batch_event_count = len(batch_events)
    extracted_events = []
    for batch_event in batch_events:
        events = batch_event.get('events')
        for event in events:
            extracted_events.append(event)
    event_count = len(extracted_events)

    return extracted_events, batch_event_count, unique_batch_event_count, event_count



def analyse_data(data_dir, topics=MAIN_TOPICS):
    topic_event_data = {}
    for topic in topics:
        pass
