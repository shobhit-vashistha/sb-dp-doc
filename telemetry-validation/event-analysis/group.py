import json
import csv


def user_id_status(uid):
    if not uid:
        return None
    elif uid.lower() == 'anonymous':
        return 'anonymous'
    else:
        return 'present'


FIELD_FUNCS = (
    ('eid', lambda x: x.get('eid', None)),

    ('actor_type', lambda x: x.get('actor', {}).get('type', None)),
    ('actor_id_status', lambda x: user_id_status(x.get('actor', {}).get('id', None))),

    ('context_channel', lambda x: x.get('context', {}).get('channel', None)),
    ('context_env', lambda x: x.get('context', {}).get('env', None)),
    ('context_pdata_id', lambda x: x.get('context', {}).get('pdata', {}).get('id', None)),

    ('object_type', lambda x: x.get('object', {}).get('type', None)),

    # dropping page id for now because there is a unique id in it sometimes that messes up the grouping
    # TODO: add an option to collect some fields into arrays
    # ('edata_pageid', lambda x: x.get('edata', {}).get('pageid', None)),

    ('edata_type', lambda x: x.get('edata', {}).get('type', None)),
    ('edata_subtype', lambda x: x.get('edata', {}).get('subtype', None)),
)


FIELD_FUNC_MAP = {f: func for f, func in FIELD_FUNCS}

DEFAULT_GROUPING_KEY_FIELDS = [f for f, _ in FIELD_FUNCS]
DEFAULT_ORDER_BY = ['eid', 'edata_type']


def get_field_value(event, field_name):
    field_func = FIELD_FUNC_MAP[field_name]
    return field_func(event)


class Grouping(object):

    def __init__(self, file_path=None, output_file_path=None, grouping_key_fields=None, order_by=None):
        self.grouping_key_fields = grouping_key_fields or DEFAULT_GROUPING_KEY_FIELDS
        self.order_by = order_by or DEFAULT_ORDER_BY
        self.file_path = file_path or 'telemetry-dump.txt'
        self.output_file_path = output_file_path or 'grouping.csv'
        # counts
        self.count_batch_events = 0
        self.count_unique_batch_events = 0
        self.count_events = 0
        self.count_unique_events = 0

        self.batch_ids = set()
        self.event_ids = set()
        self.unique_events = []
        self.grouping_data = []
        self.analyze()

    def analyze(self):
        batch_events = self.get_batch_events()
        unique_batch_events = self.de_dup_batch(batch_events)
        events = self.extract_events(unique_batch_events)
        self.unique_events = list(self.de_dup(events))
        self.grouping_data = self.group(self.unique_events)
        self.print_output()

    def print_output(self):
        self.print_summary()
        self.print_csv()

    def print_summary(self):
        print('\n')
        print(' Total Batch Events:', self.count_batch_events)
        print('Unique Batch Events:', self.count_unique_batch_events)
        print('       Total Events:', self.count_events)
        print('      Unique Events:', self.count_unique_events)
        print('\n')

    def print_csv(self):
        fields = self.grouping_key_fields + ['count']
        with open(self.output_file_path, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            writer.writerow(fields)
            for data in self.grouping_data:
                writer.writerow([data[f] for f in fields])

    def get_batch_events(self):
        file = open(self.file_path, 'rb')
        while True:
            line = file.readline()
            if not line:
                break
            self.count_batch_events += 1
            yield json.loads(line)

    def de_dup_batch(self, batch_events):
        for batch_event in batch_events:
            mid = batch_event['mid']
            if mid not in self.batch_ids:
                self.batch_ids.add(mid)
                self.count_unique_batch_events += 1
                yield batch_event

    def extract_events(self, batch_events):
        for batch_event in batch_events:
            for event in batch_event['events']:
                self.count_events += 1
                yield event

    def de_dup(self, events):
        for event in events:
            mid = event['mid']
            if mid not in self.event_ids:
                self.event_ids.add(mid)
                self.count_unique_events += 1
                yield event

    def get_key(self, event):
        return tuple(get_field_value(event, field_name) for field_name in self.grouping_key_fields)

    def group(self, unique_events):
        # group and count
        key_count = {}
        for event in unique_events:
            key = self.get_key(event)
            if key in key_count:
                key_count[key] += 1
            else:
                key_count[key] = 1

        # format info into a more readable dict
        data = []
        for key, count in key_count.items():
            row = {'count': count}
            for field_name, value in zip(self.grouping_key_fields, key):
                row[field_name] = value
            data.append(row)

        # sort by order_by fields
        data.sort(key=lambda x: tuple(str(x[f]) for f in self.order_by))
        return data


if __name__ == '__main__':
    g = Grouping()
