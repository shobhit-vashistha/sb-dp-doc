import json
import csv


def uid_status(uid):
    if not uid:
        return None
    elif uid == 'anonymous':
        return uid
    else:
        return 'present'


class Grouping(object):

    def __init__(self, file_path='telemetry-dump.txt'):
        self.file_path = file_path
        self.total_batch_events = 0
        self.unique_batch_events = 0
        self.total_events = 0
        self.unique_events = 0
        self.batch_ids = set()
        self.event_ids = set()
        self.unique_events = []
        self.grouping_data = []
        self.grouping_key_fields = [
            ('eid', lambda x: x['eid']),
            ('actor_type', lambda x: not x['actor'].get('type', None)),
            ('actor_id_present', lambda x: bool(x['actor'].get('id', None))),
            ('context_channel', lambda x: x['context'].get('channel', None)),
            ('context_env', lambda x: x['context'].get('env', None)),
            ('context_uid_status', lambda x: uid_status(x['context'].get('uid', None))),
            ('context_pdata_id', lambda x: x['context'].get('pdata', {}).get('id', None)),
            ('object_id_present', lambda x: bool(x['object'].get('id', None))),
            ('edata_pageid', lambda x: x['edata'].get('pageid', None)),
            ('edata_type', lambda x: x['edata'].get('type', None)),
            ('edata_subtype', lambda x: x['edata'].get('subtype', None)),
        ]
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
        print(' Total Batch Events:', self.total_batch_events)
        print('Unique Batch Events:', self.unique_batch_events)
        print('       Total Events:', self.total_events)
        print('      Unique Events:', self.unique_events)
        print('\n')

    def print_csv(self):
        fields = [f[0] for f in self.grouping_key_fields] + ['count']
        with open('grouping.csv', 'w', newline='') as csv_file:
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
            self.total_batch_events += 1
            yield json.loads(line)

    def de_dup_batch(self, batch_events):
        for batch_event in batch_events:
            mid = batch_event['mid']
            if mid not in self.batch_ids:
                self.batch_ids.add(mid)
                self.unique_batch_events += 1
                yield batch_event

    def extract_events(self, batch_events):
        for batch_event in batch_events:
            for event in batch_event['events']:
                self.total_events += 1
                yield event

    def de_dup(self, events):
        for event in events:
            mid = event['mid']
            if mid not in self.event_ids:
                self.event_ids.add(mid)
                self.unique_events += 1
                yield event

    def get_key(self, event):
        return tuple(func(event) for field_name, func in self.grouping_key_fields)

    def group(self, unique_events):
        key_count = {}
        for event in unique_events:
            key = self.get_key(event)
            if key in key_count:
                key_count[key] += 1
            else:
                key_count[key] = 1

        data = []
        for key, count in key_count.values():
            row = {'count': count}
            for (field_name, func), value in zip(self.grouping_key_fields, key):
                row[field_name] = value
            data.append(row)

        data.sort(key=lambda x: (x['eid'], x['edata_pageid'], x['edata_type']))
        return data
