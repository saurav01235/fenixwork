import json


def map_flatten_schema_event_params(key_value):
    temp = {}
    prop_name = "event_params_" + key_value["key"]
    value = list(key_value["value"].values())
    if len(value) > 0:
        temp[prop_name] = value[0]

    return temp


def map_flatten_schema_user_properties(key_value):
    temp = {}
    prop_name = "user_properties_" + key_value["key"]
    value = list(key_value["value"].values())
    if len(value) > 0:
        temp[prop_name] = value[0]

    return temp


def map_extract_non_null_val(val):
    temp = {}
    for k, v in val.items():
        if v and v != "null" and k != "set_timestamp_micros":
            temp[k] = v
    return temp


def map_user_properties_extarcted_key(events):
    temp = {"key": events["key"], "value": map_extract_non_null_val(events["value"])}
    return temp


def map_extract_events(raw):
    if raw.get("event_params"):
        raw["event_params"] = list(map(map_user_properties_extarcted_key, raw["event_params"]))
        for i in map(map_flatten_schema_event_params, raw["event_params"]):
            for k, v in i.items():
                raw[k] = v
        del raw["event_params"]

    if raw.get("user_properties"):
        raw["user_properties"] = list(map(map_user_properties_extarcted_key, raw["user_properties"]))
        for i in map(map_flatten_schema_user_properties, raw["user_properties"]):
            for k, v in i.items():
                raw[k] = v
        del raw["user_properties"]

    return raw


file = open("/Users/saurav/IdeaProjects/fanixwork_task/sample_events.json", mode="r")
data = json.load(file)
extracted_records = map(map_extract_events, list(data))
json_data = list(extracted_records)
with open('data.txt', 'w') as outfile:
    json.dump(json_data, outfile)

file.close()
outfile.close()
