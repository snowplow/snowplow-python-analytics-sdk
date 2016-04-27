import re
import json

SCHEMA_PATTERN = re.compile(""".+:([a-zA-Z0-9_\.]+)/([a-zA-Z0-9_]+)/[^/]+/(.*)""")

def fix_schema(prefix, schema):
	match = re.match(SCHEMA_PATTERN, schema)
	if match:
		snake_case_organization = match.group(1).replace('.', '_').lower()
		snake_case_name = re.sub('([^A-Z_])([A-Z])', '\g<1>_\g<2>', match.group(2)).lower()
		model = match.group(3).split('-')[0]
		return "{}_{}_{}_{}".format(prefix, snake_case_organization, snake_case_name, model)
	else:
		raise SnowplowEventTransformationException([
			"Schema {} does not conform to regular expression {}".format(schema, SCHEMA_PATTERN)
		])

def parse_contexts(contexts): # string
	my_json = json.loads(contexts)
	data = my_json['data']
	distinct_contexts = {}
	for context in data:
		schema = fix_schema("contexts", context['schema'])
		inner_data = context['data']
		if schema not in distinct_contexts:
			distinct_contexts[schema] = [inner_data]
		else:
			distinct_contexts[schema].append(inner_data)
	output = []
	for key in distinct_contexts:
		output.append((key, distinct_contexts[key]))
	return output

def parse_unstruct(unstruct): # string
	my_json = json.loads(unstruct)
	data = my_json['data']
	schema = data['schema']
	if 'data' in data:
		inner_data = data['data']
	else:
		raise SnowplowEventTransformationException(["Could not extract inner data field from unstructured event"])
	fixed_schema = fix_schema("unstruct_event", schema)
	return (fixed_schema, inner_data)
