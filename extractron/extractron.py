#!/usr/bin/env python2

"""
Extractron

Extracts data based on rule lists.

Copyright Geoff Howland, 2014.  MIT License.
"""


import sys
import os
import getopt
import re



DEFAULT_OUTPUT_FORMAT = 'yaml'


def LoadRules(rule_path, command_options):
  """Load the rules from a YAML or JSON"""
  # Format: Default Unknown
  file_format = command_options.get('rule_format', None)
  
  # File format was not specified, try to infer from the file path suffix
  if file_format == None:
    suffix = rule_path.split('.')[-1]
    
    if suffix in ('yaml', 'json'):
      file_format = suffix
    else:
      Usage('Unknown rule-format.  Use option format or rename file to end in .yaml or .json.')
  
  
  # Import from their formats
  if file_format == 'yaml':
    data = LoadYaml(rule_path)
  
  elif file_format == 'json':
    data = LoadJson(rule_path)
  
  else:
    Usage('Unknown file format, this should never occur: %s' % file_format)
  
  
  return data


def Extract(rule_path, input_path_list, command_options):
  """Extract data."""
  data = []
  
  
  # Load the rules, determine the format inside this
  rules = LoadRules(rule_path, command_options)
  
  # List of text sources
  text_list = []
  
  # If we have real file paths to load (not using STDIN)
  if input_path_list:
    # Process each of the paths
    for path in input_path_list:
      if command_options.get('verbose', False):
        sys.stderr.write('Processing file: %s\n' % path)
      
      # Load this file's textual data
      fp = None
      try:
        fp = open(path)
        text = fp.read()
        
        text_list.append(text)
      
      finally:
        if fp:
          fp.close()
  
  # Else, use STDIN
  else:
    text = sys.stdin.read()
    
    text_list.append(text)
  
  
  # Result will be a list of data
  data_list = []
  
  # Process the data for each of these texts, append in order
  for text in text_list:
    data = ProcessText(text, rules, command_options)
    
    data_list += data
  
  return data_list


def LoadYaml(path):
  """Returns data from YAML file"""
  #NOTE(g): Import is done here, instead of the top of the file, to not require this module if it is not used
  import yaml
  
  fp = None
  try:
    fp = open(path)
    
    data = yaml.load(fp)
  
  finally:
    if fp:
      fp.close()
  
  return data


def DumpYaml(data):
  """Returns textual YAML from data"""
  #NOTE(g): Import is done here, instead of the top of the file, to not require this module if it is not used
  import yaml
  
  text = yaml.safe_dump(data)
  
  return text


def LoadJson(path):
  """Returns data from JSON file"""
  #NOTE(g): Import is done here, instead of the top of the file, to not require this module if it is not used
  import json
  
  fp = None
  try:
    fp = open(path)
    
    data = yaml.load(fp)
  
  finally:
    if fp:
      fp.close()
  
  return data


def DumpJson(data):
  """Returns textual JSON from data"""
  #NOTE(g): Import is done here, instead of the top of the file, to not require this module if it is not used
  import json
  
  text = yaml.dumps(data)
  
  return text


def DumpPprint(data):
  """Returns textual JSON from data"""
  #NOTE(g): Import is done here, instead of the top of the file, to not require this module if it is not used
  import pprint
  
  text = pprint.pformat(data)
  
  return text


def DumpCsv(data):
  """Returns textual JSON from data"""
  
  raise Exception('TBI: Need standard container structure for this to work, cause its flat...')


def ProcessText(text, rules, command_options):
  """"""
  # Turn this path's text into lines, which is what we process
  lines = text.split('\n')
  
  # No previous line at the start
  previous_line_data = None
  
  # Processing data
  processing = {'data':{}, 'offset_processed':0}
  
  result = []
  
  # Process the lines  
  for line in lines:
    saved_previous_line_data = previous_line_data
    
    line_result = ProcessLine(line, rules, processing, previous_line_data)
    
    result.append(line_result)
  
  
  return result


def ProcessLine(line, rules, processing, previous_line_data):
  """Process this life, based on it's current position and spec."""
  line_data = {'line':line, 'line_offset':processing['offset_processed']}
  
  # Update with always-included data, like glob keys, and the component
  line_data.update(processing['data'])
  
  # Test if this line is multi-line (positive test)
  is_multi_line = False
  for rule in rules:
    if rule.get('multi line regex test', False):
      if re.match(rule['regex'], line):
        is_multi_line = True
        break
  # Negative regex test
  for rule in rules:
    if rule.get('multi line regex not', False):
      if re.match(rule['regex'], line):
        is_multi_line = True
        break
  
  # If this is multi_line and we have a real previous line to embed this data in
  if is_multi_line and previous_line_data != None:
    #print 'Multiline: %s' % line
    if 'multiline' not in previous_line_data:
      previous_line_data['multiline'] = []
    
    previous_line_data['multiline'].append(line)


  # Only process rules on first lines (not multi lines), and return the line_data to be the next line's previous_line_data
  if not is_multi_line:
    #print line
    
    # Start with: We havent found a match yet
    match_found = False
    
    for item in rules:
      # Skip the multi-line regext test/not rules
      if item.get('multi line regex test', False) or item.get('multi line regex not', False):
        continue
      
      # Break out our terms for this rule item
      terms = re.findall('%\((.*?)\)s', item['regex'])
      #print item['regex']
      #print terms
      
      regex = item['regex']
      
      # Pre-processing step, to remove any conflicting characters with the rest of the regex which need to be escaped/sanitized
      for term in terms:
        regex = regex.replace('%%(%s)s' % term, 'MATCHMATCHMATCH')
      
      regex = SanitizeRegex(regex)
      regex = regex.replace('MATCHMATCHMATCH', '(.*?)')
      
      #print '--- %s' % item['id']
      #print regex
      #print line
        
      regex_result = re.findall(regex, line)
      #print regex_result
      if regex_result:
        
        # Python does something stupid with multiple variables, so pull them out of the embedded tuple it adds to the list
        if type(regex_result[0]) == tuple:
          regex_result = regex_result[0]
        
        for count in range(0, len(terms)):
          #print '%s: %s: %s' % (count, terms[count], regex_result[count])
          line_data[terms[count]] = regex_result[count]
        
        #print regex
        #print 'MATCHED! %s' % regex
        #print regex_result
        
        match_found = True
        
        # Save the line match ID, so we can reference it for markup/state information
        line_data['__rule_id__'] = item['id']
        
        break
      
    return line_data
  
  # Else, this is multi-line, so return it to continue to be the next line's previous_line_data
  else:
    #TODO(g): Save this multi-line data every time?  Otherwise when does it get saved out?
    pass
    
    return previous_line_data


def SanitizeRegex(text):
  characters = '()[].*?'
  
  for character in characters:
    text = text.replace(character, '\\' + character)
  
  return text


def Usage(error=None):
  """Print usage information, any errors, and exit.  

  If errors, exit code = 1, otherwise 0.
  """
  output = ''
  
  if error:
    output += '\nERROR: %s\n' % error
    exit_code = 1
  else:
    exit_code = 0
  
  output += '\n'
  output += 'usage: %s [options] <rule_path> [input_file]' % os.path.basename(sys.argv[0])
  output += '\n'
  output += 'rule_path is the path to the rule YAML or JSON file.\n'
  output += '     ".yaml" or ".json" prefix will automatically set the rule file format.\n'
  output += '\n'
  output += 'input_file is optional, and is the path of the text input file.\n'
  output += '     If input_file is not present, STDIN will be used instead.\n'
  output += '\n'
  output += 'Options:\n'
  output += '\n'
  output += '  -h, -?, --help                      This usage information\n'
  output += '  -v, --verbose                       Verbose output\n'
  output += '  -r <type>, --rule-format=<type>     Rule file format: yaml, json\n'
  output += '  -o <type>, --output-format=<type>   Output file format: yaml, json, pprint, csv\n'
  output += '\n'
  
  
  # STDOUT - Non-error exit
  if exit_code == 0:
    sys.stdout.write(output)
  # STDERR - Failure exit
  else:
    sys.stderr.write(output)
  
  sys.exit(exit_code)


def Main(args=None):
  if not args:
    args = []

  
  long_options = ['verbose', 'help', 'rule-format=', 'output-format=']
  
  try:
    (options, args) = getopt.getopt(args, '?hvr:o:', long_options)
  except getopt.GetoptError, e:
    Usage(e)
  
  # Dictionary of command options, with defaults
  command_options = {}
  command_options['verbose'] = False
  command_options['always_yes'] = False
  command_options['output_format'] = DEFAULT_OUTPUT_FORMAT
  command_options['rule_format'] = None
  
  
  # Process out CLI options
  for (option, value) in options:
    # Help
    if option in ('-h', '-?', '--help'):
      Usage()
    
    # Verbose output information
    elif option in ('-v', '--verbose'):
      command_options['verbose'] = True
    
    # Always answer Yes to prompts
    elif option in ('-y', '--yes'):
      command_options['always_yes'] = True
    
    # Output Format
    elif option in ('-o', '--output-format'):
      if value not in ('yaml', 'json', 'pprint', 'csv'):
        Usage('Unknown output-format type: %s' % value)
      
      command_options['output_format'] = value
    
    # Rule Format
    elif option in ('-r', '--rule-format'):
      if value not in ('yaml', 'json'):
        Usage('Unknown rule-format type: %s' % value)
      
      command_options['rule_format'] = value
    
    # Invalid option
    else:
      Usage('Unknown option: %s' % option)


  # Ensure we at least have one spec file
  if len(args) < 1:
    Usage('No rule file path specified')
  

  #try:
  if 1:
    data = Extract(args[0], args[1:], command_options)
    
    # Dump formatted output
    if command_options['output_format'] == 'yaml':
      print DumpYaml(data)
      
    elif command_options['output_format'] == 'json':
      print DumpJson(data)
      
    elif command_options['output_format'] == 'pprint':
      print DumpPprint(data)
      
    elif command_options['output_format'] == 'csv':
      print DumpCsv(data)
      
    else:
      raise Exception('Unknown output-format, this should never happen: %s' % command_options['output_format'])
  
  #NOTE(g): Catch all exceptions, and return in properly formatted output
  #TODO(g): Implement stack trace in Exception handling so we dont lose where this
  #   exception came from, and can then wrap all runs and still get useful
  #   debugging information
  #except Exception, e:
  else:
    Error({'exception':str(e)}, command_options)  


if __name__ == '__main__':
  Main(sys.argv[1:])
