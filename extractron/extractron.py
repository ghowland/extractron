#!/usr/bin/env python2

"""
Extractron

Extracts data based on rule lists.

Copyright Geoff Howland, 2014.  MIT License.
"""


import sys
import getopt
import re


DEFAULT_OUTPUT_FORMAT = 'yaml'


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
  
  text = yaml.dumps(data)
  
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


def ProcessTextRule(line_data, process_rule):
  """Updates line_data based on the rules."""
  # Split
  if process_rule['type'] == 'split':
    split_data = process_rule['split']
    
    #print split_data
    #print
    
    parts = line_data[process_rule['key']].split(split_data['separator'], split_data.get('max split', -1))
    
    #print parts
    
    for (key, part_indexes) in split_data['values'].items():
      key_parts = []
      
      try:
        for part_index in part_indexes:
          key_parts.append(parts[part_index])
      except IndexError, e:
        log('WARNING: Part not found: %s: %s: %s' % (part_index, parts, line_data[process_rule['key']]))
      
      line_data[key] = ' '.join(key_parts)
      
  
  # Replace
  elif process_rule['type'] == 'replace':
    # Perform replacement on each term we match
    for match in process_rule['match']:
      # Match -> Replaced (usually deleting things out)
      #print 'Replacing: "%s" with "%s"' % (match, process_rule['replace'])
      #print line_data[process_rule['key']]
      line_data[process_rule['key']] = line_data[process_rule['key']].replace(match, process_rule['replace'])
      #print line_data[process_rule['key']]
  
  # Delete
  elif process_rule['type'] == 'delete':
    if process_rule['key'] in line_data:
      del line_data[process_rule['key']]
  
  # Match
  elif process_rule['type'] == 'match':
    database = LoadYaml(process_rule['database'])
    
    match_found = False
    
    for item in database:
      terms = re.findall('%\((.*?)\)s', item['regex'])
      #print item['regex']
      #print terms
      
      regex = item['regex']
      
      # Pre-processing step, to remove any conflicting characters with the rest of the regex which need to be escaped/sanitized
      for term in terms:
        regex = regex.replace('%%(%s)s' % term, 'MATCHMATCHMATCH')
        
      regex = SanitizeRegex(regex)
      regex = regex.replace('MATCHMATCHMATCH', '(.*?)')
      
      regex_result = re.findall(regex, line_data[process_rule['key']])
      if regex_result:
        
        # Python does something stupid with multiple variables, so pull them out of the embedded tuple it adds to the list
        if type(regex_result[0]) == tuple:
          regex_result = regex_result[0]
        
        for count in range(0, len(terms)):
          #print '%s: %s' % (count, regex_result)
          line_data[terms[count]] = regex_result[count]
        
        #print regex
        #print 'MATCHED! %s' % regex
        #print regex_result
        
        match_found = True
        
        # Save the line match ID, so we can reference it for markup/state information
        line_data[process_rule['match key']] = item['id']
        
        break
    
    if not match_found:
      #print 'MISSING: %s' % line_data[process_rule['key']]
      pass
      
  
  # Convert
  elif process_rule['type'] == 'convert':
    if process_rule['target'] == 'integer':
      try:
        line_data[process_rule['key']] = int(line_data[process_rule['key']])
      
      except ValueError, e:
        #print 'WARNING: Bad formatting: %s: %s' % (process_rule['key'], line_data)
        pass
      
    else:
      raise Exception('Unknown covnert target type: %s: %s' % (line_data['spec_path'], process_rule['rule']))
  
  # Error - Misconfiguration
  else:
    raise Exception('Unknown process type: %s: %s' % (line_data['spec_path'], process_rule['rule']))


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
  command_optinos['output_format'] = DEFAULT_OUTPUT_FORMAT
  
  
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
    
    # Invalid option
    else:
      Usage('Unknown option: %s' % option)


  # Store the command options for our logging
  utility.log.RUN_OPTIONS = command_options
  
  
  # Ensure we at least have one spec file
  if len(args) < 1:
    Usage('No action specified')
  

  #try:
  if 1:
    result = Extract(args[0], args[1:], command_options)
    pass
  
  #NOTE(g): Catch all exceptions, and return in properly formatted output
  #TODO(g): Implement stack trace in Exception handling so we dont lose where this
  #   exception came from, and can then wrap all runs and still get useful
  #   debugging information
  #except Exception, e:
  else:
    Error({'exception':str(e)}, command_options)  


if __name__ == '__main__':
  Main(sys.argv[1:])
