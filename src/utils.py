import datetime

# Python code to merge dict using a single 
# expression
def Merge(dict1, dict2):
  res = {**dict1, **dict2}
  return res


session_start_time = None
operation_start_time = None

def start_session():
  global session_start_time
  global operation_start_time
  session_start_time = datetime.datetime.now()
  operation_start_time = datetime.datetime.now()


def log_running_time(event_name, is_session = False):
  global session_start_time
  global operation_start_time


  if is_session == True:
    session_diff = (datetime.datetime.now() - session_start_time).total_seconds()
    print(event_name + ': ', str(session_diff))
  else:
    operation_diff = (datetime.datetime.now() - operation_start_time).total_seconds()
    print(event_name + ': ', str(operation_diff))
    operation_start_time = datetime.datetime.now()