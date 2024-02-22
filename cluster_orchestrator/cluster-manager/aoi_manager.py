from asyncio import current_task
import time
from operator import itemgetter
from datetime import datetime

aoi_by_client = {}
aoi_window_size = 50000
# print("window_size=", aoi_window_size)

aoi_history_by_client = {}
aoi_history_size = 100000
aoi_rate_by_client = {}

def calculate_aoi(client_id, timestamp):
  # We use milliseconds now as AoI time unit.
  current_ts = round(time.time() * 1000)
  delay = current_ts - timestamp

  if client_id not in aoi_by_client:
    current_aggregation = {
      'prev_t': timestamp,
      'prev_delay': delay,
      'areas': [0.0],
      'timestamps': [timestamp],
      'area_sum': 0.0,
      'peak_aoi': 0.0,
    }
    aoi_by_client[client_id] = current_aggregation
    aoi_history_by_client[client_id] = []
    return 0.0, 0.0
  
  current_aggregation = aoi_by_client[client_id]
  prev_t, prev_delay, timestamps = itemgetter('prev_t', 'prev_delay', 'timestamps')(current_aggregation)
  area = ((timestamp - prev_t + prev_delay) ** 2 - prev_delay ** 2) / 2
  current_aggregation['prev_t'] = timestamp
  current_aggregation['prev_delay'] = delay
  current_aggregation['areas'].append(area)
  current_aggregation['timestamps'].append(timestamp)
  current_aggregation['area_sum'] += area
  if len(current_aggregation['areas']) > aoi_window_size:
    # TODO: optimize performance here?
    removed_area = current_aggregation['areas'].pop(0)
    current_aggregation['area_sum'] -= removed_area
    current_aggregation['timestamps'].pop(0)
  average_aoi = round(current_aggregation['area_sum'] / (prev_delay + prev_t - timestamps[0]))
  # TODO: verify this PAoI calculation and see if there's a more accurate one?
  peak_aoi = round(max(current_aggregation['peak_aoi'], current_ts - (prev_delay + prev_t)))
  current_aggregation['peak_aoi'] = peak_aoi

  # Populate history for graph drawing.
  aoi_history_by_client[client_id].append([current_ts, average_aoi])
  # if (len(aoi_history_by_client[client_id]) > aoi_history_size):
  #   aoi_history_by_client[client_id].pop(0)
  return average_aoi, peak_aoi

def get_aoi():
  second = datetime.now().second
  minute = datetime.now().minute
  hour = datetime.now().hour
  day = datetime.now().day
  month = datetime.now().month
  year = datetime.now().year
  timestamp = str(year) + "_" + str(month) + "_" + str(day) + "_" + str(hour) + ":" + str(minute) + ":" + str(second)
  aoi_log = open("/aoi_log/log_" + timestamp + ".txt", "w")
  aoi_log.write(str(aoi_history_by_client))
  aoi_log.close()

  return {
    'history': 'written to file',
    'rate': aoi_rate_by_client,
  }
  # return {
  #   'history': aoi_history_by_client,
  #   'rate': aoi_rate_by_client,
  # }

def reset_aoi():
  aoi_history_by_client.clear()
  aoi_rate_by_client.clear()
  aoi_record_by_client.clear()
  return {
    'history': aoi_history_by_client,
    'rate': aoi_rate_by_client,
  }

aoi_record_by_client = {}

def calculate_acp_aoi(client_id, departure_ts, cpu, mem, packet_size, arrival_ts, msg_seq):
  # arrival_ts = round(time.time() * 1000)
  delay = arrival_ts - departure_ts
  print("delay: " + str(delay))
  current = {
    'delay': delay,
    'arrival_ts': arrival_ts,
  }
  if client_id not in aoi_record_by_client:
    aoi_record_by_client[client_id] = []
    aoi_record_by_client[client_id].append(current)
    aoi_history_by_client[client_id] = []
    aoi_history_by_client[client_id].append([departure_ts, arrival_ts, delay, msg_seq])
    aoi_rate_by_client[client_id] = {
      'last_100_cadence_arrival': 0,
      'last_100_cadence_departure': 0,
    }
    return 0.0

  aoi_record_by_client[client_id].append(current)
  age_estimate = 0
  sum_denominator = 0
  prev_delay = -1
  prev_arrival = -1
  # Loop through all the records to calculate current age.
  for record in aoi_record_by_client[client_id]:
    if prev_delay == -1:
      # record t`_i-1 - t_i-1
      prev_delay = record['delay']
      # record t`_i-1
      prev_arrival = record['arrival_ts']
      continue
    # t`_i - t`_i-1
    departure_time = record['arrival_ts'] - prev_arrival
    # (t`_i-1 - t_i-1) * (t`_i - t`_i-1) + 0.5 * (t`_i - t`_i-1) ^ 2
    current_aoi = prev_delay * departure_time + 0.5 * departure_time * departure_time
    age_estimate += current_aoi
    prev_delay = record['delay']
    prev_arrival = record['arrival_ts']
    # += t`_i - t`_i-1
    sum_denominator += departure_time

  # Populate history for graph drawing.
  average_aoi = age_estimate / sum_denominator
  aoi_history_by_client[client_id].append([departure_ts, arrival_ts, average_aoi, cpu, mem, packet_size, current_aoi, msg_seq])
  history_size = len(aoi_history_by_client[client_id])
  if history_size > aoi_history_size:
    aoi_history_by_client[client_id].pop(0)
  
  # Update aoi count and rate.
  # 1. Calculate the seconds taken for the last 100 messages.
  if history_size >= 10:
    aoi_rate_by_client[client_id]['last_100_cadence_arrival'] = (arrival_ts - aoi_history_by_client[client_id][history_size - 10][1]) * 1000 / 10
    aoi_rate_by_client[client_id]['last_100_cadence_departure'] = (departure_ts - aoi_history_by_client[client_id][history_size - 10][0]) * 1000 / 10
  return average_aoi
