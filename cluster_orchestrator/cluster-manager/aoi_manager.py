import datetime
from operator import itemgetter

aoi_by_client = {}
aoi_window_size = 12

def calculate_aoi(client_id, timestamp):
  dt = datetime.datetime.now()
  # We use milliseconds now as AoI time unit.
  current_ts = dt.microsecond / 1000
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
  average_aoi = current_aggregation['area_sum'] / (prev_delay + prev_t - timestamps[0])
  # TODO: verify this PAoI calculation and see if there's a more accurate one?
  peak_aoi = max(current_aggregation['peak_aoi'], current_ts - (prev_delay + prev_t))
  current_aggregation['peak_aoi'] = peak_aoi
  return average_aoi, peak_aoi
