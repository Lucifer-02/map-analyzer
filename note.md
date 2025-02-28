Traceback (most recent call last):
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/urllib3/connectionpool.py", line 787, in urlopen
    response = self._make_request(
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/urllib3/connectionpool.py", line 534, in _make_request
    response = conn.getresponse()
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/urllib3/connection.py", line 516, in getresponse
    httplib_response = super().getresponse()
  File "/usr/lib/python3.10/http/client.py", line 1375, in getresponse
    response.begin()
  File "/usr/lib/python3.10/http/client.py", line 318, in begin
    version, status, reason = self._read_status()
  File "/usr/lib/python3.10/http/client.py", line 287, in _read_status
    raise RemoteDisconnected("Remote end closed connection without"
http.client.RemoteDisconnected: Remote end closed connection without response

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/requests/adapters.py", line 667, in send
    resp = conn.urlopen(
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/urllib3/connectionpool.py", line 841, in urlopen
    retries = retries.increment(
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/urllib3/util/retry.py", line 474, in increment
    raise reraise(type(error), error, _stacktrace)
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/urllib3/util/util.py", line 38, in reraise
    raise value.with_traceback(tb)
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/urllib3/connectionpool.py", line 787, in urlopen
    response = self._make_request(
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/urllib3/connectionpool.py", line 534, in _make_request
    response = conn.getresponse()
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/urllib3/connection.py", line 516, in getresponse
    httplib_response = super().getresponse()
  File "/usr/lib/python3.10/http/client.py", line 1375, in getresponse
    response.begin()
  File "/usr/lib/python3.10/http/client.py", line 318, in begin
    version, status, reason = self._read_status()
  File "/usr/lib/python3.10/http/client.py", line 287, in _read_status
    raise RemoteDisconnected("Remote end closed connection without"
urllib3.exceptions.ProtocolError: ('Connection aborted.', RemoteDisconnected(
'Remote end closed connection without response'))

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/googlemaps/client.py", line 315, in _request
    response = requests_method(base_url + authed_url,
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/requests/sessions.py", line 602, in get
    return self.request("GET", url, **kwargs)
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/requests/sessions.py", line 589, in request
    resp = self.send(prep, **send_kwargs)
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/requests/sessions.py", line 703, in send
    r = adapter.send(request, **kwargs)
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/requests/adapters.py", line 682, in send
    raise ConnectionError(err, request=request)
requests.exceptions.ConnectionError: ('Connection aborted.', RemoteDisconnect
ed('Remote end closed connection without response'))

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/./main.py", line 636, i
n <module>
    main()
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/./main.py", line 510, i
n main
    test_area_api()
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/./main.py", line 442, i
n test_area_api
    places_api.nearby_search(
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/engines/google_api/plac
es_api.py", line 70, in nearby_search
    response = places_nearby(
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/googlemaps/places.py", line 360, in places_nearby
    return _places(
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/googlemaps/places.py", line 425, in _places
    return client._request(url, params)
  File "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/.venv/lib/python3.10/si
te-packages/googlemaps/client.py", line 320, in _request
    raise googlemaps.exceptions.TransportError(e)
googlemaps.exceptions.TransportError: ('Connection aborted.', RemoteDisconnec
ted('Remote end closed connection without response'))

googlemaps.exceptions.ApiError: REQUEST_DENIED (You must enable Billing on th
e Google Cloud Project at https://console.cloud.google.com/project/_/billing/
enable Learn more at https://developers.google.com/maps/gmp-get-started)
