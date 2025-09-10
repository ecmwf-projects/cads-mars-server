# this test is supposed to be executed in an environment where cads-mars-server is installed and running as a server.
# This test will use the settings from the environment where it is executed.
from cads_mars_server.config import get_config, local_target
from cads_mars_server.client_file import RemoteMarsClientCluster
import time

config = get_config()

workers = config['WORKERS']
print(f"Workers from config: {workers}")
_urls = [f'http://{_}:9000' for _ in workers]
print(f"Worker URLs: {_urls}")
cluster = RemoteMarsClientCluster(urls=_urls)

# Add required fields to the env dictionary:
env = {
    "user_id": "testingcci1cephfs",
    "request_id": "testing-request-cxgb",
    "namespace": "prod",
    "host": "test-host"
}
env["username"] = str(env["namespace"]) + ":" + str(env["user_id"]).split("-")[-1]


request = {
    'dataset': ['members'],
    'time': ['00:00:00', '01:00:00', '02:00:00', '03:00:00', '04:00:00', '05:00:00', '06:00:00', '07:00:00', '08:00:00', '09:00:00', '10:00:00', '11:00:00', '12:00:00', '13:00:00', '14:00:00', '15:00:00', '16:00:00', '17:00:00', '18:00:00', '19:00:00', '20:00:00', '21:00:00', '22:00:00', '23:00:00'],
    'param': ['140239'],
    'class': ['ea'],
    'expect': ['any'],
    'number': ['all'],
    'levtype': ['sfc'],
    'date': ['2000-02-01']
}

reply = cluster.execute(request, env)
print(f"Initial request status: {reply}")

while reply.data is None:
    if reply.data is None:
        time.sleep(1)
    reply = cluster.execute(request, env)
    print(f"Waiting for the file to be ready. Current status: {reply.message}")
print(f"Request completed with status: {reply.data}")
target = local_target(reply.data)
print(f"File is available at {target}")