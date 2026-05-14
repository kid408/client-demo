region      = "global"
datacenters = ["dc1"]
namespace   = "default"

image = "client-demo:dev"

consul_http_addr = "http://127.0.0.1:8500"

consul_service_tags = []
discovery_service_tags = []

count  = 1
cpu    = 100
memory = 128

gateway_addr             = ""
peer_refresh_interval_ms = "5000"
reconnect_delay_ms       = "3000"
heartbeat_interval_ms    = "2000"
session_gap_ms           = "3000"
virtual_clients          = "5"
heartbeat_min            = "1"
heartbeat_max            = "3"

host_volume = "logs"
