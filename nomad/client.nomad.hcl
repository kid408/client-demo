job "client-demo" {
  region      = var.region
  datacenters = var.datacenters
  namespace   = var.namespace
  type        = "service"

  group "client-demo" {
    count = var.count

    update {
      max_parallel      = 1
      health_check      = "checks"
      min_healthy_time  = "10s"
      healthy_deadline  = "5m"
      progress_deadline = "10m"
      auto_revert       = true
      canary            = 0
      stagger           = "30s"
    }

    migrate {
      max_parallel     = 1
      health_check     = "checks"
      min_healthy_time = "10s"
      healthy_deadline = "5m"
    }

    shutdown_delay = "20s"

    volume "logs" {
      type   = "host"
      source = var.host_volume
    }

    network {
      port "http" {}
      port "metrics" {}
    }

    service {
      name         = "client-demo-http"
      tags         = var.discovery_service_tags
      port         = "http"
      address_mode = "host"
      check {
        name     = "client-demo HTTP Check"
        type     = "http"
        path     = "/healthz"
        interval = "3s"
        timeout  = "1s"
      }
    }

    service {
      name         = "client-demo-prom"
      tags         = concat(["prometheus"], var.consul_service_tags)
      port         = "metrics"
      address_mode = "host"
      check {
        name     = "client-demo Metrics Check"
        type     = "http"
        path     = "/metrics"
        interval = "3s"
        timeout  = "1s"
      }
    }

    task "client-demo" {
      driver = "docker"
      user   = "root"
      kill_signal  = "SIGTERM"
      kill_timeout = "30s"

      volume_mount {
        volume      = "logs"
        destination = "/app/logs"
      }

      config {
        image        = var.image
        network_mode = "host"
        force_pull   = false
      }

      env {
        TZ                            = "Asia/Shanghai"
        SERVICE_NAME                  = "client-demo"
        TARGET_DISCOVERY_SERVICE_NAME = "gateway-grpc"
        APP_PORT                      = "${NOMAD_PORT_http}"
        METRICS_PORT                  = "${NOMAD_PORT_metrics}"
        INSTANCE_ID                   = "${NOMAD_ALLOC_ID}"
        CONSUL_HTTP_ADDR              = var.consul_http_addr
        APP_LOG_PATH                  = "/app/logs/client-demo/${NOMAD_ALLOC_ID}.log"
        GATEWAY_ADDR                  = var.gateway_addr
        PEER_REFRESH_INTERVAL_MS      = var.peer_refresh_interval_ms
        RECONNECT_DELAY_MS            = var.reconnect_delay_ms
        HEARTBEAT_INTERVAL_MS         = var.heartbeat_interval_ms
        SESSION_GAP_MS                = var.session_gap_ms
        VIRTUAL_CLIENTS               = var.virtual_clients
        HEARTBEAT_MIN                 = var.heartbeat_min
        HEARTBEAT_MAX                 = var.heartbeat_max
      }

      resources {
        cpu    = var.cpu
        memory = var.memory
      }
    }
  }
}

variable "region" {
  type = string
}

variable "datacenters" {
  type = list(string)
}

variable "namespace" {
  type    = string
  default = "default"
}

variable "image" {
  type = string
}

variable "consul_http_addr" {
  type    = string
  default = "http://127.0.0.1:8500"
}

variable "consul_service_tags" {
  type    = list(string)
  default = []
}

variable "discovery_service_tags" {
  type    = list(string)
  default = []
}

variable "count" {
  type    = number
  default = 1
}

variable "cpu" {
  type    = number
  default = 100
}

variable "memory" {
  type    = number
  default = 128
}

variable "gateway_addr" {
  type    = string
  default = ""
}

variable "peer_refresh_interval_ms" {
  type    = string
  default = "5000"
}

variable "reconnect_delay_ms" {
  type    = string
  default = "3000"
}

variable "heartbeat_interval_ms" {
  type    = string
  default = "2000"
}

variable "session_gap_ms" {
  type    = string
  default = "3000"
}

variable "virtual_clients" {
  type    = string
  default = "5"
}

variable "heartbeat_min" {
  type    = string
  default = "1"
}

variable "heartbeat_max" {
  type    = string
  default = "3"
}

variable "host_volume" {
  type    = string
  default = "logs"
}
