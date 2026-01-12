exit_after_auth = false
pid_file = "/tmp/vault-agent.pid"

vault {
  address = "http://vault:8200"
}

auto_auth {
  method "approle" {
    config = {
      role_id_file_path   = "/vault/approle/role_id"
      secret_id_file_path = "/vault/approle/secret_id"
      remove_secret_id_file_after_reading = true
    }
  }
  sink "file" {
    config = { path = "/vault/token" }
  }
}

template {
  source      = "/vault/templates/pg_dsn.tpl"
  destination = "/secrets/pg_dsn"
  perms       = "0640"
  command     = ["/bin/sh", "-c", "docker restart ingest-raw-writer-1"]
}

