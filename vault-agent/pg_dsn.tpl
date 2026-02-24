{{- with secret "database/creds/ingestor" -}}
postgresql://{{ .Data.username }}:{{ .Data.password }}@wireguard:6432/pmoi?sslmode=disable
{{- end -}}
