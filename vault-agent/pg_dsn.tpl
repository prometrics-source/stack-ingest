{{- with secret "database/creds/ingestor" -}}
postgresql://{{ .Data.username }}:{{ .Data.password }}@pg-proxy:5432/pmoi?sslmode=disable
{{- end -}}
