{{- with secret "database/creds/ingestor" -}}
postgresql://{{ .Data.username }}:{{ .Data.password }}@dbserver:5432/mydb?sslmode=disable
{{- end -}}
