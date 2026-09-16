{{- define "nifi-cluster.authClassName" -}}
{{ .Release.Namespace }}-keycloak
{{- end }}

{{- define "nifi-cluster.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}
