{{/* vim: set filetype=mustache: */}}

{{/*
Fullname of configMap/secret that contains environment vaiables
*/}}
{{- define "cogynt.env.fullname" -}}
{{- $root := index . 0 -}}
{{- $postfix := index . 1 -}}
{{- printf "%s-%s-%s" (include "common.fullname" $root) "env" $postfix -}}
{{- end -}}

{{/*
Fullname of configMap/secret that contains files
*/}}
{{- define "cogynt.files.fullname" -}}
{{- $root := index . 0 -}}
{{- $postfix := index . 1 -}}
{{- printf "%s-%s-%s" (include "common.fullname" $root) "files" $postfix -}}
{{- end -}}

{{/*
Environment template block for deployable resources
*/}}
{{- define "cogynt.env" -}}
{{- $root := . -}}
{{- if or .Values.configMaps $root.Values.secrets }}
envFrom:
{{- range $name, $config := $root.Values.configMaps -}}
{{- if $config.enabled }}
{{- if not ( empty $config.env ) }}
- configMapRef:
    name: {{ include "cogynt.env.fullname" (list $root $name) }}
{{- end }}
{{- end }}
{{- end }}
{{- range $name, $secret := $root.Values.secrets -}}
{{- if $secret.enabled }}
{{- if not ( empty $secret.env ) }}
- secretRef:
    name: {{ include "cogynt.env.fullname" (list $root $name) }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- with $root.Values.env }}
env:
{{- if not ( empty $root.Values.envValueFrom) }}
{{ toYaml $root.Values.envValueFrom | indent 2 }}
{{- end }}
{{- range $name, $value := . }}
  - name: {{ $name }}
    value: {{ default "" $value | quote }}
{{- end }}
{{- end }}
{{- end -}}


{{/*
Volumes template block for deployable resources
*/}}
{{- define "cogynt.files.volumes" -}}
{{- $root := . -}}
{{- range $name, $config := $root.Values.configMaps -}}
{{- if $config.enabled }}
{{- if not ( empty $config.files ) }}
- name: config-{{ $name }}-files
  configMap:
    name: {{ include "cogynt.files.fullname" (list $root $name) }}
{{- end }}
{{- end }}
{{- end -}}
{{- range $name, $secret := $root.Values.secrets -}}
{{- if $secret.enabled }}
{{- if not ( empty $secret.files ) }}
- name: secret-{{ $name }}-files
  secret:
    secretName: {{ include "cogynt.files.fullname" (list $root $name) }}
{{- end }}
{{- end }}
{{- end -}}
{{- end -}}

{{/*
VolumeMounts template block for deployable resources
*/}}
{{- define "cogynt.files.volumeMounts" -}}
{{- range $name, $config := .Values.configMaps -}}
{{- if $config.enabled }}
{{- if not ( empty $config.files ) }}
- mountPath: {{ default (printf "/%s" $name) $config.mountPath }}
  name: config-{{ $name }}-files
{{- end }}
{{- end }}
{{- end -}}
{{- range $name, $secret := .Values.secrets -}}
{{- if $secret.enabled }}
{{- if not ( empty $secret.files ) }}
- mountPath: {{ default (printf "/%s" $name) $secret.mountPath }}
  name: secret-{{ $name }}-files
  readOnly: true
{{- end }}
{{- end }}
{{- end -}}
{{- end -}}

{{/*
initContainers template block for deployable resources
*/}}
{{- define "cogynt.initContainers" -}}
{{- $root := . -}}
{{- with $root.Values.initContainers }}
initContainers:
{{- end }}
{{- range $name, $initContainers := .Values.initContainers -}}
{{- if $initContainers.enabled }}
- name: {{ include "common.fullname" $root }}-{{ $name }}
  image: {{ $initContainers.image }}
{{- with $initContainers.command }}  
  command: 
{{ toYaml . | indent 2 }}
{{- end }}
{{- with $initContainers.volumeMounts }}
  volumeMounts:
{{ toYaml . | indent 2 }}
{{- end }}
{{- end }}
{{- end -}}
{{- end -}}
