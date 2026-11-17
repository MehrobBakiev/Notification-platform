#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p diagrams/rendered
if [[ -n "${PLANTUML_JAR:-}" ]]; then
  java -Djava.awt.headless=true -jar "$PLANTUML_JAR" -failfast2 -charset UTF-8 -tsvg -o rendered 'diagrams/*.puml'
elif command -v plantuml >/dev/null 2>&1; then
  plantuml -failfast2 -charset UTF-8 -tsvg -o rendered 'diagrams/*.puml'
else
  echo 'Укажите PLANTUML_JAR=/path/to/plantuml.jar или установите plantuml.' >&2
  exit 1
fi
