#!/bin/bash
# Render Trino catalog properties from templates before starting Trino.
#
# Trino treats ${VAR} in catalog files as a secret-provider reference at node
# startup, so credentials cannot be env-substituted inside the files. Instead
# the templates are mounted read-only and rendered here from container env.
#
# Catalog selection: ICEBERG_CATALOG (nessie | glue). Nessie is the default
# dev catalog (proven path). glue is the M1 spike (see docs/FOCI_VERIFICATION.md
# - Floci's Glue lacks UpdateTable, so Iceberg commits may not persist).

set -euo pipefail

src_dir=/etc/trino/templates
out_dir=/etc/trino/catalog
catalog="${ICEBERG_CATALOG:-nessie}"

case "${catalog}" in
  nessie|glue) ;;
  *) echo "ICEBERG_CATALOG must be 'nessie' or 'glue', got '${catalog}'" >&2; exit 1 ;;
esac

mkdir -p "${out_dir}"

render() {
    local tmpl="$1" out="$2"
    local content ref var val
    content="$(cat "${tmpl}")"
    while IFS= read -r ref; do
        [ -n "${ref}" ] || continue
        # ref is "${NAME}": strip the leading "${" and trailing "}".
        var="${ref:2:${#ref}-3}"
        val="${!var:-}"
        content="${content//${ref}/${val}}"
    done < <(printf '%s\n' "${content}" | grep -oE '\$\{[A-Za-z_][A-Za-z0-9_]*\}' | sort -u)
    printf '%s\n' "${content}" > "${out}"
    echo "Rendered ${out}"
}

render "${src_dir}/iceberg-${catalog}.properties.tmpl" "${out_dir}/iceberg.properties"
echo "iceberg.properties -> catalog '${catalog}'"

exec /usr/lib/trino/bin/run-trino "$@"