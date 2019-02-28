#!/usr/bin/env bash
set -Eeux -o pipefail

readonly ROOT="$(CDPATH= cd "$(dirname "$0")/.." && pwd)"
readonly USAGE="Usage: $0 kube-context kube-namespace optional-broker-namespace"
readonly NAMESPACE="${2?Kubernetes namespace is required. ${USAGE}}"
readonly BK_NAMESPACE="${3:-${NAMESPACE}}"

. "${ROOT}/vendor/github.com/LiveRamp/gazette/v2/test/lib.sh"
configure_environment "${1?Kubernetes context is required. ${USAGE}}"

# GAZCTL runs gazctl in an ephemeral docker container which has direct access to
# the kubernetes networking space. The alternative is to run gazctl on the host
# and port-forward broker or consumer pods as required to expose the service (yuck).
readonly GAZCTL="${DOCKER} run \
  --rm \
  --interactive \
  --env BROKER_ADDRESS \
  --env CONSUMER_ADDRESS \
  liveramp/gazette \
  gazctl"

readonly FACTCTL="${DOCKER} run \
  --rm \
  --interactive \
  --env BROKER_ADDRESS \
  --env EXTRACTOR_ADDRESS \
  --env VTABLE_ADDRESS \
  liveramp/factable
  factctl"


# Install a test "gazette-zonemap" ConfigMap in the namespace, if one doesn't already exist.
install_zonemap ${NAMESPACE}

# Install the "factable" chart, first updating dependencies and blocking until release is complete.
${HELM} dependency update ${ROOT}/charts/factable
${HELM} install --namespace ${NAMESPACE} --wait ${ROOT}/charts/factable --values /dev/stdin << EOF
extractor:
  image:
    binary: /go/bin/quotes-extractor

global:
  etcd:
    endpoint: http://$(helm_release ${BK_NAMESPACE} etcd)-etcd.${BK_NAMESPACE}:2379
  gazette:
    endpoint: http://$(helm_release ${BK_NAMESPACE} gazette)-gazette.${BK_NAMESPACE}:80
EOF

# Create all journal fixtures. Use `sed` to replace the MINIO_RELEASE and
# RELEASE_NAME tokens with the correct values.
cat ${ROOT}/test/quotes.journalspace.yaml \
  | sed -e "s/MINIO_RELEASE/$(helm_release ${BK_NAMESPACE} minio)-minio.${BK_NAMESPACE}/g" \
  | sed -e "s/RELEASE_NAME/$(helm_release ${BK_NAMESPACE} factable)/g" \
  | BROKER_ADDRESS=$(release_address $(helm_release ${BK_NAMESPACE} gazette) gazette) ${GAZCTL} journals apply --specs /dev/stdin

# Load the "quotes" example schema.
${DOCKER} run --rm --interactive liveramp/factable quotes-publisher write-schema \
  | EXTRACTOR_ADDRESS=$(release_address $(helm_release ${NAMESPACE} factable) extractor) \
    ${FACTCTL} schema update --instance $(helm_release ${NAMESPACE} factable) --revision 0 --format yaml --path /dev/stdin

cat <<REAL_EOF
# Factable is running. Run the following to update factctl.ini & gazctl.ini
# with local service addresses:
cat > ~/.config/gazette/factctl.ini << EOF
[Broker]
Address = $(release_address $(helm_release ${BK_NAMESPACE} gazette) gazette)

[Extractor]
Address = $(release_address $(helm_release ${NAMESPACE} factable) extractor)

[VTable]
Address = $(release_address $(helm_release ${NAMESPACE} factable) vtable)
EOF

cat > ~/.config/gazette/gazctl.ini << EOF
[journals.Broker]
Address = $(release_address $(helm_release ${BK_NAMESPACE} gazette) gazette)

[shards.Consumer]
Address = $(release_address $(helm_release ${NAMESPACE} factable) extractor)
EOF
REAL_EOF

