#!/usr/bin/env bash
# Telcoin host network collector for the node_exporter textfile collector.
#
# Emits bounded host series in the Prometheus text format: named nft counters
# with the tn_ prefix, the conntrack mode and the UDP receive buffer ceiling.
# A missing tool or kernel feature is reported as an explicit state, never as
# a healthy zero. Runs as root on Linux.
# See docs/src/getting-started/host-network-observability.md.
#
# Usage: tn-host-network.sh                     print the series to stdout
#        TEXTFILE_DIR=/dir tn-host-network.sh   write <dir>/tn_host_network.prom atomically
set -euo pipefail

MAX_RULES=32
NFT_PREFIX='tn_'
CONNTRACK_MAX_FILE=/proc/sys/net/netfilter/nf_conntrack_max
RMEM_MAX_FILE=/proc/sys/net/core/rmem_max

emit_header() {
    printf '# HELP %s %s\n# TYPE %s %s\n' "$1" "$3" "$1" "$2"
}

collect_info() {
    emit_header tn_host_network_collector_info gauge 'Collector version; always 1.'
    printf 'tn_host_network_collector_info{version="1"} 1\n'
}

collect_firewall() {
    emit_header tn_host_firewall_collector_available gauge '1 when nft and jq can list named counters, else 0.'
    local counters
    if ! command -v nft >/dev/null 2>&1 || ! command -v jq >/dev/null 2>&1 \
        || ! counters=$(nft -j list counters 2>/dev/null); then
        printf 'tn_host_firewall_collector_available 0\n'
        return 0
    fi
    printf 'tn_host_firewall_collector_available 1\n'
    local rows total
    # rule (prefix stripped), packets, bytes; a name used in several tables is summed
    rows=$(printf '%s' "$counters" | jq -r --arg p "$NFT_PREFIX" '
        [ .nftables[] | .counter? // empty | select(.name | startswith($p)) ]
        | group_by(.name)
        | map({ rule: (.[0].name | ltrimstr($p)), packets: (map(.packets) | add), bytes: (map(.bytes) | add) })
        | sort_by(.rule) | .[] | [ .rule, .packets, .bytes ] | @tsv')
    total=$(printf '%s\n' "$rows" | awk 'NF { n++ } END { print n + 0 }')
    emit_header tn_host_firewall_rules_truncated gauge "1 when more than $MAX_RULES tn_ counters exist and the list was cut."
    if [ "$total" -gt "$MAX_RULES" ]; then
        printf 'tn_host_firewall_rules_truncated 1\n'
    else
        printf 'tn_host_firewall_rules_truncated 0\n'
    fi
    emit_header tn_host_firewall_rule_packets_total counter 'Packets counted by the named nft counter (tn_ prefix stripped).'
    printf '%s\n' "$rows" | awk -v max="$MAX_RULES" -F '\t' \
        'NF && NR <= max { printf "tn_host_firewall_rule_packets_total{rule=\"%s\"} %s\n", $1, $2 }'
    emit_header tn_host_firewall_rule_bytes_total counter 'Bytes counted by the named nft counter (tn_ prefix stripped).'
    printf '%s\n' "$rows" | awk -v max="$MAX_RULES" -F '\t' \
        'NF && NR <= max { printf "tn_host_firewall_rule_bytes_total{rule=\"%s\"} %s\n", $1, $3 }'
}

collect_conntrack() {
    local mode=tracked tracked=0 notrack=0 unsupported=0
    if [ ! -r "$CONNTRACK_MAX_FILE" ]; then
        mode=unsupported
    elif command -v nft >/dev/null 2>&1 \
        && nft list ruleset 2>/dev/null | awk '/notrack/ { found = 1 } END { exit found ? 0 : 1 }'; then
        mode=notrack
    fi
    case "$mode" in
        tracked) tracked=1 ;;
        notrack) notrack=1 ;;
        unsupported) unsupported=1 ;;
    esac
    emit_header tn_host_conntrack_mode gauge \
        'One-hot conntrack mode: tracked, notrack (the ruleset has a notrack statement) or unsupported (no nf_conntrack_max).'
    printf 'tn_host_conntrack_mode{mode="%s"} %s\n' \
        tracked "$tracked" notrack "$notrack" unsupported "$unsupported"
}

collect_udp() {
    emit_header tn_host_udp_rmem_max_bytes gauge 'net.core.rmem_max: the largest receive buffer a UDP socket may request.'
    if [ -r "$RMEM_MAX_FILE" ]; then
        printf 'tn_host_udp_rmem_max_bytes %s\n' "$(cat "$RMEM_MAX_FILE")"
    fi
}

collect_all() {
    collect_info
    collect_firewall
    collect_conntrack
    collect_udp
}

main() {
    local dir="${TEXTFILE_DIR:-}"
    if [ -z "$dir" ]; then
        collect_all
        return 0
    fi
    local target="$dir/tn_host_network.prom" tmp="$dir/tn_host_network.prom.$$"
    collect_all > "$tmp"
    mv -f "$tmp" "$target"
}

main "$@"
