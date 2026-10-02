#!/usr/bin/env python3
"""Generate the declared isolated committee, public identities, and deployment inputs."""

import argparse
import copy
import json
from pathlib import Path
import shutil
import subprocess

import yaml


CHAIN_ID = 4476
ROLES = ("primary", "worker-0", "worker-1")


def write_json(path, value):
    with path.open("x") as output:
        json.dump(value, output, allow_nan=False, sort_keys=True, indent=2)
        output.write("\n")


def address(ip, worker=0):
    return f"/ip4/{ip}/udp/{40000 + worker}/quic-v1"


def peer_population():
    ordinary = [{"name": f"ordinary-{index:02}", "seed": 1000 + index,
                 "namespace": f"ordinary-{index:02}", "ip": f"10.147.1.{index + 1}", "nat": False}
                for index in range(48)]
    ordinary += [{"name": f"nat-{index:02}", "seed": 1048 + index,
                  "namespace": f"nat-{index:02}", "ip": f"10.147.2.{index + 1}", "nat": True}
                 for index in range(16)]
    dao = [{"name": f"dao-{index:02}", "seed": 2000 + index,
            "namespace": f"dao-{index:02}", "ip": f"10.147.3.{index + 1}", "nat": False}
           for index in range(8)]
    return ordinary, dao


def validators():
    return [{"name": f"validator-{index + 1:02}", "ip": f"10.147.0.{10 + index}" if index < 2 else f"10.147.4.{index - 1}",
             "namespace": None if index < 2 else f"validator-{index + 1:02}", "hub": index < 2}
            for index in range(4)]


def initialize(root, binary, peer_binary):
    root.mkdir(parents=True, exist_ok=False)
    templates = root / "templates"
    templates.mkdir()
    nodes = validators()
    for index, node in enumerate(nodes):
        directory = templates / node["name"]
        directory.mkdir()
        subprocess.run([str(binary), "--datadir", str(directory), "--bls-passphrase-source", "no-passphrase",
                        "keytool", "generate", "validator", "--address", f"0x{index + 1:040x}", "--workers", "2"], check=True)
        path = directory / "node-info.yaml"
        information = yaml.safe_load(path.read_text())
        information["name"] = node["name"]
        information["p2p_info"]["primary"]["network_address"] = address(node["ip"])
        for worker, record in enumerate(information["p2p_info"]["workers"]):
            record["network_address"] = address(node["ip"], worker + 1)
            record["rpc"] = {"http": f"http://{node['ip']}:8545", "ws": None}
        path.write_text(yaml.safe_dump(information, sort_keys=False))
        node["bls_key"] = information["bls_public_key"]
        node["p2p_info"] = information["p2p_info"]
    ordinary, dao = peer_population()
    for peer in ordinary + dao:
        public = subprocess.run([str(peer_binary), "identity", "--seed", str(peer["seed"])],
                                check=True, capture_output=True, text=True)
        peer.update(json.loads(public.stdout))
    write_json(root / "population.json", {"validators": nodes, "ordinary": ordinary, "dao": dao})


def phase_inputs(root, phase, binary, profile_path):
    population = json.loads((root / "population.json").read_text())
    output = root / phase
    output.mkdir(exist_ok=False)
    nodes = population["validators"]
    shared = output / "ceremony"
    inputs = shared / "genesis" / "validators"
    inputs.mkdir(parents=True)
    for node in nodes:
        shutil.copy(root / "templates" / node["name"] / "node-info.yaml", inputs / f"{node['name']}.yaml")
    subprocess.run([str(binary), "--datadir", str(shared), "--bls-passphrase-source", "no-passphrase", "genesis",
                    "--basefee-address", "0x9999999999999999999999999999999999999999",
                    "--consensus-registry-owner", "0x00000000000000000000000000000000000007a0",
                    "--dev-funded-account", "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266", "--chain-id", str(CHAIN_ID),
                    "--max-header-delay-ms", "500", "--min-header-delay-ms", "250",
                    "--max-batch-delay-ms", "250", "--epoch-duration-in-secs", "20",
                    "--worker-fee-config", "0:0:18446744073709551615",
                    "--worker-fee-config", "1:0:18446744073709551615"], check=True)
    parameters_path = shared / "parameters.yaml"
    parameters = yaml.safe_load(parameters_path.read_text())
    parameters["allow_private_forward_targets"] = True
    parameters_path.write_text(yaml.safe_dump(parameters, sort_keys=False))
    bootstrap = {node["bls_key"]: node["p2p_info"] for node in nodes[:2]}
    for peer in population["dao"]:
        swarms = {swarm["swarm"]: swarm for swarm in peer["swarms"]}
        bootstrap[peer["bls_key"]] = {
            "primary": {"network_address": address(peer["ip"]), "network_key": swarms["primary"]["network_key"], "rpc": None},
            "workers": [{"network_address": address(peer["ip"], worker + 1),
                         "network_key": swarms[f"worker-{worker}"]["network_key"], "rpc": None} for worker in range(2)],
        }
    dao = [peer["bls_key"] for peer in population["dao"]]
    candidate = json.loads(profile_path.read_text())
    candidate.setdefault("libp2p_config", {})["chain_id"] = CHAIN_ID
    candidate.update({"bootstrap_peers": bootstrap, "dao_observers": dao})
    baseline = {"libp2p_config": {"chain_id": CHAIN_ID}, "bootstrap_peers": bootstrap, "dao_observers": dao}
    write_json(output / "candidate-profile.json", candidate)
    write_json(output / "baseline-profile.json", baseline)
    for index, node in enumerate(nodes):
        directory = output / node["name"]
        shutil.copytree(root / "templates" / node["name"], directory)
        shutil.copytree(shared / "genesis", directory / "genesis")
        shutil.copy(parameters_path, directory / "parameters.yaml")
        profile = baseline if phase == "baseline" and node["hub"] else candidate
        write_json(directory / "network-config", profile)
        command = {"argv": [str(binary), "node", "--datadir", str(directory),
                             "--bls-passphrase-source", "no-passphrase", "--http", "--http.addr", "0.0.0.0",
                             "--http.port", "8545", "--ipcdisable", "--node-name", node["name"],
                             "--metrics", "0.0.0.0:9000", "--log.stdout.format", "json"],
                   "environment": {"RUST_LOG": "info,network::capacity=debug"},
                   "log": str(output / f"{node['name']}.jsonl"), "pid_file": str(output / f"{node['name']}.pid")}
        write_json(output / f"{node['name']}-command.json", command)
    peers = output / "peers"
    peers.mkdir()
    for index, peer in enumerate(population["ordinary"] + population["dao"]):
        network = copy.deepcopy(candidate)
        network["dao_observers"] = []
        network["bootstrap_peers"] = {node["bls_key"]: node["p2p_info"] for node in nodes[:2]}
        # Bootstrap grants admission rather than protected retention. Clients allocate their
        # ordinary slots to both hubs; measured hub profiles remain exact and separate.
        network["public_peer_limit"] = len(network["bootstrap_peers"])
        network["peer_config"]["target_num_peers"] = 2
        network["process_budget"].update({"max_established_connections": 12,
                                         "max_inbound_streams": 192,
                                         "max_receive_credit_bytes": 48 * 1024**2})
        network["source_admission"].update({"max_connections": 12, "max_connections_per_address": 12,
                                            "max_connections_per_prefix": 12, "max_sources": 12})
        network["gossip_mesh"] = {"target": 2, "low": 1, "high": 4, "outbound_min": 1}
        write_json(peers / f"{peer['name']}.json", {"seed": peer["seed"], "network": network, "chain_id": CHAIN_ID,
                   "listen": [address(peer["ip"], role) for role in range(3)],
                   "control": f"{peer['ip']}:9500", "required_hubs": [node["bls_key"] for node in nodes[:2]],
                   "target": nodes[index % 2]["bls_key"], "sync_epoch": 0})
    return population


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    initial = commands.add_parser("initialize")
    initial.add_argument("root", type=Path)
    initial.add_argument("binary", type=Path)
    initial.add_argument("peer_binary", type=Path)
    phase = commands.add_parser("phase")
    phase.add_argument("root", type=Path)
    phase.add_argument("phase", choices=("baseline", "candidate"))
    phase.add_argument("binary", type=Path)
    phase.add_argument("profile", type=Path)
    args = parser.parse_args()
    if args.command == "initialize":
        initialize(args.root, args.binary, args.peer_binary)
    else:
        phase_inputs(args.root, args.phase, args.binary, args.profile)


if __name__ == "__main__":
    main()
