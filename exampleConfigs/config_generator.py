import yaml
import os
import argparse
import copy

def generate_node_configs(input_file, output_dir):
    # Load the initial YAML
    with open(input_file, "r") as f:
        data = yaml.safe_load(f)

    nodes = data["nodes"]
    common = data["common"]

    # Build a dictionary: domain -> node data
    domain_map = {n["domain"]: n for n in nodes}

    # Identify the cloud node
    cloud_nodes = [n for n in nodes if n.get("is_cloud", False)]
    if not cloud_nodes:
        raise ValueError("No cloud node found in the YAML (is_cloud: true missing).")
    if len(cloud_nodes) > 1:
        raise ValueError("Multiple cloud nodes found; expected exactly one.")
    cloud_node = cloud_nodes[0]

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    for node in nodes:
        node_domain = node["domain"]
        node_name = node["name"]

        # === Father ===
        parts = node_domain.split("/")
        father = None
        if len(parts) > 1:  # has a parent
            father_domain = "/".join(parts[:-1])
            if father_domain in domain_map:
                father_node = domain_map[father_domain]
                father = {
                    "address": f"{father_node['address']}:{common['port']}",
                    "domain": father_node["domain"],
                    "latency_up": node.get("latency_up", 0)
                }

        # === Children ===
        children = []
        for child in nodes:
            if child["domain"].startswith(node_domain + "/"):
                child_parts = child["domain"].split("/")
                if child_parts[:-1] == parts:  # direct child
                    children.append({
                        "address": f"{child['address']}:{common['port']}",
                        "domain": child["domain"],
                        "latency_up": child.get("latency_up", 0)
                    })

        # === Cloud ===
        cloud = {
            "address": f"{cloud_node['address']}:{common['port']}",
            "domain": cloud_node["domain"]
        }

        # === Node Config ===
        node_config = {
            "node": {
                "name": f"{node['address']}:{common['port']}",
                "domain": node_domain,
                "address": node["address"],
                "port": common["port"],
                "secret": common["secret"],
            },
            "hierarchy": {
                "cloud": cloud,
                "children": children
            },
        }

        # Only include father if it exists
        if father is not None:
            node_config["hierarchy"]["father"] = father

        # Include the rest of common, removing keys already used
        common_copy = copy.deepcopy(common)
        for k in ["port", "secret", "migrationManager", "lru", "redis"]:
            common_copy.pop(k, None)
        node_config.update(common_copy)
        # Explicitly include these
        node_config["migrationManager"] = common.get("migrationManager", {})
        node_config["lru"] = common.get("lru", {})
        node_config["redis"] = common.get("redis", {})

        # Save YAML
        output_file = os.path.join(output_dir, f"{node_name}.yaml")
        with open(output_file, "w") as f:
            yaml.dump(node_config, f, sort_keys=False)

        print(f"Generated config for {node_name} -> {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate node configs from a YAML network file.")
    parser.add_argument("input_file", help="Path to the input YAML file")
    parser.add_argument("output_dir", help="Directory where node configs will be saved")
    args = parser.parse_args()

    generate_node_configs(args.input_file, args.output_dir)
