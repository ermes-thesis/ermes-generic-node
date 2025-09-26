import yaml
import os
import argparse

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
        father = ["nil", "nil", "0"]
        if len(parts) > 1:  # has a parent
            father_domain = "/".join(parts[:-1])
            if father_domain in domain_map:
                father_node = domain_map[father_domain]
                father = [
                    father_node["address"] + ":" + str(common["port"]), 
                    father_node["domain"], 
                    str(node["latency_up"])
                ]

        # === Children ===
        children = []
        for child in nodes:
            if child["domain"].startswith(node_domain + "/"):
                # child is directly under this node
                child_parts = child["domain"].split("/")
                if child_parts[:-1] == parts:  # direct child
                    children.extend([
                        child["address"] + ":" + str(common["port"]),
                        child["domain"],
                        str(child["latency_up"])
                    ])
        if not children:
            children = ["nil", "nil", "0"]

        # === Cloud ===
        cloud = [cloud_node["address"] + ":" + str(common["port"]), cloud_node["domain"]]

        # === Node Config ===
        node_config = {
            "node": {
                "name": node["address"] + ":" + str(common["port"]),
                "domain": node_domain,
                "address": node["address"],
                "port": common["port"],
                "secret": common["secret"],
            },
            "hierarchy": {
                "father": father,
                "cloud": cloud,
                "children": children,
            },
            "migrationManager": common["migrationManager"],
            "lru": {
                "size": common["lru"]["size"],
                "safeLevel": common["lru"]["safeLevel"],
                },
            "redis": common["redis"],
        }

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
