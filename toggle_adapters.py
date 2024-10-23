import sys
import re
import subprocess

def get_adapter_config(file_path, adapter_name):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    current_adapter = None
    current_block = []

    for line in lines:
        # Check if the line starts with an adapter name
        adapter_name_match = re.match(r'^\s*([a-zA-Z0-9_]+):', line)
        if adapter_name_match:
            if current_adapter == adapter_name:
                # If we are still in the desired adapter block, add the line
                current_block.append(line)
            elif current_adapter:  # If we were tracking another adapter, stop
                break  # Exit if we reach a new adapter
            current_adapter = adapter_name_match.group(1)  # Start a new adapter
            if current_adapter == adapter_name:
                current_block.append(line)  # Start collecting lines for the desired adapter
        elif current_adapter == adapter_name:
            current_block.append(line)  # Add the line to the current block

    if current_adapter == adapter_name:  # Return the block if we found the adapter
        return ''.join(current_block)

    return None  # Return None if the adapter was not found

def test_adapter(adapter_name, adapter_config):
    print(f"Testing adapter: {adapter_name}")

    # Create a temporary config file for the current adapter
    temp_config_path = 'temp_adapter_config.yaml'
    with open(temp_config_path, 'w') as temp_file:
        temp_file.write(adapter_config)

    dbsnp_rsids_path = './aux_files/sample_dbsnp_rsids.pkl'
    dbsnp_pos_path = './aux_files/sample_dbsnp_pos.pkl'

    # Run your tests and capture the result
    result = subprocess.run(
        ["poetry", "run", "pytest", "test/test_nodes.py", 
         "--adapters-config", temp_config_path, 
         "--dbsnp-rsids", dbsnp_rsids_path, 
         "--dbsnp-pos", dbsnp_pos_path],
        capture_output=True,
        text=True
    )

    # Check the result of the test run
    if result.returncode != 0:
        print(f"Adapter {adapter_name} failed.")
        print(result.stdout)  # Print the output for debugging
        print(result.stderr)  # Print the error output for debugging
        return False
    else:
        print(f"Adapter {adapter_name} passed.")
        return True

def main(file_path, adapter_name):
    adapter_config = get_adapter_config(file_path, adapter_name)

    if adapter_config:
        test_adapter(adapter_name, adapter_config)
    else:
        print(f"Adapter {adapter_name} not found in the configuration file.")
        print("Available adapters:")
        # Print all adapters for debugging
        all_adapters = get_all_adapters(file_path)
        print(all_adapters)

def get_all_adapters(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    adapters = []
    current_adapter = None

    for line in lines:
        adapter_name_match = re.match(r'^\s*([a-zA-Z0-9_]+):', line)
        if adapter_name_match:
            adapters.append(adapter_name_match.group(1))

    return adapters

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python toggle_adapters.py <file_path> <adapter_name>")
        sys.exit(1)

    file_path = sys.argv[1]
    adapter_name = sys.argv[2]
    main(file_path, adapter_name)