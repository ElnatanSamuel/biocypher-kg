import sys
import re
import os

def toggle_adapters(file_path, adapters_to_toggle, comment=True):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    with open(file_path, 'w') as file:
        for line in lines:
            # Check if the line contains an adapter name to toggle
            if any(adapter in line for adapter in adapters_to_toggle):
                if comment:
                    # Comment the line if it's not already commented
                    if not line.startswith('#'):
                        line = '# ' + line
                else:
                    # Uncomment the line if it's commented
                    line = line.lstrip('# ')
            file.write(line)

def get_all_adapters(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    adapters = []
    for line in lines:
        match = re.match(r'^\s*([a-zA-Z0-9_]+):', line)
        if match:
            adapters.append(match.group(1))
    return adapters

def main(file_path):
    all_adapters = get_all_adapters(file_path)
    total_adapters = len(all_adapters)

    # Process adapters in chunks of 2
    for i in range(0, total_adapters, 2):
        # Determine which adapters to toggle
        current_adapters = all_adapters[i:i+2]
        print(f"Testing adapters: {current_adapters}")

        # Comment out all other adapters
        toggle_adapters(file_path, all_adapters, comment=True)
        # Uncomment the current adapters
        toggle_adapters(file_path, current_adapters, comment=False)

        # Here you would run your tests
        # For example, you can call your test command here
        # os.system("pytest test/test_nodes.py")  # Uncomment this line to run tests

        # Simulate test success/failure (replace this with actual test result checking)
        test_success = True  # Change this based on actual test results

        if not test_success:
            print(f"Adapters {current_adapters} failed. Stopping further tests.")
            break
        else:
            print(f"Adapters {current_adapters} passed.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python toggle_adapters.py <file_path>")
        sys.exit(1)

    file_path = sys.argv[1]
    main(file_path)