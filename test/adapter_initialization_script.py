import sys
import yaml
import importlib

def initialize_adapters(config_path):
    with open(config_path, 'r') as file:
        adapters_config = yaml.safe_load(file)

    for adapter_name, config in adapters_config.items():
        print(f"Initializing adapter: {adapter_name}")

        # Dynamically import the adapter module and class
        adapter_module = importlib.import_module(config['adapter']['module'])
        adapter_class = getattr(adapter_module, config['adapter']['cls'])

        # Prepare the arguments for the adapter
        adapter_args = config['adapter']['args'].copy()

        # Create an instance of the adapter
        adapter_instance = adapter_class(**adapter_args)

        # Optionally, you can perform any initialization tasks here
        # For example, loading data or validating the adapter
        try:
            # Example: Load data if the adapter requires it
            if hasattr(adapter_instance, 'load_data'):
                adapter_instance.load_data()  # Assuming the adapter has a method to load data
                print(f"Data loaded for adapter: {adapter_name}")

            # Example: Validate the adapter's configuration
            if not adapter_instance.validate_config():
                raise ValueError(f"Invalid configuration for adapter: {adapter_name}")

            # Example: Test connectivity if applicable
            if hasattr(adapter_instance, 'test_connection'):
                adapter_instance.test_connection()  # Assuming the adapter has a method to test connection
                print(f"Connection successful for adapter: {adapter_name}")

            print(f"Adapter {adapter_name} initialized successfully.")

        except Exception as e:
            print(f"Error during initialization of adapter {adapter_name}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python adapter_initialization_script.py <adapters_config_path>")
        sys.exit(1)

    # Extract the config path directly from the argument
    config_path = sys.argv[1]
    initialize_adapters(config_path)