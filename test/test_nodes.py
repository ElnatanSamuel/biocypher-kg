import importlib
import os
import pytest
import logging
from utils import setup_class, convert_input_labels
# Cache for adapter instances
adapter_cache = {}
adapter_last_modified = {}

@pytest.mark.filterwarnings("ignore")
class TestBiocypherNode:
    def test_adapter_nodes_in_schema(self, setup_class):
        logging.info("Starting node tests...")  # Log the start of the test
        node_labels, edges_schema, adapters_config, dbsnp_rsids_dict, dbsnp_pos_dict = setup_class
        
        for adapter_name, config in adapters_config.items():
            if config["nodes"]:
                print(f"Testing adapter: {adapter_name}")  # Log the adapter being tested
                
                # Check if the adapter is already cached
                if adapter_name not in adapter_cache:
                    # Check if the configuration or data files have changed
                    config_file = './config/adapters_config_sample.yaml'  # Path to your config file
                    
                    # Initialize last_modified to None
                    last_modified = None
                    
                    # Check if 'filepath' exists in args
                    if 'args' in config['adapter'] and 'filepath' in config['adapter']['args']:
                        data_file = config['adapter']['args']['filepath']  # Example data file path
                        last_modified = max(os.path.getmtime(config_file), os.path.getmtime(data_file))
                    else:
                        # If no filepath, just use the config file's last modified time
                        last_modified = os.path.getmtime(config_file)

                    # If the adapter has not been initialized or the files have changed, initialize it
                    if adapter_name not in adapter_last_modified or adapter_last_modified[adapter_name] < last_modified:
                        adapter_module = importlib.import_module(config['adapter']['module'])
                        adapter_class = getattr(adapter_module, config['adapter']['cls'])
                        
                        # Prepare the arguments for the adapter
                        adapter_args = config['adapter']['args'].copy()               
                        if "dbsnp_rsid_map" in adapter_args: #this for dbs that use grch37 assembly and to map grch37 to grch38
                             adapter_args["dbsnp_rsid_map"] = dbsnp_rsids_dict
                        if "dbsnp_pos_map" in adapter_args:
                             adapter_args["dbsnp_pos_map"] = dbsnp_pos_dict
                        adapter_args['write_properties'] = True
                        adapter_args['add_provenance'] = True
                        
                        logging.info(f"Creating adapter instance for {adapter_name}...")  # Log adapter creation
                        adapter = adapter_class(**adapter_args)
                        
                        # Cache the adapter instance
                        adapter_cache[adapter_name] = adapter
                        adapter_last_modified[adapter_name] = last_modified
                    else:
                        logging.info(f"Using cached adapter instance for {adapter_name}...")
                        adapter = adapter_cache[adapter_name]
                
                # Get a sample node from the adapter
                sample_node = next(adapter.get_nodes(), None)
                assert sample_node, f"No nodes found for adapter '{adapter_name}'"
                
                _, node_label, node_props = sample_node
                label = convert_input_labels(node_label)
                assert label in node_labels, f"Node label '{label}' from adapter '{adapter_name}' not found in schema"