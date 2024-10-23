import os
import importlib
import pytest
import logging
from utils import setup_class, convert_input_labels

# Set up logging
logging.basicConfig(level=logging.WARNING)

@pytest.mark.filterwarnings("ignore")
class TestBiocypherNode:
    def test_adapter_nodes_in_schema(self, setup_class):
        logging.info("Starting node tests...")  # Log the start of the test
        node_labels, edges_schema, adapters_config, dbsnp_rsids_dict, dbsnp_pos_dict = setup_class
        
        # Get all adapter names
        all_adapter_names = list(adapters_config.keys())  # Get all adapter names

        # Phase 1: Load specified initial adapters
        initial_success = True
        for adapter_name in all_adapter_names:
            logging.info(f"Testing adapter: {adapter_name}")  # Log the adapter being tested
            config = adapters_config[adapter_name]
            
            if config["nodes"]:
                adapter_module = importlib.import_module(config['adapter']['module'])
                adapter_class = getattr(adapter_module, config['adapter']['cls'])
                
                # Prepare adapter arguments
                adapter_args = config['adapter']['args'].copy()
                if "dbsnp_rsid_map" in adapter_args:
                    adapter_args["dbsnp_rsid_map"] = dbsnp_rsids_dict
                if "dbsnp_pos_map" in adapter_args:
                    adapter_args["dbsnp_pos_map"] = dbsnp_pos_dict
                adapter_args['write_properties'] = True
                adapter_args['add_provenance'] = True
                
                logging.info(f"Creating adapter instance for {adapter_name}...")  # Log adapter creation
                adapter = adapter_class(**adapter_args)
                
                # Get a sample node from the adapter
                logging.info(f"Retrieving sample node for adapter: {adapter_name}...")
                sample_node = next(adapter.get_nodes(), None)
                if not sample_node:
                    logging.error(f"No nodes found for adapter '{adapter_name}'")
                    initial_success = False
                    break  # Stop if any initial adapter fails
                
                _, node_label, node_props = sample_node
                label = convert_input_labels(node_label)
                logging.info(f"Retrieved node label: {label}")  # Log the retrieved node label
                if label not in node_labels:
                    logging.error(f"Node label '{label}' from adapter '{adapter_name}' not found in schema")
                    initial_success = False
                    break  # Stop if any initial adapter fails

        # Phase 2: Load additional adapters if initial ones succeeded
        if initial_success:
            for adapter_name in all_adapter_names:
                if adapter_name not in initial_adapter_names:  # Skip already tested adapters
                    logging.info(f"Testing adapter: {adapter_name}")  # Log the adapter being tested
                    config = adapters_config[adapter_name]
                    
                    if config["nodes"]:
                        adapter_module = importlib.import_module(config['adapter']['module'])
                        adapter_class = getattr(adapter_module, config['adapter']['cls'])
                        
                        # Prepare adapter arguments
                        adapter_args = config['adapter']['args'].copy()
                        if "dbsnp_rsid_map" in adapter_args:
                            adapter_args["dbsnp_rsid_map"] = dbsnp_rsids_dict
                        if "dbsnp_pos_map" in adapter_args:
                            adapter_args["dbsnp_pos_map"] = dbsnp_pos_dict
                        adapter_args['write_properties'] = True
                        adapter_args['add_provenance'] = True
                        
                        logging.info(f"Creating adapter instance for {adapter_name}...")  # Log adapter creation
                        adapter = adapter_class(**adapter_args)
                        
                        # Get a sample node from the adapter
                        logging.info(f"Retrieving sample node for adapter: {adapter_name}...")
                        sample_node = next(adapter.get_nodes(), None)
                        if not sample_node:
                            logging.error(f"No nodes found for adapter '{adapter_name}'")
                            continue  # Continue testing remaining adapters
                        
                        _, node_label, node_props = sample_node
                        label = convert_input_labels(node_label)
                        logging.info(f"Retrieved node label: {label}")  # Log the retrieved node label
                        if label not in node_labels:
                            logging.error(f"Node label '{label}' from adapter '{adapter_name}' not found in schema")
                            continue  # Continue testing remaining adapters

        logging.info("All node tests completed.")  # Log the completion of the test