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
        
        # Read the environment variable to control loading of adapters
        load_all_adapters = os.getenv("LOAD_ALL_ADAPTERS", "true").lower() == "true"
        initial_chunk_size = 4  # Number of adapters to load initially
        adapter_names = list(adapters_config.keys())  # Get all adapter names

        if load_all_adapters:
            # Load all adapters
            logging.info("Loading all adapters...")
            for adapter_name in adapter_names:
                config = adapters_config[adapter_name]
                if config["nodes"]:
                    logging.info(f"Testing adapter: {adapter_name}")  # Log the adapter being tested
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
                    assert sample_node, f"No nodes found for adapter '{adapter_name}'"
                    
                    _, node_label, node_props = sample_node
                    label = convert_input_labels(node_label)
                    logging.info(f"Retrieved node label: {label}")  # Log the retrieved node label
                    assert label in node_labels, f"Node label '{label}' from adapter '{adapter_name}' not found in schema"

        else:
            # Load only the initial chunk
            initial_chunk = [name for name, config in adapters_config.items() if config.get("load_initially", False)]
            logging.info(f"Processing initial chunk: {initial_chunk}")  # Log the initial chunk being processed
            
            for adapter_name in initial_chunk[:initial_chunk_size]:
                config = adapters_config[adapter_name]
                if config["nodes"]:
                    logging.info(f"Testing adapter: {adapter_name}")  # Log the adapter being tested
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
                    assert sample_node, f"No nodes found for adapter '{adapter_name}'"
                    
                    _, node_label, node_props = sample_node
                    label = convert_input_labels(node_label)
                    logging.info(f"Retrieved node label: {label}")  # Log the retrieved node label
                    assert label in node_labels, f"Node label '{label}' from adapter '{adapter_name}' not found in schema"

            logging.info("Initial chunk tests completed.")  # Log completion of the initial chunk

            # Load remaining adapters in chunks
            remaining_adapter_names = [name for name in adapter_names if name not in initial_chunk[:initial_chunk_size]]
            for i in range(0, len(remaining_adapter_names), initial_chunk_size):
                chunk = remaining_adapter_names[i:i + initial_chunk_size]  # Get the next chunk of adapter names
                logging.info(f"Processing remaining chunk: {chunk}")  # Log the current chunk being processed
                
                for adapter_name in chunk:
                    config = adapters_config[adapter_name]
                    if config["nodes"]:
                        logging.info(f"Testing adapter: {adapter_name}")  # Log the adapter being tested
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
                        assert sample_node, f"No nodes found for adapter '{adapter_name}'"
                        
                        _, node_label, node_props = sample_node
                        label = convert_input_labels(node_label)
                        logging.info(f"Retrieved node label: {label}")  # Log the retrieved node label
                        assert label in node_labels, f"Node label '{label}' from adapter '{adapter_name}' not found in schema"

                logging.info(f"Completed processing remaining chunk: {chunk}")  # Log completion of the chunk

        logging.info("All node tests completed.")  # Log the completion of the test