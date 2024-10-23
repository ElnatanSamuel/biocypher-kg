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
        
        chunk_size = 4  # Define the number of adapters to process in each chunk
        adapter_names = list(adapters_config.keys())  # Get all adapter names

        # Process adapters in chunks
        for i in range(0, len(adapter_names), chunk_size):
            chunk = adapter_names[i:i + chunk_size]  # Get the current chunk of adapter names
            logging.info(f"Processing chunk: {chunk}")  # Log the current chunk being processed
            
            for adapter_name in chunk:
                config = adapters_config[adapter_name]
                if config["nodes"]:
                    logging.info(f"Testing adapter: {adapter_name}")  # Log the adapter being tested
                    adapter_module = importlib.import_module(config['adapter']['module'])
                    adapter_class = getattr(adapter_module, config['adapter']['cls'])
                    
                    # Add write_properties and add_provenance to the arguments
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

            logging.info(f"Completed processing chunk: {chunk}")  # Log completion of the chunk

        logging.info("Node tests completed.")  # Log the completion of the test