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
        
        # Create a cache for adapter instances
        adapter_cache = {}

        for adapter_name, config in adapters_config.items():
            if config["nodes"]:
                logging.info(f"Testing adapter: {adapter_name}")  # Log the adapter being tested
                
                # Check if the adapter is already cached
                if adapter_name not in adapter_cache:
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
                    
                    # Cache the adapter instance
                    adapter_cache[adapter_name] = adapter
                else:
                    logging.info(f"Using cached adapter instance for {adapter_name}...")
                    adapter = adapter_cache[adapter_name]

                # Get a sample node from the adapter
                logging.info(f"Retrieving sample node for adapter: {adapter_name}...")
                sample_node = next(adapter.get_nodes(), None)
                assert sample_node, f"No nodes found for adapter '{adapter_name}'"
                
                _, node_label, node_props = sample_node
                label = convert_input_labels(node_label)
                logging.info(f"Retrieved node label: {label}")  # Log the retrieved node label
                assert label in node_labels, f"Node label '{label}' from adapter '{adapter_name}' not found in schema"

        logging.info("Node tests completed.")  # Log the completion of the test