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
        
        load_all_adapters = os.getenv("LOAD_ALL_ADAPTERS", "true").lower() == "true"
        initial_chunk_size = 4  # Number of adapters to load initially
        adapter_names = list(adapters_config.keys())  # Get all adapter names

        if load_all_adapters:
            # Load all adapters
            logging.info("Loading all adapters...")
            for adapter_name in adapter_names:
                # (Load and test each adapter as before)
                pass  # Replace with your existing loading logic
        else:
            # Load only the initial chunk
            initial_chunk = adapter_names[:initial_chunk_size]
            logging.info(f"Processing initial chunk: {initial_chunk}")  # Log the initial chunk being processed
            
            for adapter_name in initial_chunk:
                # (Load and test each adapter as before)
                pass  # Replace with your existing loading logic

            logging.info("Initial chunk tests completed.")  # Log completion of the initial chunk

            # Load remaining adapters in chunks
            remaining_adapter_names = adapter_names[initial_chunk_size:]
            for i in range(0, len(remaining_adapter_names), initial_chunk_size):
                chunk = remaining_adapter_names[i:i + initial_chunk_size]  # Get the next chunk of adapter names
                logging.info(f"Processing remaining chunk: {chunk}")  # Log the current chunk being processed
                
                for adapter_name in chunk:
                    # (Load and test each adapter as before)
                    pass  # Replace with your existing loading logic

                logging.info(f"Completed processing remaining chunk: {chunk}")  # Log completion of the chunk

        logging.info("All node tests completed.")  # Log the completion of the test