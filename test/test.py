import pickle
from biocypher import BioCypher
import pytest
import yaml
import importlib
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed


logging.basicConfig(level=logging.INFO)

def convert_input_labels(label, replace_char="_"):
    return label.replace(" ", replace_char)

def parse_schema(bcy):
    schema = bcy._get_ontology_mapping()._extend_schema()
    edges_schema = {}
    node_labels = set()

    for k, v in schema.items():
        if v["represented_as"] == "edge": 
            edge_type = convert_input_labels(k)
            source_type = v.get("source", None)
            target_type = v.get("target", None)
            if source_type is not None and target_type is not None:
                if isinstance(v["input_label"], list):
                    label = convert_input_labels(v["input_label"][0])
                    source_type = convert_input_labels(source_type[0])
                    target_type = convert_input_labels(target_type[0])
                else:
                    label = convert_input_labels(v["input_label"])
                    source_type = convert_input_labels(source_type)
                    target_type = convert_input_labels(target_type)

                output_label = v.get("output_label", None)
                edges_schema[label.lower()] = {"source": source_type.lower(), "target":
                    target_type.lower(), "output_label": output_label.lower() if output_label is not None else None}

        elif v["represented_as"] == "node":
            label = v["input_label"]
            if isinstance(label, list):
                label = label[0]
            label = convert_input_labels(label)
            node_labels.add(label)

    return node_labels, edges_schema
    

@pytest.fixture(scope="session")
def setup_class(request):
    try:
        bcy = BioCypher(
            schema_config_path='config/schema_config.yaml',
            biocypher_config_path='config/biocypher_config.yaml'
        )
        node_labels, edges_schema = parse_schema(bcy) 
    except FileNotFoundError as e:
        pytest.fail(f"Configuration file not found: {e}")
    except yaml.YAMLError as e:
        pytest.fail(f"Error parsing YAML file: {e}")
    except Exception as e:
        pytest.fail(f"Error initializing BioCypher: {e}")
   
    # Load adapters config
    adapters_config_path = request.config.getoption("--adapters-config")
    dbsnp_rsids = request.config.getoption("--dbsnp-rsids")
    dbsnp_pos = request.config.getoption("--dbsnp-pos")
    if dbsnp_rsids:
        logging.info("Loading dbsnp rsids map")
        dbsnp_rsids_dict = pickle.load(open(dbsnp_rsids, 'rb'))
    else:
        logging.warning("--dbsnp-rsids not provided, skipping dbsnp rsids map loading")
        dbsnp_rsids_dict = None
    dbsnp_pos_dict = pickle.load(open(dbsnp_pos, 'rb'))
   
    # Load adapters config
    with open(adapters_config_path, 'r') as f:
        adapters_config = yaml.safe_load(f)

    return node_labels, edges_schema, adapters_config, dbsnp_rsids_dict, dbsnp_pos_dict

@pytest.mark.filterwarnings("ignore")
class TestBiocypherKG:
    def run_adapter_node_test(self, setup_class, adapter_name, config):
        """
        Run the node test for a specific adapter.
        """
        node_labels, edges_schema, adapters_config, dbsnp_rsids_dict, dbsnp_pos_dict = setup_class
        print("this is node")
        
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
        
        adapter = adapter_class(**adapter_args)
        
        # Get a sample node from the adapter
        sample_node = next(adapter.get_nodes(), None)
        assert sample_node, f"No nodes found for adapter '{adapter_name}'"
        
        _, node_label, node_props = sample_node
        label = convert_input_labels(node_label)
        assert label in node_labels, f"Node label '{label}' from adapter '{adapter_name}' not found in schema"

    def run_adapter_edge_test(self, setup_class, adapter_name, config):
        """
        Run the edge test for a specific adapter.
        """
        node_labels, edges_schema, adapters_config, dbsnp_rsids_dict, dbsnp_pos_dict = setup_class
        print("this is edge")
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
        
        adapter = adapter_class(**adapter_args)
        
        # Get a sample edge from the adapter
        sample_edge = next(adapter.get_edges(), None)
        assert sample_edge, f"No edges found for adapter '{adapter_name}'"
                
        _, _, edge_label, edge_props = sample_edge
        assert edge_label.lower() in edges_schema, f"Edge label '{edge_label}' from adapter '{adapter_name}' not found in schema"

    def test_adapters_in_parallel(self, setup_class):
        """
        Run all adapter tests in parallel.
        """
        node_labels, edges_schema, adapters_config, dbsnp_rsids_dict, dbsnp_pos_dict = setup_class
        
        with ThreadPoolExecutor() as executor:
            futures = []
            # Schedule node tests
            for adapter_name, config in adapters_config.items():
                if config["nodes"]:
                    futures.append(executor.submit(self.run_adapter_node_test, setup_class, adapter_name, config))
                if config["edges"]:
                    futures.append(executor.submit(self.run_adapter_edge_test, setup_class, adapter_name, config))
            
            # Wait for all futures to complete
            for future in as_completed(futures):
                try:
                    future.result()  # This will raise any exceptions caught during execution
                except Exception as e:
                    pytest.fail(f"Test failed with exception: {e}")
