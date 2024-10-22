import importlib
import pytest
from utils import setup_class, convert_input_labels

@pytest.mark.filterwarnings("ignore")
class TestBiocypherEdge:
    def test_adapter_edges_in_schema(self, setup_class):
        """
        What it tests: Similar to the node test, this one ensures that the edge labels produced by the adapters 
        are also part of the defined schema.
        """
        node_labels, edges_schema, adapters_config, dbsnp_rsids_dict, dbsnp_pos_dict = setup_class
        for adapter_name, config in adapters_config.items():
            if config['edges']:
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