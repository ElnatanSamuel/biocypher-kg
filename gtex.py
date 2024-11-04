import pandas as pd
from pathlib import Path
import numpy as np
from typing import Dict, List
import logging
import pickle
from pronto import Ontology

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExpressionDataCreator:
    def __init__(self):
        self.cell_type_mapping = self._load_cell_type_mapping()
        
    def _load_cell_type_mapping(self) -> Dict[str, str]:
        """
        Load cell type mappings from Cell Ontology OWL file
        """
        try:
            # Load Cell Ontology
            logger.info("Loading Cell Ontology...")
            onto = Ontology("ontology_dataset_cache/cl.owl")
            
            # Define the tissue-to-cell mappings we want
            tissue_mappings = {
                # Blood and Immune cells
                "Whole Blood": "blood",
                "T cells CD4": "CD4-positive T cell",
                "T cells CD8": "CD8-positive T cell", 
                "B cells": "B cell",
                "NK cells": "natural killer cell",
                "Monocytes": "monocyte",
                
                # Brain tissues
                "Brain Cortex": "cerebral cortex cell",
                "Brain Hippocampus": "hippocampus cell",
                "Brain Cerebellum": "cerebellar cell",
                
                # Other tissues  
                "Liver": "hepatocyte",
                "Lung": "lung cell",
                "Heart": "cardiac cell",
                "Skeletal Muscle": "skeletal muscle cell",
                "Pancreas": "pancreatic cell",
                
                # HCA specific mappings
                "CD4+ T cell": "CD4-positive T cell",
                "CD8+ T cell": "CD8-positive T cell",
                "B cell": "B cell", 
                "NK cell": "natural killer cell",
                "monocyte": "monocyte",
                "hepatocyte": "hepatocyte"
            }
            
            # Create mapping dictionary
            cell_type_mapping = {}
            for tissue_name, cell_term in tissue_mappings.items():
                # Search for the term in ontology
                matching_terms = []
                for term in onto.terms():
                    # Check if term has a name before comparing
                    if term.name and cell_term.lower() in term.name.lower():
                        matching_terms.append(term)
                
                if matching_terms:
                    # Get the CL ID (remove the 'CL:' prefix if present)
                    term_id = matching_terms[0].id
                    if term_id.startswith('CL:'):
                        cell_type_mapping[tissue_name] = term_id
                    else:
                        cell_type_mapping[tissue_name] = f"CL:{term_id}"
                    logger.debug(f"Mapped {tissue_name} to {cell_type_mapping[tissue_name]}")
                else:
                    logger.warning(f"No match found for {tissue_name} ({cell_term})")
                    
            if not cell_type_mapping:
                raise ValueError("No cell types could be mapped from the ontology")
                
            logger.info(f"Successfully loaded {len(cell_type_mapping)} cell type mappings from ontology")
            return cell_type_mapping
                
        except FileNotFoundError:
            logger.error("Cell Ontology file not found in aux_files/cl.owl")
            raise
        except Exception as e:
            logger.error(f"Error loading cell ontology: {str(e)}")
            raise

    def create_expression_data_file(self) -> str:
        """
        Main function to create expression data file
        Returns path to created file
        """
        # 1. Set up output directory
        Path('aux_files').mkdir(exist_ok=True)
        
        # 2. Get data using existing mappings
        logger.info("Creating dataset from existing mappings...")
        expression_data = self.get_expression_data()
        
        if expression_data is None or expression_data.empty:
            raise ValueError("No data was generated")
            
        # 3. Clean and validate data
        logger.info("Cleaning and validating data...")
        expression_data = self.clean_data(expression_data)
        
        # 4. Save to TSV
        output_path = 'aux_files/expression_data.tsv'
        logger.info(f"Saving data to {output_path}...")
        expression_data.to_csv(output_path, sep='\t', index=False)
        
        logger.info(f"Successfully created file at {output_path}")
        return output_path

    def get_expression_data(self) -> pd.DataFrame:
        """
        Creates dataset using gene names, gene IDs and cell type mappings from ontology
        """
        try:
            # Load gene mappings from pickle file
            logger.info("Loading gene mappings...")
            with open("aux_files/hgnc_to_ensembl.pkl", "rb") as f:
                hgnc_to_ensembl = pickle.load(f)
                
            # Load GTEx tissue mappings
            logger.info("Loading GTEx tissue mappings...")
            with open("aux_files/gtex_tissues_to_ontology_map.pkl", "rb") as f:
                tissue_to_cl = pickle.load(f)
                
            # Get unique cell types from tissue mapping
            cell_types = set(tissue_to_cl.values())
            
            logger.info(f"Found {len(hgnc_to_ensembl)} genes and {len(cell_types)} cell types")
            
            # Create records for each gene-cell type pair
            records = []
            for gene_name, ensembl_id in hgnc_to_ensembl.items():
                # For each gene, add relationships to relevant cell types
                for tissue, cell_type in tissue_to_cl.items():
                    record = {
                        'gene_name': gene_name,
                        'gene_id': ensembl_id,
                        'cell_type_id': cell_type
                    }
                    records.append(record)
                        
            df = pd.DataFrame(records)
            logger.info(f"Created dataset with {len(df)} gene-cell type combinations")
            return df
                
        except FileNotFoundError:
            logger.error("Required mapping files not found")
            raise
        except Exception as e:
            logger.error(f"Error creating expression data: {str(e)}")
            raise

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and validate the data
        """
        logger.info("Starting data cleaning...")
        
        # Remove duplicates
        initial_len = len(df)
        df = df.drop_duplicates(subset=['gene_id', 'cell_type_id'])
        logger.info(f"Removed {initial_len - len(df)} duplicate records")
        
        # Ensure proper formatting
        df['gene_name'] = df['gene_name'].str.strip()
        df['gene_id'] = df['gene_id'].str.strip()
        df['cell_type_id'] = df['cell_type_id'].str.strip()
        
        # Remove any rows with missing values
        df = df.dropna(subset=['gene_name', 'gene_id', 'cell_type_id'])
        
        # Sort by gene_name and cell_type_id
        df = df.sort_values(['gene_name', 'cell_type_id'])
        
        logger.info(f"Final dataset contains {len(df)} records")
        return df

def main():
    """
    Main execution function
    """
    try:
        creator = ExpressionDataCreator()
        output_path = creator.create_expression_data_file()
        
        # Verify the output
        df = pd.read_csv(output_path, sep='\t')
        logger.info(f"Successfully created file with {len(df)} records")
        logger.info(f"Number of unique genes: {df['gene_name'].nunique()}")
        logger.info(f"Number of unique gene IDs: {df['gene_id'].nunique()}")
        logger.info(f"Number of unique cell types: {df['cell_type_id'].nunique()}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()