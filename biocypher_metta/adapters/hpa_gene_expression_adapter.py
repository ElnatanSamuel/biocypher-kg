import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from pronto import Ontology
import gzip
from biocypher_metta.adapters import Adapter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HPAGeneExpressionAdapter(Adapter):
    def __init__(self, 
                 rna_file: str,
                 protein_file: str,
                 cell_ontology_path: str,
                 write_properties: bool,
                 add_provenance: bool,
                 min_confidence: float = 0.3,  # Lowered default threshold
                 label: str = "expressed_in"):
        """Initialize the adapter"""
        self.rna_file = rna_file
        self.protein_file = protein_file
        self.cell_ontology_path = cell_ontology_path
        self.write_properties = write_properties
        self.add_provenance = add_provenance
        self.min_confidence = min_confidence
        self.label = label
        
        # Initialize ontology
        logger.info("Loading Cell Ontology...")
        self.onto = Ontology(cell_ontology_path)
        
        # Initialize empty cell type mapping
        self.cell_type_mapping = {}
        
        # Initialize standard mappings
        self._initialize_standard_mappings()
        
        # Load data
        self.rna_data = self._load_rna_data()
        if self.protein_file:
            self.protein_data = self._load_protein_data()
        
        self.source = "Human_Protein_Atlas"
        self.source_url = "https://www.proteinatlas.org/"
        self.version = "23.0"
        
        super().__init__(write_properties=write_properties, add_provenance=add_provenance)

    def _initialize_standard_mappings(self):
        """Initialize standard cell type mappings"""
        standard_terms = {
            'neuropil': 'neuropil',
            'glandular cells': 'glandular epithelial cell',
            'squamous epithelial cells': 'squamous epithelial cell',
            't-cells': 'T cell',
            'b-cells': 'B cell',
            'nk-cells': 'natural killer cell',
            'monocytes': 'monocyte',
            'hepatocytes': 'hepatocyte',
            'cardiomyocytes': 'cardiac muscle cell',
            'neurons': 'neuron',
            'adipocytes': 'adipocyte',
            'fibroblasts': 'fibroblast',
            'endothelial cells': 'endothelial cell',
            'macrophages': 'macrophage',
            'melanocytes': 'melanocyte',
            'myocytes': 'muscle cell',
            'pneumocytes': 'pneumocyte',
            'epithelial cells': 'epithelial cell',
            'smooth muscle cells': 'smooth muscle cell',
            'skeletal muscle cells': 'skeletal muscle cell'
        }
        
        logger.info("Initializing standard cell type mappings...")
        for hpa_term, onto_term in standard_terms.items():
            try:
                # Search for the term in the ontology
                matching_terms = [term for term in self.onto.terms() 
                                if term.name and onto_term.lower() in term.name.lower()]
                
                if matching_terms:
                    term_id = matching_terms[0].id
                    self.cell_type_mapping[hpa_term] = f"CL:{term_id}" if not term_id.startswith('CL:') else term_id
                    logger.debug(f"Mapped {hpa_term} to {self.cell_type_mapping[hpa_term]}")
                else:
                    logger.warning(f"No ontology match found for {hpa_term} ({onto_term})")
                    
            except Exception as e:
                logger.error(f"Error mapping {hpa_term}: {str(e)}")
        
        logger.info(f"Initialized {len(self.cell_type_mapping)} standard mappings")

    def _map_reliability_to_confidence(self, reliability: str) -> float:
        """Map HPA reliability scores to confidence values"""
        reliability_map = {
            'Enhanced': 0.9,
            'Supported': 0.7,
            'Approved': 0.5,
            'Uncertain': 0.3
        }
        return reliability_map.get(reliability, 0.0)

    def _map_expression_to_confidence(self, level: str) -> float:
        """Map expression levels to confidence values"""
        expression_map = {
            'High': 0.9,
            'Medium': 0.7,
            'Low': 0.5,
            'Not detected': 0.1
        }
        return expression_map.get(level, 0.0)

    def _map_to_cell_ontology(self, cell_type: str) -> str:
        """Map HPA cell types to Cell Ontology IDs"""
        try:
            if not cell_type or pd.isna(cell_type):
                return None
                
            cell_type_lower = cell_type.lower().strip()
            
            # Log the cell type being processed
            logger.debug(f"Processing cell type: {cell_type_lower}")
            
            # Check existing mapping
            if cell_type_lower in self.cell_type_mapping:
                logger.debug(f"Found existing mapping for {cell_type}: {self.cell_type_mapping[cell_type_lower]}")
                return self.cell_type_mapping[cell_type_lower]
            
            # Search ontology with different variations
            search_terms = [
                cell_type_lower,
                cell_type_lower.replace(' cells', ''),
                cell_type_lower.replace(' cell', ''),
                f"{cell_type_lower} cell",
                f"{cell_type_lower} cells"
            ]
            
            logger.debug(f"Searching with terms: {search_terms}")
            
            for term in self.onto.terms():
                if not term.name:
                    continue
                term_name = term.name.lower()
                
                if any(search in term_name for search in search_terms):
                    result = f"CL:{term.id}" if not term.id.startswith('CL:') else term.id
                    logger.debug(f"Found ontology match: {term_name} -> {result}")
                    self.cell_type_mapping[cell_type_lower] = result
                    return result
            
            # If no match found, try partial matches
            for term in self.onto.terms():
                if not term.name:
                    continue
                term_name = term.name.lower()
                
                # Try matching parts of the cell type name
                parts = cell_type_lower.split()
                if any(part in term_name for part in parts if len(part) > 3):
                    result = f"CL:{term.id}" if not term.id.startswith('CL:') else term.id
                    logger.debug(f"Found partial match: {term_name} -> {result}")
                    self.cell_type_mapping[cell_type_lower] = result
                    return result
            
            logger.warning(f"No mapping found for cell type: {cell_type}")
            return None
                
        except Exception as e:
            logger.error(f"Error mapping cell type {cell_type}: {str(e)}")
            return None

    def _load_rna_data(self) -> pd.DataFrame:
        """Load and process RNA expression data"""
        try:
            import zipfile
            with zipfile.ZipFile(self.rna_file, 'r') as zip_ref:
                files = zip_ref.namelist()
                logger.info(f"Files in RNA zip archive: {files}")
                
                tsv_files = [f for f in files if f.endswith('.tsv')]
                file_name = tsv_files[0]
                
                with zip_ref.open(file_name) as f:
                    # Read first 100 rows but ensure we get unique genes
                    df = pd.read_csv(f, sep='\t', nrows=500)
                    logger.info(f"Initial data shape: {df.shape}")
                    logger.info(f"Unique genes before processing: {df['Gene'].nunique()}")
                    logger.info(f"Unique cell types before processing: {df['Cell type'].nunique()}")
                    
                    # Calculate confidence score based on nTPM
                    max_ntpm = df['nTPM'].max()
                    min_ntpm = df['nTPM'].min()
                    logger.info(f"nTPM range: {min_ntpm} to {max_ntpm}")
                    
                    # Normalize nTPM values to confidence scores
                    df['confidence_score'] = df['nTPM'] / max_ntpm
                    logger.info(f"Confidence score range: {df['confidence_score'].min()} to {df['confidence_score'].max()}")
                    
                    # Map cell types to ontology
                    logger.info("Starting cell type mapping...")
                    df['cell_type_id'] = df['Cell type'].apply(self._map_to_cell_ontology)
                    logger.info(f"Mapped cell types: {df['cell_type_id'].value_counts().to_dict()}")
                    
                    # Remove rows with no cell type mapping
                    result_df = df[df['cell_type_id'].notna()].copy()
                    
                    logger.info(f"Final data shape: {result_df.shape}")
                    logger.info(f"Unique genes after processing: {result_df['Gene'].nunique()}")
                    logger.info(f"Unique cell types after processing: {result_df['cell_type_id'].nunique()}")
                    logger.info("\nSample of final data:")
                    logger.info(f"{result_df[['Gene', 'Cell type', 'cell_type_id', 'nTPM', 'confidence_score']].head()}")
                    
                    return result_df
                    
        except Exception as e:
            logger.error(f"Error loading RNA data: {str(e)}")
            raise

    def _load_protein_data(self) -> pd.DataFrame:
        """Load and process protein expression data"""
        try:
            import zipfile
            with zipfile.ZipFile(self.protein_file, 'r') as zip_ref:
                files = zip_ref.namelist()
                logger.info(f"Files in protein zip archive: {files}")
                
                tsv_files = [f for f in files if f.endswith('.tsv')]
                file_name = tsv_files[0]
                
                with zip_ref.open(file_name) as f:
                    # Read first 100 rows but ensure we get unique genes
                    df = pd.read_csv(f, sep='\t', nrows=500)
                    logger.info(f"Initial protein data shape: {df.shape}")
                    logger.info(f"Unique genes before processing: {df['Gene'].nunique()}")
                    logger.info(f"Unique cell types before processing: {df['Cell type'].nunique()}")
                    
                    # Map cell types to ontology
                    logger.info("Starting cell type mapping for protein data...")
                    df['cell_type_id'] = df['Cell type'].apply(self._map_to_cell_ontology)
                    
                    # Add confidence scores based on what columns are actually available
                    if 'Reliability' in df.columns:
                        df['confidence_score'] = df['Reliability'].apply(self._map_reliability_to_confidence)
                    elif 'Level' in df.columns:
                        df['confidence_score'] = df['Level'].apply(self._map_expression_to_confidence)
                    else:
                        df['confidence_score'] = 0.5
                        
                    logger.info(f"Confidence score range: {df['confidence_score'].min()} to {df['confidence_score'].max()}")
                    
                    # Remove rows with no cell type mapping
                    result_df = df[df['cell_type_id'].notna()].copy()
                    
                    logger.info(f"Final protein data shape: {result_df.shape}")
                    logger.info(f"Unique genes after processing: {result_df['Gene'].nunique()}")
                    logger.info(f"Unique cell types after processing: {result_df['cell_type_id'].nunique()}")
                    logger.info("\nSample of final protein data:")
                    logger.info(f"{result_df[['Gene', 'Cell type', 'cell_type_id', 'Level' if 'Level' in result_df.columns else 'Reliability', 'confidence_score']].head()}")
                    
                    return result_df
                    
        except Exception as e:
            logger.error(f"Error loading protein data: {str(e)}")
            raise

    def get_edges(self):
        """Generate gene-cell type relationships"""
        try:
            logger.info("Starting edge generation...")
            edge_count = 0
            
            # Process RNA expression relationships
            logger.info(f"Processing RNA data with shape: {self.rna_data.shape}")
            logger.info(f"RNA data columns: {self.rna_data.columns.tolist()}")
            logger.info(f"Confidence scores in RNA data: {self.rna_data['confidence_score'].describe()}")
            logger.info(f"Min confidence threshold: {self.min_confidence}")
            
            for idx, row in self.rna_data.iterrows():
                if pd.isna(row['cell_type_id']):
                    logger.debug(f"Skipping RNA row {idx} - no cell type mapping")
                    continue
                    
                if row['confidence_score'] >= self.min_confidence:
                    _props = {
                        'confidence_score': float(row['confidence_score']),
                        'nTPM': float(row['nTPM']),
                        'evidence_type': 'RNA_expression',
                        'source': self.source,
                        'source_url': self.source_url,
                        'version': self.version
                    }
                    
                    logger.info(f"Generating RNA edge: {row['Gene']} -> {row['cell_type_id']} (conf: {row['confidence_score']:.2f})")
                    edge_count += 1
                    yield row['Gene'], row['cell_type_id'], self.label, _props
                else:
                    logger.debug(f"Skipping RNA row {idx} - confidence too low: {row['confidence_score']:.2f}")
            
            logger.info(f"Generated {edge_count} edges from RNA data")
            
            # Process protein expression relationships if available
            if hasattr(self, 'protein_data'):
                protein_edge_count = 0
                logger.info(f"Processing protein data with shape: {self.protein_data.shape}")
                logger.info(f"Protein data columns: {self.protein_data.columns.tolist()}")
                logger.info(f"Confidence scores in protein data: {self.protein_data['confidence_score'].describe()}")
                
                for idx, row in self.protein_data.iterrows():
                    if pd.isna(row['cell_type_id']):
                        logger.debug(f"Skipping protein row {idx} - no cell type mapping")
                        continue
                        
                    if row['confidence_score'] >= self.min_confidence:
                        _props = {
                            'confidence_score': float(row['confidence_score']),
                            'level': row['Level'] if 'Level' in row else None,
                            'evidence_type': 'protein_expression',
                            'source': self.source,
                            'source_url': self.source_url,
                            'version': self.version
                        }
                        
                        logger.info(f"Generating protein edge: {row['Gene']} -> {row['cell_type_id']} (conf: {row['confidence_score']:.2f})")
                        protein_edge_count += 1
                        yield row['Gene'], row['cell_type_id'], self.label, _props
                    else:
                        logger.debug(f"Skipping protein row {idx} - confidence too low: {row['confidence_score']:.2f}")
                        
                logger.info(f"Generated {protein_edge_count} edges from protein data")
                
        except Exception as e:
            logger.error(f"Error generating edges: {str(e)}")
            logger.error(f"Error details:", exc_info=True)
            raise