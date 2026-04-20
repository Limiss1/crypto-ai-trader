"""
Base model classes for machine learning models.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field

import numpy as np

from ..infra.logger import LogMixin


@dataclass
class ModelMetadata:
    """Model metadata."""
    model_type: str
    accuracy: float = 0.0
    training_samples: int = 0
    features: List[str] = field(default_factory=list)
    last_trained: Optional[datetime] = None
    hyperparameters: Dict[str, Any] = field(default_factory=dict)


class BaseModel(ABC, LogMixin):
    """Abstract base class for machine learning models."""
    
    def __init__(self):
        """Initialize base model."""
        super().__init__()
        self.model: Optional[Any] = None
        self.model_path: Optional[Path] = None
        self.metadata = ModelMetadata(model_type=self.__class__.__name__)
    
    @abstractmethod
    def train(self, X: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
        """
        Train model on data.
        
        Args:
            X: Feature matrix
            y: Labels
        
        Returns:
            Training metrics
        """
        pass
    
    @abstractmethod
    def predict(self, X: np.ndarray) -> Tuple[int, float]:
        """
        Make prediction.
        
        Args:
            X: Feature vector
        
        Returns:
            Tuple of (prediction, confidence)
        """
        pass
    
    @abstractmethod
    def save(self) -> bool:
        """Save model to disk."""
        pass
    
    @abstractmethod
    def load(self) -> bool:
        """Load model from disk."""
        pass
    
    def online_update(self, X: np.ndarray, y: int) -> None:
        """
        Update model with new sample (online learning).
        
        Args:
            X: Feature vector
            y: Label
        
        Note:
            Subclasses should override if they support online learning.
        """
        pass
    
    def evaluate(self, X_test: np.ndarray, y_test: np.ndarray) -> Dict[str, float]:
        """
        Evaluate model on test data.
        
        Args:
            X_test: Test features
            y_test: Test labels
        
        Returns:
            Evaluation metrics
        """
        if self.model is None:
            return {'error': 'Model not trained'}
        
        try:
            y_pred = self.model.predict(X_test)
            
            # Calculate metrics
            from sklearn.metrics import (
                accuracy_score, precision_score, recall_score, 
                f1_score, roc_auc_score, confusion_matrix
            )
            
            metrics = {
                'accuracy': accuracy_score(y_test, y_pred),
                'precision': precision_score(y_test, y_pred, zero_division=0),
                'recall': recall_score(y_test, y_pred, zero_division=0),
                'f1': f1_score(y_test, y_pred, zero_division=0),
            }
            
            # Try ROC-AUC if we have probability predictions
            try:
                y_prob = self.model.predict_proba(X_test)[:, 1]
                metrics['roc_auc'] = roc_auc_score(y_test, y_prob)
            except:
                pass
            
            # Confusion matrix
            cm = confusion_matrix(y_test, y_pred)
            metrics['true_positives'] = int(cm[1, 1])
            metrics['true_negatives'] = int(cm[0, 0])
            metrics['false_positives'] = int(cm[0, 1])
            metrics['false_negatives'] = int(cm[1, 0])
            
            return metrics
            
        except Exception as e:
            self.log_exception("Model evaluation failed", e)
            return {'error': str(e)}
    
    def get_metadata(self) -> ModelMetadata:
        """Get model metadata."""
        return self.metadata
    
    def is_trained(self) -> bool:
        """Check if model is trained."""
        return self.model is not None