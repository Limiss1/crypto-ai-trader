"""
AI-powered trading strategy using machine learning models.
Three-class classification: BUY(2) / HOLD(1) / SELL(0)
Enhanced feature engineering with RSI, MACD, Bollinger Bands, ATR, OBV, VWAP.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

from .base import Strategy, Signal, SignalType
from ..data.market_data import MarketData
from ..infra.config import get_config
from ..infra.logger import LogMixin
from ..models.base_model import BaseModel, ModelMetadata

LABEL_BUY = 2
LABEL_HOLD = 1
LABEL_SELL = 0

FIXED_FEATURE_COLUMNS = [
    'returns', 'log_returns',
    'sma_5', 'ema_5', 'sma_10', 'ema_10', 'sma_20', 'ema_20', 'sma_50', 'ema_50',
    'volatility_20', 'volatility_50',
    'volume_sma_20', 'volume_ratio', 'volume_change',
    'range', 'normalized_range',
    'momentum_10', 'momentum_20',
    'rsi_14', 'rsi_7',
    'macd', 'macd_signal', 'macd_hist',
    'bb_upper', 'bb_middle', 'bb_lower', 'bb_width', 'bb_pct',
    'atr_14',
    'obv', 'obv_sma_20',
    'vwap',
    'price_to_sma20', 'price_to_sma50',
    'volume_price_trend',
]


class FeatureEngine:
    """Feature engineering for AI strategy with fixed feature columns."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.feature_columns: List[str] = list(FIXED_FEATURE_COLUMNS)

    def calculate_features(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        if len(result) < 55:
            return result

        result['returns'] = result['close'].pct_change()
        result['log_returns'] = np.log(result['close'] / result['close'].shift(1))

        for window in [5, 10, 20, 50]:
            result[f'sma_{window}'] = result['close'].rolling(window=window).mean()
            result[f'ema_{window}'] = result['close'].ewm(span=window, adjust=False).mean()

        result['volatility_20'] = result['returns'].rolling(window=20).std()
        result['volatility_50'] = result['returns'].rolling(window=50).std()

        if 'volume' in result.columns:
            result['volume_sma_20'] = result['volume'].rolling(window=20).mean()
            result['volume_sma_20'] = result['volume_sma_20'].replace(0, np.nan)
            result['volume_ratio'] = result['volume'] / result['volume_sma_20']
            result['volume_change'] = result['volume'].pct_change()
        else:
            result['volume_sma_20'] = 0.0
            result['volume_ratio'] = 1.0
            result['volume_change'] = 0.0

        result['range'] = (result['high'] - result['low']) / result['close']
        range_mean = result['range'].rolling(20).mean()
        result['normalized_range'] = result['range'] / range_mean.replace(0, np.nan)

        result['momentum_10'] = result['close'] / result['close'].shift(10) - 1
        result['momentum_20'] = result['close'] / result['close'].shift(20) - 1

        result['rsi_14'] = self._calc_rsi(result['close'], 14)
        result['rsi_7'] = self._calc_rsi(result['close'], 7)

        macd_line, macd_signal, macd_hist = self._calc_macd(result['close'])
        result['macd'] = macd_line
        result['macd_signal'] = macd_signal
        result['macd_hist'] = macd_hist

        bb_upper, bb_middle, bb_lower, bb_width, bb_pct = self._calc_bollinger(result['close'], 20, 2)
        result['bb_upper'] = bb_upper
        result['bb_middle'] = bb_middle
        result['bb_lower'] = bb_lower
        result['bb_width'] = bb_width
        result['bb_pct'] = bb_pct

        result['atr_14'] = self._calc_atr(result['high'], result['low'], result['close'], 14)

        if 'volume' in result.columns:
            obv = self._calc_obv(result['close'], result['volume'])
            result['obv'] = obv
            result['obv_sma_20'] = obv.rolling(window=20).mean()
        else:
            result['obv'] = 0.0
            result['obv_sma_20'] = 0.0

        if 'volume' in result.columns:
            result['vwap'] = self._calc_vwap(result['high'], result['low'], result['close'], result['volume'])
        else:
            result['vwap'] = result['close']

        result['price_to_sma20'] = result['close'] / result['sma_20'].replace(0, np.nan) - 1
        result['price_to_sma50'] = result['close'] / result['sma_50'].replace(0, np.nan) - 1

        if 'volume' in result.columns:
            result['volume_price_trend'] = (
                result['volume'] * result['returns']
            ).rolling(window=20).sum()
        else:
            result['volume_price_trend'] = 0.0

        for col in self.feature_columns:
            if col not in result.columns:
                result[col] = 0.0

        result = result.dropna()

        return result

    @staticmethod
    def _calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100.0 - (100.0 / (1.0 + rs))
        rsi = rsi.fillna(50.0)
        return rsi

    @staticmethod
    def _calc_macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
        ema_fast = series.ewm(span=fast, adjust=False).mean()
        ema_slow = series.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        macd_signal = macd_line.ewm(span=signal, adjust=False).mean()
        macd_hist = macd_line - macd_signal
        return macd_line, macd_signal, macd_hist

    @staticmethod
    def _calc_bollinger(series: pd.Series, period: int = 20, num_std: float = 2.0):
        middle = series.rolling(window=period).mean()
        std = series.rolling(window=period).std()
        upper = middle + num_std * std
        lower = middle - num_std * std
        width = (upper - lower) / middle.replace(0, np.nan)
        pct = (series - lower) / (upper - lower).replace(0, np.nan)
        return upper, middle, lower, width.fillna(0), pct.fillna(0.5)

    @staticmethod
    def _calc_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1.0 / period, min_periods=period).mean()
        return atr

    @staticmethod
    def _calc_obv(close: pd.Series, volume: pd.Series) -> pd.Series:
        direction = np.sign(close.diff())
        direction.iloc[0] = 0
        return (direction * volume).cumsum()

    @staticmethod
    def _calc_vwap(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> pd.Series:
        typical_price = (high + low + close) / 3
        cum_tp_vol = (typical_price * volume).cumsum()
        cum_vol = volume.cumsum().replace(0, np.nan)
        return cum_tp_vol / cum_vol

    def prepare_training_data(self, df: pd.DataFrame, hold_threshold: float = 0.0001) -> Tuple[np.ndarray, np.ndarray]:
        """
        Prepare features and labels for three-class training.
        Label: BUY(2) if future_return > hold_threshold,
               SELL(0) if future_return < -hold_threshold,
               HOLD(1) otherwise.

        Features at position i use only data up to and including i.
        Label at position i uses close[i+1] / close[i] - 1.
        This ensures no future data leakage.
        """
        if len(df) < 3:
            return np.array([]), np.array([])

        if not self.feature_columns:
            self.feature_columns = list(FIXED_FEATURE_COLUMNS)

        features = []
        labels = []

        for i in range(len(df) - 1):
            row = df.iloc[i]
            feature_values = []
            valid = True
            for col in self.feature_columns:
                val = row.get(col)
                if val is None or (isinstance(val, float) and np.isnan(val)):
                    valid = False
                    break
                feature_values.append(float(val))

            if not valid:
                continue

            future_return = df['close'].iloc[i + 1] / df['close'].iloc[i] - 1

            if future_return > hold_threshold:
                label = LABEL_BUY
            elif future_return < -hold_threshold:
                label = LABEL_SELL
            else:
                label = LABEL_HOLD

            features.append(feature_values)
            labels.append(label)

        return np.array(features), np.array(labels)

    def get_feature_importance(self, model: Any) -> Dict[str, float]:
        if not hasattr(model, 'feature_importances_'):
            return {}
        if len(self.feature_columns) != len(model.feature_importances_):
            return {}
        return {
            feature: float(importance)
            for feature, importance in zip(self.feature_columns, model.feature_importances_)
        }


class AIModel(BaseModel):
    """AI trading model based on XGBoost with three-class classification."""

    def __init__(self, model_path: Optional[Path] = None):
        super().__init__()

        if model_path is None:
            config = get_config()
            model_path = config.data.cache_dir / "models" / "ai_trading_model.pkl"

        self.model_path = model_path
        self.model: Optional[xgb.XGBClassifier] = None
        self.feature_columns: List[str] = []
        self.accuracy_history: List[float] = []
        self.training_samples: int = 0

    def create_model(self) -> xgb.XGBClassifier:
        return xgb.XGBClassifier(
            n_estimators=200,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            objective='multi:softprob',
            num_class=3,
            eval_metric='mlogloss',
            random_state=42,
            min_child_weight=3,
            gamma=0.1,
            reg_alpha=0.1,
            reg_lambda=1.0,
        )

    def train(self, X: np.ndarray, y: np.ndarray, feature_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        if len(X) < 100:
            self.logger.warning(f"Insufficient training samples: {len(X)}")
            return {'success': False, 'error': 'Insufficient samples'}

        if feature_columns is not None:
            self.feature_columns = feature_columns

        unique_labels = np.unique(y)
        if len(unique_labels) < 2:
            self.logger.warning(f"Only one class in training data: {unique_labels}")
            return {'success': False, 'error': 'Only one class in data'}

        split_idx = int(len(X) * 0.8)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        from sklearn.utils.class_weight import compute_sample_weight
        sample_weights = compute_sample_weight('balanced', y_train)

        self.model = self.create_model()
        self.model.fit(X_train, y_train, sample_weight=sample_weights)

        y_pred = self.model.predict(X_test)

        metrics = {
            'accuracy': accuracy_score(y_test, y_pred),
            'precision': precision_score(y_test, y_pred, average='weighted', zero_division=0),
            'recall': recall_score(y_test, y_pred, average='weighted', zero_division=0),
            'f1': f1_score(y_test, y_pred, average='weighted', zero_division=0),
            'train_size': len(X_train),
            'test_size': len(X_test),
            'class_distribution': {
                int(k): int(v) for k, v in zip(*np.unique(y_train, return_counts=True))
            }
        }

        self.accuracy_history.append(metrics['accuracy'])
        self.training_samples += len(X_train)

        self.save()

        self.logger.info(
            f"Model trained: accuracy={metrics['accuracy']:.2%}, "
            f"f1={metrics['f1']:.2%}, "
            f"samples={len(X_train)}, "
            f"classes={metrics['class_distribution']}"
        )

        return {'success': True, **metrics}

    def predict(self, X: np.ndarray) -> Tuple[int, float]:
        if self.model is None:
            return LABEL_HOLD, 0.0

        try:
            prob = self.model.predict_proba(X.reshape(1, -1))[0]
            prediction = int(np.argmax(prob))
            confidence = float(max(prob))
            return prediction, confidence
        except Exception as e:
            self.log_exception("Prediction failed", e)
            return LABEL_HOLD, 0.0

    def save(self) -> bool:
        if self.model is None:
            return False

        try:
            import pickle

            save_path = self.model_path
            try:
                save_path.parent.mkdir(parents=True, exist_ok=True)
                with open(save_path, 'wb') as f:
                    pickle.dump({
                        'model': self.model,
                        'feature_columns': self.feature_columns,
                        'accuracy_history': self.accuracy_history,
                        'training_samples': self.training_samples,
                        'num_classes': 3,
                    }, f)
            except (PermissionError, OSError):
                alt_path = Path(".") / "models" / "ai_trading_model.pkl"
                alt_path.parent.mkdir(parents=True, exist_ok=True)
                with open(alt_path, 'wb') as f:
                    pickle.dump({
                        'model': self.model,
                        'feature_columns': self.feature_columns,
                        'accuracy_history': self.accuracy_history,
                        'training_samples': self.training_samples,
                        'num_classes': 3,
                    }, f)
                save_path = alt_path
                self.model_path = alt_path

            self.logger.info(f"Model saved to {save_path}")
            return True

        except Exception as e:
            self.log_exception(f"Failed to save model to {self.model_path}", e)
            return False

    def load(self) -> bool:
        try:
            import pickle

            load_path = self.model_path
            if not load_path.exists():
                alt_path = Path(".") / "models" / "ai_trading_model.pkl"
                if alt_path.exists():
                    load_path = alt_path
                    self.model_path = alt_path
                else:
                    return False

            with open(load_path, 'rb') as f:
                data = pickle.load(f)

            self.model = data['model']
            self.feature_columns = data.get('feature_columns', [])
            self.accuracy_history = data.get('accuracy_history', [])
            self.training_samples = data.get('training_samples', 0)

            self.logger.info(f"Model loaded from {load_path}")
            return True

        except Exception as e:
            self.log_exception(f"Failed to load model from {self.model_path}", e)
            return False

    def get_metadata(self) -> ModelMetadata:
        return ModelMetadata(
            model_type="XGBoost-3Class",
            accuracy=self.accuracy_history[-1] if self.accuracy_history else 0.0,
            training_samples=self.training_samples,
            features=self.feature_columns,
            last_trained=datetime.now()
        )


class AIStrategy(Strategy):
    """AI-powered trading strategy with three-class prediction."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)

        self.confidence_threshold = self.config.get(
            'confidence_threshold',
            get_config().strategy.confidence_threshold
        )

        self.feature_engine = FeatureEngine()
        self.ai_model = AIModel()

        if not self.ai_model.load():
            self.logger.info("No pre-trained model found, will train on first run")
        else:
            n_features_in = getattr(self.ai_model.model, 'n_features_in_', None) if self.ai_model.model else None
            fc_count = len(self.ai_model.feature_columns)

            if fc_count == 0 or (n_features_in and fc_count != n_features_in):
                self.logger.warning(
                    f"Model feature_columns mismatch (saved={fc_count}, model={n_features_in}), "
                    f"discarding model for retraining"
                )
                self.ai_model.model = None
                self.ai_model.feature_columns = list(FIXED_FEATURE_COLUMNS)
                self.feature_engine.feature_columns = list(FIXED_FEATURE_COLUMNS)
            else:
                self.feature_engine.feature_columns = self.ai_model.feature_columns[:]

            self.logger.info(
                f"Model status: features={len(self.feature_engine.feature_columns)}, "
                f"loaded={self.ai_model.model is not None}, "
                f"accuracy history: {self.ai_model.accuracy_history}"
            )

    async def analyze(self, data: MarketData, symbol: str) -> Signal:
        try:
            df = await data.get_ohlcv(
                symbol=symbol,
                timeframe='1m',
                limit=max(get_config().strategy.lookback_period, 1500)
            )

            if len(df) < 100:
                return Signal(
                    signal_type=SignalType.HOLD,
                    symbol=symbol,
                    confidence=0.0,
                    price=df.iloc[-1]['close'] if len(df) > 0 else 0.0,
                    timestamp=datetime.now()
                )

            if self.ai_model.model is None:
                await self._train_model(df)

            df_features = self.feature_engine.calculate_features(df)

            if len(df_features) == 0:
                return Signal(
                    signal_type=SignalType.HOLD,
                    symbol=symbol,
                    confidence=0.0,
                    price=df.iloc[-1]['close'],
                    timestamp=datetime.now()
                )

            latest = df_features.iloc[-1]
            current_price = float(latest['close'])

            if not self.feature_engine.feature_columns:
                self.feature_engine.feature_columns = list(FIXED_FEATURE_COLUMNS)

            feature_values = np.array([
                float(latest[col]) if col in latest.index and pd.notna(latest[col]) else 0.0
                for col in self.feature_engine.feature_columns
            ])

            nan_count = np.isnan(feature_values).sum()
            if nan_count > 0:
                feature_values = np.nan_to_num(feature_values, nan=0.0)
                self.logger.debug(f"[{symbol}] {nan_count} NaN features replaced with 0")

            prediction, confidence = self.ai_model.predict(feature_values)

            pred_label = {LABEL_BUY: 'BUY', LABEL_HOLD: 'HOLD', LABEL_SELL: 'SELL'}.get(prediction, 'UNKNOWN')
            self.logger.info(
                f"[SIGNAL] {symbol}: {pred_label} confidence={confidence:.2%} "
                f"price={current_price:.2f} features={len(feature_values)}"
            )

            if prediction == LABEL_BUY:
                signal_type = SignalType.BUY
            elif prediction == LABEL_SELL:
                signal_type = SignalType.SELL
            else:
                signal_type = SignalType.HOLD

            return Signal(
                signal_type=signal_type,
                symbol=symbol,
                confidence=confidence,
                price=current_price,
                timestamp=datetime.now(),
                metadata={
                    'prediction': int(prediction),
                    'prediction_label': pred_label,
                    'feature_count': len(self.feature_engine.feature_columns),
                    'model_accuracy': self.ai_model.accuracy_history[-1] if self.ai_model.accuracy_history else 0.0
                }
            )

        except Exception as e:
            self.log_exception(f"AI analysis failed for {symbol}", e)
            return Signal(
                signal_type=SignalType.HOLD,
                symbol=symbol,
                confidence=0.0,
                price=0.0,
                timestamp=datetime.now()
            )

    async def retrain_model(self, market_data: MarketData, symbols: list) -> None:
        self.logger.info(f"[RETRAIN] Starting model retraining for {len(symbols)} symbols...")

        all_X = []
        all_y = []

        for symbol in symbols:
            try:
                df = await market_data.get_ohlcv(
                    symbol=symbol,
                    timeframe='1m',
                    limit=1000
                )

                if len(df) < 100:
                    self.logger.warning(f"[RETRAIN] Insufficient data for {symbol}: {len(df)} rows")
                    continue

                df_features = self.feature_engine.calculate_features(df)
                X, y = self.feature_engine.prepare_training_data(df_features)

                if len(X) > 0:
                    all_X.append(X)
                    all_y.append(y)
                    self.logger.info(f"[RETRAIN] {symbol}: {len(X)} samples prepared")
            except Exception as e:
                self.logger.error(f"[RETRAIN] Failed to prepare data for {symbol}: {e}")

        if not all_X:
            self.logger.warning("[RETRAIN] No training data available, aborting retrain")
            return

        X_combined = np.vstack(all_X)
        y_combined = np.concatenate(all_y)

        self.logger.info(f"[RETRAIN] Combined dataset: {len(X_combined)} samples, classes: {dict(zip(*np.unique(y_combined, return_counts=True)))}")

        self.ai_model.model = None

        metrics = self.ai_model.train(X_combined, y_combined, self.feature_engine.feature_columns)

        if metrics.get('success'):
            self.logger.info(
                f"[RETRAIN] Model retrained: accuracy={metrics['accuracy']:.2%}, "
                f"precision={metrics.get('precision', 0):.2%}, "
                f"recall={metrics.get('recall', 0):.2%}, "
                f"samples={metrics['train_size']}, "
                f"classes={metrics.get('class_distribution', {})}"
            )
        else:
            self.logger.error(f"[RETRAIN] Model retraining failed: {metrics.get('error')}")

    async def _train_model(self, df: pd.DataFrame) -> None:
        try:
            if len(df) < 200:
                self.logger.warning(f"Not enough data for training: {len(df)} rows (need 200+)")
                return

            df_features = self.feature_engine.calculate_features(df)
            X, y = self.feature_engine.prepare_training_data(df_features)

            if len(X) == 0:
                self.logger.warning("No training data available")
                return

            metrics = self.ai_model.train(X, y, self.feature_engine.feature_columns)

            if metrics.get('success'):
                self.logger.info(
                    f"Model trained successfully: "
                    f"accuracy={metrics['accuracy']:.2%}, "
                    f"f1={metrics.get('f1', 0):.2%}, "
                    f"samples={metrics['train_size']}, "
                    f"classes={metrics.get('class_distribution', {})}"
                )
            else:
                self.logger.error(f"Model training failed: {metrics.get('error')}")

        except Exception as e:
            self.log_exception("Model training failed", e)
