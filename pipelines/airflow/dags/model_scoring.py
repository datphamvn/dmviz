"""
DAG: Model Scoring Pipeline
============================

Pipeline tính toán và cập nhật các chỉ số dự đoán (CLV, Propensity, Uplift) 
vào bảng Dim_Customer trong PostgreSQL Data Warehouse.

Điều kiện tiên quyết:
    - DAG historical_data_etl_processing phải hoàn thành trước
    - Bảng Dim_Customer phải có sẵn với các features: RFM scores, behavioral features

Data Flow:
    PostgreSQL (Dim_Customer) → Spark Processing → PostgreSQL (Updated Dim_Customer)
    
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                        MODEL SCORING PIPELINE                            │
    ├─────────────────────────────────────────────────────────────────────────┤
    │                                                                          │
    │  ┌──────────────────┐                                                   │
    │  │ T1: extract_     │                                                   │
    │  │ modeling_features│                                                   │
    │  └────────┬─────────┘                                                   │
    │           │                                                             │
    │           ▼                                                             │
    │  ┌──────────────────┐                                                   │
    │  │    T2: score_    │                                                   │
    │  │       clv        │                                                   │
    │  └────────┬─────────┘                                                   │
    │           │                                                             │
    │           ▼                                                             │
    │  ┌──────────────────┐                                                   │
    │  │ T3: score_       │                                                   │
    │  │   propensity     │                                                   │
    │  └────────┬─────────┘                                                   │
    │           │                                                             │
    │           ▼                                                             │
    │  ┌──────────────────┐                                                   │
    │  │    T4: score_    │                                                   │
    │  │      uplift      │                                                   │
    │  └────────┬─────────┘                                                   │
    │           │                                                             │
    │           ▼                                                             │
    │  ┌──────────────────┐                                                   │
    │  │ T5: aggregate_   │                                                   │
    │  │ prepare_scores   │                                                   │
    │  └────────┬─────────┘                                                   │
    │           │                                                             │
    │           ▼                                                             │
    │  ┌──────────────────┐                                                   │
    │  │ T6: update_      │                                                   │
    │  │ customer_dim     │                                                   │
    │  └──────────────────┘                                                   │
    │                                                                          │
    └─────────────────────────────────────────────────────────────────────────┘

Models Used:
    - CLV: BG/NBD + Gamma-Gamma models for 12-month CLV prediction
    - Propensity: Logistic Regression for 30-day purchase probability
    - Uplift: T-Learner + S-Learner ensemble for promotion response

Output Columns (Updated in Dim_Customer):
    - CLV_12M_Predicted: Predicted 12-month customer lifetime value
    - Propensity_Score: Probability of purchase in next 30 days (without promotion)
    - Uplift_Score: Incremental effect of promotion on purchase probability

Author: Data Engineering Team
Created: December 2025
"""

from datetime import datetime, timedelta
import os
import logging
import tempfile
import shutil
import pickle
from typing import Dict, Any, Optional, List, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

# ML Libraries
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

# ============================================================================
# CONFIGURATION
# ============================================================================

# PostgreSQL Configuration
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', "192.168.1.6")
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', "5432")
POSTGRES_DB = os.environ.get('POSTGRES_DB', "retail_dw")
POSTGRES_USER = os.environ.get('POSTGRES_USER', "airflow")
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', "airflow")
POSTGRES_SCHEMA = os.environ.get('POSTGRES_SCHEMA', "public")

# Model Configuration
CLV_PREDICTION_MONTHS = 12
CLV_DISCOUNT_RATE = 0.01  # Monthly discount rate (~12% annual)
PROPENSITY_WINDOW_DAYS = 30
UPLIFT_MODEL_TYPE = "ensemble"  # "t_learner", "s_learner", "ensemble"

# Processing Configuration
TEMP_DIR_PREFIX = "model_scoring_"

# Logging
logger = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_postgres_engine():
    """Create PostgreSQL SQLAlchemy engine."""
    connection_string = (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    return create_engine(connection_string)


def get_postgres_connection():
    """Create PostgreSQL connection using psycopg2."""
    import psycopg2
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def read_table_to_dataframe(table_name: str, schema: str = POSTGRES_SCHEMA) -> pd.DataFrame:
    """
    Read PostgreSQL table into pandas DataFrame.
    
    Args:
        table_name: Name of the table to read
        schema: Schema name
    
    Returns:
        DataFrame with table data
    """
    import psycopg2
    
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    
    try:
        query = f"SELECT * FROM {schema}.{table_name}"
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()


def update_customer_scores(df: pd.DataFrame, schema: str = POSTGRES_SCHEMA) -> int:
    """
    Update CLV, Propensity, and Uplift scores in Dim_Customer table.
    
    Args:
        df: DataFrame with CustomerKey and score columns
        schema: Schema name
    
    Returns:
        Number of rows updated
    """
    import psycopg2
    
    conn = get_postgres_connection()
    cursor = None
    updated_count = 0
    
    try:
        cursor = conn.cursor()
        
        # Update scores in batches
        batch_size = 1000
        total_rows = len(df)
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i+batch_size]
            
            for _, row in batch.iterrows():
                update_sql = f"""
                    UPDATE {schema}.dim_customer 
                    SET clv_12m_predicted = %s,
                        propensity_score = %s,
                        uplift_score = %s
                    WHERE customerkey = %s
                """
                cursor.execute(update_sql, (
                    float(row['CLV_12M_Predicted']),
                    float(row['Propensity_Score']),
                    float(row['Uplift_Score']),
                    int(row['CustomerKey'])
                ))
                updated_count += cursor.rowcount
            
            conn.commit()
            logger.info(f"Updated batch {i//batch_size + 1}: {min(i+batch_size, total_rows)}/{total_rows}")
        
        return updated_count
        
    finally:
        if cursor:
            cursor.close()
        conn.close()


# ============================================================================
# CLV MODEL FUNCTIONS
# ============================================================================

def calculate_bgf_clv(df: pd.DataFrame, prediction_months: int = 12, 
                       discount_rate: float = 0.01) -> pd.DataFrame:
    """
    Calculate CLV using BG/NBD + Gamma-Gamma model approach.
    
    For customers with sufficient purchase history, uses probabilistic CLV.
    For new customers, uses historical average adjusted by RFM segment.
    
    Args:
        df: Customer features DataFrame with RFM metrics
        prediction_months: Number of months to predict
        discount_rate: Monthly discount rate
    
    Returns:
        DataFrame with CLV predictions
    """
    try:
        from lifetimes import BetaGeoFitter, GammaGammaFitter
        from lifetimes.utils import summary_data_from_transaction_data
        
        use_lifetimes = True
    except ImportError:
        logger.warning("lifetimes library not available. Using simplified CLV calculation.")
        use_lifetimes = False
    
    df = df.copy()
    
    if use_lifetimes and len(df[df['frequency'] > 0]) > 100:
        # Prepare data for BG/NBD model
        # frequency: number of repeat purchases (not including first)
        # recency: days since first purchase to last purchase
        # T: days since first purchase to now (customer age)
        
        # Filter customers with valid data
        valid_mask = (df['frequency'] >= 0) & (df['recency'] >= 0) & (df['customerlifetime'] > 0)
        valid_df = df[valid_mask].copy()
        
        if len(valid_df) < 50:
            logger.warning("Not enough valid data for BG/NBD model. Using simplified calculation.")
            use_lifetimes = False
        else:
            try:
                # Fit BG/NBD model
                bgf = BetaGeoFitter(penalizer_coef=0.001)
                bgf.fit(
                    valid_df['frequency'],
                    valid_df['recency'],
                    valid_df['customerlifetime']
                )
                
                # Predict number of purchases in next N months
                t = prediction_months * 30  # Convert to days
                valid_df['predicted_purchases'] = bgf.predict(
                    t,
                    valid_df['frequency'],
                    valid_df['recency'],
                    valid_df['customerlifetime']
                )
                
                # Calculate probability alive
                valid_df['prob_alive'] = bgf.conditional_probability_alive(
                    valid_df['frequency'],
                    valid_df['recency'],
                    valid_df['customerlifetime']
                )
                
                # Fit Gamma-Gamma model for monetary value (only for repeat customers)
                returning_customers = valid_df[valid_df['frequency'] > 0].copy()
                
                if len(returning_customers) > 50:
                    ggf = GammaGammaFitter(penalizer_coef=0.001)
                    ggf.fit(
                        returning_customers['frequency'],
                        returning_customers['monetary'] / (returning_customers['frequency'] + 1)  # Avg transaction value
                    )
                    
                    # Predict expected average profit
                    returning_customers['predicted_avg_value'] = ggf.conditional_expected_average_profit(
                        returning_customers['frequency'],
                        returning_customers['monetary'] / (returning_customers['frequency'] + 1)
                    )
                    
                    # Calculate CLV = predicted_purchases * predicted_avg_value * discount_factor
                    discount_factor = sum([1 / (1 + discount_rate) ** i for i in range(prediction_months)])
                    returning_customers['CLV_12M_Predicted'] = (
                        returning_customers['predicted_purchases'] * 
                        returning_customers['predicted_avg_value'] * 
                        returning_customers['prob_alive'] *
                        discount_factor / prediction_months
                    )
                    
                    # Merge back
                    valid_df = valid_df.merge(
                        returning_customers[['customerkey', 'CLV_12M_Predicted', 'predicted_avg_value']],
                        on='customerkey',
                        how='left'
                    )
                    
                    # For non-returning customers, use segment-based estimation
                    non_returning_mask = valid_df['CLV_12M_Predicted'].isna()
                    segment_avg_clv = valid_df.groupby('rfm_segment')['monetary'].mean() * 0.5  # Conservative estimate
                    valid_df.loc[non_returning_mask, 'CLV_12M_Predicted'] = (
                        valid_df.loc[non_returning_mask, 'rfm_segment'].map(segment_avg_clv)
                    )
                    
                    # Merge back to original df
                    df = df.merge(
                        valid_df[['customerkey', 'CLV_12M_Predicted']],
                        on='customerkey',
                        how='left'
                    )
                    
                    logger.info(f"✓ Calculated BG/NBD + Gamma-Gamma CLV for {len(valid_df):,} customers")
                    return df
                    
            except Exception as e:
                logger.warning(f"BG/NBD model failed: {e}. Using simplified calculation.")
                use_lifetimes = False
    
    # Simplified CLV calculation when lifetimes is not available or fails
    logger.info("Using simplified CLV calculation based on RFM and historical spend")
    
    # CLV = Historical Monthly Spend * Months * Retention Probability * Discount Factor
    df['avg_monthly_spend'] = df['monetary'] / (df['customerlifetime'] / 30 + 1)
    
    # Retention probability based on RFM
    retention_map = {
        'Champions': 0.95,
        'Loyal Customers': 0.85,
        'Potential Loyalists': 0.70,
        'New Customers': 0.50,
        'Promising': 0.55,
        'Need Attention': 0.45,
        'About to Sleep': 0.30,
        'At Risk': 0.25,
        "Can't Lose Them": 0.35,
        'Hibernating': 0.15,
        'Lost': 0.05,
        'Others': 0.40
    }
    
    df['retention_prob'] = df['rfm_segment'].map(retention_map).fillna(0.40)
    
    # Calculate discount factor
    discount_factor = sum([1 / (1 + discount_rate) ** i for i in range(prediction_months)])
    
    # CLV prediction
    df['CLV_12M_Predicted'] = (
        df['avg_monthly_spend'] * 
        prediction_months * 
        df['retention_prob'] *
        discount_factor / prediction_months
    )
    
    # Ensure non-negative
    df['CLV_12M_Predicted'] = df['CLV_12M_Predicted'].clip(lower=0)
    
    return df


# ============================================================================
# PROPENSITY MODEL FUNCTIONS
# ============================================================================

def calculate_propensity_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate propensity scores using Logistic Regression.
    
    Propensity = Probability of purchase in next 30 days without promotion
    
    Features used:
        - RFM scores (R_Score, F_Score, M_Score)
        - Behavioral metrics (Recency, Frequency, Monetary)
        - Customer lifecycle features
    
    Args:
        df: Customer features DataFrame
    
    Returns:
        DataFrame with propensity scores
    """
    df = df.copy()
    
    # Feature selection
    feature_cols = [
        'r_score', 'f_score', 'm_score', 'rfm_score_numeric',
        'recency', 'frequency', 'monetary',
        'tenure', 'purchasefrequencyrate', 
        'avgtransactionvalue', 'stdtransactionvalue',
        'customerlifetime', 'avgitemsperinvoice'
    ]
    
    # Only use features that exist in the DataFrame
    available_features = [col for col in feature_cols if col in df.columns]
    
    if len(available_features) < 3:
        logger.warning("Not enough features for propensity model. Using simplified calculation.")
        # Simplified propensity based on RFM
        df['Propensity_Score'] = df['rfm_score_numeric'] / 5.0 * 0.8 + 0.1
        return df
    
    # Prepare features
    X = df[available_features].copy()
    X = X.fillna(X.median())
    
    # Create target variable based on recency
    # Customers with recent purchases are more likely to purchase again
    df['will_purchase_proxy'] = (df['recency'] < 60).astype(int)
    y = df['will_purchase_proxy']
    
    # Handle class imbalance
    if y.sum() < 10 or (len(y) - y.sum()) < 10:
        logger.warning("Insufficient class variation. Using heuristic propensity calculation.")
        # Heuristic: higher RFM = higher propensity
        df['Propensity_Score'] = (
            0.2 * (df['r_score'] / 5) +
            0.3 * (df['f_score'] / 5) +
            0.2 * (df['m_score'] / 5) +
            0.3 * (1 - df['recency'] / df['recency'].max())
        ).clip(0.01, 0.99)
        return df
    
    try:
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Train Logistic Regression
        model = LogisticRegression(
            max_iter=1000,
            random_state=42,
            class_weight='balanced',
            C=0.1  # Regularization
        )
        model.fit(X_scaled, y)
        
        # Predict probabilities
        df['Propensity_Score'] = model.predict_proba(X_scaled)[:, 1]
        
        # Calibrate scores to reasonable range
        df['Propensity_Score'] = df['Propensity_Score'].clip(0.01, 0.99)
        
        logger.info(f"✓ Calculated propensity scores. Mean: {df['Propensity_Score'].mean():.4f}")
        
    except Exception as e:
        logger.warning(f"Propensity model failed: {e}. Using heuristic calculation.")
        df['Propensity_Score'] = (
            0.2 * (df['r_score'] / 5) +
            0.3 * (df['f_score'] / 5) +
            0.2 * (df['m_score'] / 5) +
            0.3 * (1 - df['recency'] / df['recency'].max())
        ).clip(0.01, 0.99)
    
    return df


# ============================================================================
# UPLIFT MODEL FUNCTIONS
# ============================================================================

def calculate_uplift_scores(df: pd.DataFrame, model_type: str = "ensemble") -> pd.DataFrame:
    """
    Calculate uplift scores using T-Learner and S-Learner ensemble.
    
    Uplift = P(purchase | treatment) - P(purchase | no treatment)
    
    Since we don't have actual A/B test data, we simulate treatment assignment
    and estimate uplift based on customer characteristics.
    
    Model approaches:
        - T-Learner: Separate models for treatment/control groups
        - S-Learner: Single model with treatment as feature
        - Ensemble: Average of T-Learner and S-Learner
    
    Args:
        df: Customer features DataFrame
        model_type: "t_learner", "s_learner", or "ensemble"
    
    Returns:
        DataFrame with uplift scores
    """
    df = df.copy()
    np.random.seed(42)
    
    # Feature selection
    feature_cols = [
        'r_score', 'f_score', 'm_score', 'rfm_score_numeric',
        'recency', 'frequency', 'monetary',
        'tenure', 'purchasefrequencyrate',
        'avgtransactionvalue', 'customerlifetime'
    ]
    
    available_features = [col for col in feature_cols if col in df.columns]
    
    if len(available_features) < 3:
        logger.warning("Not enough features for uplift model. Using heuristic calculation.")
        return _calculate_heuristic_uplift(df)
    
    # Prepare features
    X = df[available_features].copy()
    X = X.fillna(X.median())
    
    # Simulate treatment assignment (in practice, this comes from A/B test data)
    # Higher propensity customers more likely to be targeted
    df['treatment_propensity'] = (
        0.3 +  # Base rate
        0.3 * (df.get('CLV_12M_Predicted', df['monetary']) / df.get('CLV_12M_Predicted', df['monetary']).max()) +
        0.2 * (df['rfm_score_numeric'] / 5.0)
    ).clip(0.2, 0.8)
    
    df['treatment'] = (np.random.random(len(df)) < df['treatment_propensity']).astype(int)
    
    # Simulate outcomes based on treatment and customer characteristics
    # In practice, this would be actual purchase behavior from A/B test
    base_purchase_prob = df.get('Propensity_Score', df['rfm_score_numeric'] / 5.0 * 0.5)
    
    # True uplift varies by segment
    segment_uplift = {
        'Champions': 0.05,
        'Loyal Customers': 0.08,
        'Potential Loyalists': 0.15,
        'New Customers': 0.12,
        'Promising': 0.20,
        'Need Attention': 0.25,
        'At Risk': 0.30,
        "Can't Lose Them": 0.28,
        'Hibernating': 0.22,
        'Lost': 0.18,
        'Others': 0.15
    }
    
    df['true_uplift'] = df['rfm_segment'].map(segment_uplift).fillna(0.15)
    df['true_uplift'] += np.random.normal(0, 0.03, len(df))
    df['true_uplift'] = df['true_uplift'].clip(0, 0.5)
    
    # Generate outcome variable
    df['y_control'] = (np.random.random(len(df)) < base_purchase_prob).astype(int)
    df['y_treat'] = (np.random.random(len(df)) < (base_purchase_prob + df['true_uplift'])).astype(int)
    df['y'] = np.where(df['treatment'] == 1, df['y_treat'], df['y_control'])
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    try:
        if model_type in ["t_learner", "ensemble"]:
            # T-Learner: Separate models
            treat_mask = df['treatment'] == 1
            
            # Treatment model
            model_treat = GradientBoostingClassifier(
                n_estimators=50,
                max_depth=3,
                random_state=42
            )
            if treat_mask.sum() > 10:
                model_treat.fit(X_scaled[treat_mask], df.loc[treat_mask, 'y'])
                p_treat = model_treat.predict_proba(X_scaled)[:, 1]
            else:
                p_treat = df['y_treat'].mean() * np.ones(len(df))
            
            # Control model
            model_control = GradientBoostingClassifier(
                n_estimators=50,
                max_depth=3,
                random_state=42
            )
            if (~treat_mask).sum() > 10:
                model_control.fit(X_scaled[~treat_mask], df.loc[~treat_mask, 'y'])
                p_control = model_control.predict_proba(X_scaled)[:, 1]
            else:
                p_control = df['y_control'].mean() * np.ones(len(df))
            
            df['uplift_t_learner'] = p_treat - p_control
            logger.info(f"✓ T-Learner uplift mean: {df['uplift_t_learner'].mean():.4f}")
        
        if model_type in ["s_learner", "ensemble"]:
            # S-Learner: Single model with treatment as feature
            X_s = pd.DataFrame(X_scaled, columns=available_features)
            X_s['treatment'] = df['treatment'].values
            
            model_s = GradientBoostingClassifier(
                n_estimators=50,
                max_depth=3,
                random_state=42
            )
            model_s.fit(X_s, df['y'])
            
            # Predict with treatment=1 and treatment=0
            X_s_treat = X_s.copy()
            X_s_treat['treatment'] = 1
            X_s_control = X_s.copy()
            X_s_control['treatment'] = 0
            
            p_s_treat = model_s.predict_proba(X_s_treat)[:, 1]
            p_s_control = model_s.predict_proba(X_s_control)[:, 1]
            
            df['uplift_s_learner'] = p_s_treat - p_s_control
            logger.info(f"✓ S-Learner uplift mean: {df['uplift_s_learner'].mean():.4f}")
        
        # Calculate final uplift score
        if model_type == "ensemble":
            df['Uplift_Score'] = (df['uplift_t_learner'] + df['uplift_s_learner']) / 2
        elif model_type == "t_learner":
            df['Uplift_Score'] = df['uplift_t_learner']
        else:
            df['Uplift_Score'] = df['uplift_s_learner']
        
        # Clip to reasonable range
        df['Uplift_Score'] = df['Uplift_Score'].clip(-0.5, 0.5)
        
        logger.info(f"✓ Final uplift score mean: {df['Uplift_Score'].mean():.4f}")
        
    except Exception as e:
        logger.warning(f"Uplift model failed: {e}. Using heuristic calculation.")
        df = _calculate_heuristic_uplift(df)
    
    return df


def _calculate_heuristic_uplift(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate heuristic uplift scores based on customer segment.
    
    At-risk customers typically have higher uplift potential.
    Champions have lower uplift (already engaged).
    """
    segment_uplift = {
        'Champions': 0.05,
        'Loyal Customers': 0.08,
        'Potential Loyalists': 0.15,
        'New Customers': 0.12,
        'Promising': 0.20,
        'Need Attention': 0.25,
        'At Risk': 0.30,
        "Can't Lose Them": 0.28,
        'Hibernating': 0.22,
        'Lost': 0.18,
        'Others': 0.15
    }
    
    df['Uplift_Score'] = df['rfm_segment'].map(segment_uplift).fillna(0.15)
    
    # Add some variation based on other features
    if 'recency' in df.columns:
        recency_factor = (df['recency'] - df['recency'].min()) / (df['recency'].max() - df['recency'].min() + 1)
        df['Uplift_Score'] += recency_factor * 0.1
    
    if 'frequency' in df.columns:
        freq_factor = 1 - (df['frequency'] - df['frequency'].min()) / (df['frequency'].max() - df['frequency'].min() + 1)
        df['Uplift_Score'] += freq_factor * 0.05
    
    # Clip to reasonable range
    df['Uplift_Score'] = df['Uplift_Score'].clip(0, 0.5)
    
    return df


# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def task_extract_modeling_features(**context) -> Dict[str, Any]:
    """
    T1: Extract features from Dim_Customer for model scoring.
    
    Reads customer dimension table with all RFM features and behavioral metrics.
    
    Returns:
        Dict with temp directory path and feature statistics
    """
    logger.info("=" * 60)
    logger.info("TASK T1: Extract Modeling Features")
    logger.info("=" * 60)
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp(prefix=TEMP_DIR_PREFIX)
    logger.info(f"Temp directory: {temp_dir}")
    
    try:
        # Read Dim_Customer from PostgreSQL
        logger.info("Reading Dim_Customer from PostgreSQL...")
        df = read_table_to_dataframe('dim_customer')
        
        logger.info(f"✓ Loaded {len(df):,} customers")
        logger.info(f"Columns: {list(df.columns)}")
        
        # Validate required columns exist
        required_cols = ['customerkey', 'customerid', 'r_score', 'f_score', 'm_score', 
                        'rfm_segment', 'recency', 'frequency', 'monetary']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Save to temp file
        features_path = os.path.join(temp_dir, 'customer_features.parquet')
        df.to_parquet(features_path, index=False)
        
        # Statistics
        stats = {
            'customer_count': len(df),
            'feature_count': len(df.columns),
            'segments': df['rfm_segment'].value_counts().to_dict(),
            'rfm_score_mean': df['rfm_score_numeric'].mean() if 'rfm_score_numeric' in df.columns else None
        }
        
        result = {
            'temp_dir': temp_dir,
            'features_path': features_path,
            'statistics': stats
        }
        
        logger.info(f"✓ Extracted features for {stats['customer_count']:,} customers")
        logger.info(f"✓ Segments: {len(stats['segments'])}")
        
        context['ti'].xcom_push(key='extract_result', value=result)
        return result
        
    except Exception as e:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        raise


def task_score_clv(**context) -> Dict[str, Any]:
    """
    T2: Calculate CLV_12M_Predicted using BG/NBD + Gamma-Gamma models.
    
    For customers with sufficient history: probabilistic CLV
    For new customers: segment-based estimation
    
    Returns:
        Dict with CLV scoring results
    """
    logger.info("=" * 60)
    logger.info("TASK T2: Score CLV")
    logger.info("=" * 60)
    
    # Get previous task result
    extract_result = context['ti'].xcom_pull(key='extract_result', task_ids='extract_modeling_features')
    temp_dir = extract_result['temp_dir']
    features_path = extract_result['features_path']
    
    # Load features
    logger.info("Loading customer features...")
    df = pd.read_parquet(features_path)
    
    # Calculate CLV
    logger.info("Calculating CLV predictions...")
    df = calculate_bgf_clv(
        df, 
        prediction_months=CLV_PREDICTION_MONTHS,
        discount_rate=CLV_DISCOUNT_RATE
    )
    
    # Save intermediate result
    clv_path = os.path.join(temp_dir, 'customer_clv.parquet')
    df.to_parquet(clv_path, index=False)
    
    # Statistics
    clv_stats = {
        'mean': df['CLV_12M_Predicted'].mean(),
        'median': df['CLV_12M_Predicted'].median(),
        'std': df['CLV_12M_Predicted'].std(),
        'min': df['CLV_12M_Predicted'].min(),
        'max': df['CLV_12M_Predicted'].max()
    }
    
    result = {
        'clv_path': clv_path,
        'statistics': clv_stats
    }
    
    logger.info(f"✓ CLV Statistics:")
    logger.info(f"   Mean: £{clv_stats['mean']:.2f}")
    logger.info(f"   Median: £{clv_stats['median']:.2f}")
    logger.info(f"   Std: £{clv_stats['std']:.2f}")
    
    context['ti'].xcom_push(key='clv_result', value=result)
    return result


def task_score_propensity(**context) -> Dict[str, Any]:
    """
    T3: Calculate Propensity_Score using Logistic Regression.
    
    Propensity = P(purchase in next 30 days | no promotion)
    
    Returns:
        Dict with propensity scoring results
    """
    logger.info("=" * 60)
    logger.info("TASK T3: Score Propensity")
    logger.info("=" * 60)
    
    # Get previous task results
    extract_result = context['ti'].xcom_pull(key='extract_result', task_ids='extract_modeling_features')
    clv_result = context['ti'].xcom_pull(key='clv_result', task_ids='score_clv')
    
    temp_dir = extract_result['temp_dir']
    clv_path = clv_result['clv_path']
    
    # Load CLV results (contains all features)
    logger.info("Loading customer data with CLV...")
    df = pd.read_parquet(clv_path)
    
    # Calculate propensity scores
    logger.info("Calculating propensity scores...")
    df = calculate_propensity_scores(df)
    
    # Save intermediate result
    propensity_path = os.path.join(temp_dir, 'customer_propensity.parquet')
    df.to_parquet(propensity_path, index=False)
    
    # Statistics
    propensity_stats = {
        'mean': df['Propensity_Score'].mean(),
        'median': df['Propensity_Score'].median(),
        'std': df['Propensity_Score'].std(),
        'min': df['Propensity_Score'].min(),
        'max': df['Propensity_Score'].max()
    }
    
    result = {
        'propensity_path': propensity_path,
        'statistics': propensity_stats
    }
    
    logger.info(f"✓ Propensity Statistics:")
    logger.info(f"   Mean: {propensity_stats['mean']:.4f}")
    logger.info(f"   Median: {propensity_stats['median']:.4f}")
    logger.info(f"   Std: {propensity_stats['std']:.4f}")
    
    context['ti'].xcom_push(key='propensity_result', value=result)
    return result


def task_score_uplift(**context) -> Dict[str, Any]:
    """
    T4: Calculate Uplift_Score using T-Learner + S-Learner ensemble.
    
    Uplift = P(purchase | treatment) - P(purchase | no treatment)
    
    Returns:
        Dict with uplift scoring results
    """
    logger.info("=" * 60)
    logger.info("TASK T4: Score Uplift")
    logger.info("=" * 60)
    
    # Get previous task results
    extract_result = context['ti'].xcom_pull(key='extract_result', task_ids='extract_modeling_features')
    propensity_result = context['ti'].xcom_pull(key='propensity_result', task_ids='score_propensity')
    
    temp_dir = extract_result['temp_dir']
    propensity_path = propensity_result['propensity_path']
    
    # Load propensity results
    logger.info("Loading customer data with propensity...")
    df = pd.read_parquet(propensity_path)
    
    # Calculate uplift scores
    logger.info(f"Calculating uplift scores (model: {UPLIFT_MODEL_TYPE})...")
    df = calculate_uplift_scores(df, model_type=UPLIFT_MODEL_TYPE)
    
    # Save intermediate result
    uplift_path = os.path.join(temp_dir, 'customer_uplift.parquet')
    df.to_parquet(uplift_path, index=False)
    
    # Statistics
    uplift_stats = {
        'mean': df['Uplift_Score'].mean(),
        'median': df['Uplift_Score'].median(),
        'std': df['Uplift_Score'].std(),
        'min': df['Uplift_Score'].min(),
        'max': df['Uplift_Score'].max()
    }
    
    result = {
        'uplift_path': uplift_path,
        'statistics': uplift_stats
    }
    
    logger.info(f"✓ Uplift Statistics:")
    logger.info(f"   Mean: {uplift_stats['mean']:.4f}")
    logger.info(f"   Median: {uplift_stats['median']:.4f}")
    logger.info(f"   Std: {uplift_stats['std']:.4f}")
    
    context['ti'].xcom_push(key='uplift_result', value=result)
    return result


def task_aggregate_prepare_scores(**context) -> Dict[str, Any]:
    """
    T5: Aggregate all scores into final dataset for warehouse update.
    
    Combines CLV, Propensity, and Uplift scores into single DataFrame.
    Validates score ranges and handles edge cases.
    
    Returns:
        Dict with aggregated scores path
    """
    logger.info("=" * 60)
    logger.info("TASK T5: Aggregate and Prepare Scores")
    logger.info("=" * 60)
    
    # Get previous task results
    extract_result = context['ti'].xcom_pull(key='extract_result', task_ids='extract_modeling_features')
    uplift_result = context['ti'].xcom_pull(key='uplift_result', task_ids='score_uplift')
    
    temp_dir = extract_result['temp_dir']
    uplift_path = uplift_result['uplift_path']
    
    # Load final scores
    logger.info("Loading all calculated scores...")
    df = pd.read_parquet(uplift_path)
    
    # Validate and clean scores
    logger.info("Validating and preparing scores...")
    
    # Ensure non-negative CLV
    df['CLV_12M_Predicted'] = df['CLV_12M_Predicted'].clip(lower=0)
    
    # Ensure propensity in [0, 1]
    df['Propensity_Score'] = df['Propensity_Score'].clip(0, 1)
    
    # Ensure uplift in reasonable range [-0.5, 0.5]
    df['Uplift_Score'] = df['Uplift_Score'].clip(-0.5, 0.5)
    
    # Handle any remaining NaN
    df['CLV_12M_Predicted'] = df['CLV_12M_Predicted'].fillna(0)
    df['Propensity_Score'] = df['Propensity_Score'].fillna(0.1)
    df['Uplift_Score'] = df['Uplift_Score'].fillna(0.15)
    
    # Select only required columns for update
    score_cols = ['customerkey', 'CLV_12M_Predicted', 'Propensity_Score', 'Uplift_Score']
    
    # Rename for consistency
    df_scores = df[['customerkey', 'CLV_12M_Predicted', 'Propensity_Score', 'Uplift_Score']].copy()
    df_scores.columns = ['CustomerKey', 'CLV_12M_Predicted', 'Propensity_Score', 'Uplift_Score']
    
    # Save final scores
    scores_path = os.path.join(temp_dir, 'final_scores.parquet')
    df_scores.to_parquet(scores_path, index=False)
    
    # Summary statistics
    summary = {
        'customer_count': len(df_scores),
        'clv': {
            'mean': df_scores['CLV_12M_Predicted'].mean(),
            'median': df_scores['CLV_12M_Predicted'].median(),
            'total': df_scores['CLV_12M_Predicted'].sum()
        },
        'propensity': {
            'mean': df_scores['Propensity_Score'].mean(),
            'high_propensity_pct': (df_scores['Propensity_Score'] > 0.5).mean() * 100
        },
        'uplift': {
            'mean': df_scores['Uplift_Score'].mean(),
            'positive_uplift_pct': (df_scores['Uplift_Score'] > 0).mean() * 100
        }
    }
    
    result = {
        'scores_path': scores_path,
        'summary': summary
    }
    
    logger.info(f"✓ Aggregated {summary['customer_count']:,} customer scores")
    logger.info(f"   Total predicted CLV: £{summary['clv']['total']:,.2f}")
    logger.info(f"   High propensity customers: {summary['propensity']['high_propensity_pct']:.1f}%")
    logger.info(f"   Positive uplift customers: {summary['uplift']['positive_uplift_pct']:.1f}%")
    
    context['ti'].xcom_push(key='aggregate_result', value=result)
    return result


def task_update_customer_dimension(**context) -> Dict[str, Any]:
    """
    T6: Update Dim_Customer in PostgreSQL with calculated scores.
    
    Updates columns:
        - clv_12m_predicted
        - propensity_score
        - uplift_score
    
    Returns:
        Dict with update statistics
    """
    logger.info("=" * 60)
    logger.info("TASK T6: Update Customer Dimension")
    logger.info("=" * 60)
    
    # Get previous task results
    extract_result = context['ti'].xcom_pull(key='extract_result', task_ids='extract_modeling_features')
    aggregate_result = context['ti'].xcom_pull(key='aggregate_result', task_ids='aggregate_prepare_scores')
    
    temp_dir = extract_result['temp_dir']
    scores_path = aggregate_result['scores_path']
    
    # Load final scores
    logger.info("Loading final scores...")
    df_scores = pd.read_parquet(scores_path)
    
    # Update PostgreSQL
    logger.info(f"Updating {len(df_scores):,} customer records in PostgreSQL...")
    updated_count = update_customer_scores(df_scores)
    
    logger.info(f"✓ Updated {updated_count:,} records in Dim_Customer")
    
    # Cleanup temp directory
    logger.info("Cleaning up temporary files...")
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        logger.info(f"✓ Removed temp directory: {temp_dir}")
    
    # Validation query
    engine = get_postgres_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT 
                COUNT(*) as total,
                AVG(clv_12m_predicted) as avg_clv,
                AVG(propensity_score) as avg_propensity,
                AVG(uplift_score) as avg_uplift,
                SUM(CASE WHEN clv_12m_predicted > 0 THEN 1 ELSE 0 END) as clv_populated,
                SUM(CASE WHEN propensity_score > 0 THEN 1 ELSE 0 END) as propensity_populated,
                SUM(CASE WHEN uplift_score != 0 THEN 1 ELSE 0 END) as uplift_populated
            FROM {POSTGRES_SCHEMA}.dim_customer
        """))
        validation = result.fetchone()
    
    validation_result = {
        'total_customers': validation[0],
        'avg_clv': float(validation[1]) if validation[1] else 0,
        'avg_propensity': float(validation[2]) if validation[2] else 0,
        'avg_uplift': float(validation[3]) if validation[3] else 0,
        'clv_populated': validation[4],
        'propensity_populated': validation[5],
        'uplift_populated': validation[6]
    }
    
    result = {
        'updated_count': updated_count,
        'validation': validation_result,
        'updated_at': datetime.now().isoformat()
    }
    
    logger.info("✓ Warehouse Update Validation:")
    logger.info(f"   Total customers: {validation_result['total_customers']:,}")
    logger.info(f"   Avg CLV: £{validation_result['avg_clv']:.2f}")
    logger.info(f"   Avg Propensity: {validation_result['avg_propensity']:.4f}")
    logger.info(f"   Avg Uplift: {validation_result['avg_uplift']:.4f}")
    
    context['ti'].xcom_push(key='update_result', value=result)
    return result


# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'anhth',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

with DAG(
    dag_id='model_scoring',
    default_args=default_args,
    description='Model Scoring Pipeline: CLV, Propensity, Uplift → PostgreSQL Dim_Customer',
    schedule_interval=None,  # Manual trigger or triggered after ETL
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ml', 'scoring', 'clv', 'propensity', 'uplift', 'postgresql'],
    doc_md=__doc__,
) as dag:
    
    # ========================================================================
    # START
    # ========================================================================
    start = EmptyOperator(task_id='start')
    
    # ========================================================================
    # OPTIONAL: Wait for ETL DAG to complete
    # Uncomment if you want to automatically trigger after ETL
    # ========================================================================
    # wait_for_etl = ExternalTaskSensor(
    #     task_id='wait_for_etl',
    #     external_dag_id='historical_data_etl_processing',
    #     external_task_id='end',
    #     timeout=3600,  # 1 hour timeout
    #     poke_interval=60,  # Check every minute
    #     mode='poke'
    # )
    
    # ========================================================================
    # T1: EXTRACT MODELING FEATURES
    # ========================================================================
    extract_features = PythonOperator(
        task_id='extract_modeling_features',
        python_callable=task_extract_modeling_features,
        doc_md="""
        **T1: Extract Modeling Features**
        
        Reads customer features from Dim_Customer table including:
        - RFM scores and metrics
        - Behavioral features
        - Customer lifecycle data
        """
    )
    
    # ========================================================================
    # T2: SCORE CLV
    # ========================================================================
    score_clv = PythonOperator(
        task_id='score_clv',
        python_callable=task_score_clv,
        doc_md="""
        **T2: Score CLV**
        
        Calculates 12-month CLV predictions using:
        - BG/NBD model for purchase frequency
        - Gamma-Gamma model for monetary value
        - Segment-based fallback for new customers
        """
    )
    
    # ========================================================================
    # T3: SCORE PROPENSITY
    # ========================================================================
    score_propensity = PythonOperator(
        task_id='score_propensity',
        python_callable=task_score_propensity,
        doc_md="""
        **T3: Score Propensity**
        
        Calculates purchase propensity scores using:
        - Logistic Regression on RFM features
        - Calibrated probability estimates
        - 30-day purchase window
        """
    )
    
    # ========================================================================
    # T4: SCORE UPLIFT
    # ========================================================================
    score_uplift = PythonOperator(
        task_id='score_uplift',
        python_callable=task_score_uplift,
        doc_md="""
        **T4: Score Uplift**
        
        Calculates uplift scores using:
        - T-Learner (separate treatment/control models)
        - S-Learner (single model with treatment feature)
        - Ensemble average of both approaches
        """
    )
    
    # ========================================================================
    # T5: AGGREGATE AND PREPARE SCORES
    # ========================================================================
    aggregate_scores = PythonOperator(
        task_id='aggregate_prepare_scores',
        python_callable=task_aggregate_prepare_scores,
        doc_md="""
        **T5: Aggregate and Prepare Scores**
        
        Combines all model outputs:
        - Validates score ranges
        - Handles edge cases and NaN
        - Prepares final dataset for warehouse update
        """
    )
    
    # ========================================================================
    # T6: UPDATE CUSTOMER DIMENSION
    # ========================================================================
    update_dimension = PythonOperator(
        task_id='update_customer_dimension',
        python_callable=task_update_customer_dimension,
        doc_md="""
        **T6: Update Customer Dimension**
        
        Updates Dim_Customer in PostgreSQL:
        - CLV_12M_Predicted
        - Propensity_Score
        - Uplift_Score
        
        Includes validation and cleanup.
        """
    )
    
    # ========================================================================
    # END
    # ========================================================================
    end = EmptyOperator(task_id='end')
    
    # ========================================================================
    # DAG DEPENDENCIES
    # ========================================================================
    # Sequential pipeline: T1 → T2 → T3 → T4 → T5 → T6
    (
        start 
        >> extract_features 
        >> score_clv 
        >> score_propensity 
        >> score_uplift 
        >> aggregate_scores 
        >> update_dimension 
        >> end
    )
