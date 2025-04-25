#!/usr/bin/env python3
import os
import json
import pandas as pd
import numpy as np
from kafka import KafkaConsumer
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import joblib
from pymongo import MongoClient

# Disable warnings
warnings.filterwarnings('ignore')

# Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'telemetry-data'
MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB = 'f1_data'
MONGO_COLLECTION = 'laps'
MODELS_DIR = 'models'

# Ensure models directory exists
os.makedirs(MODELS_DIR, exist_ok=True)

class F1TelemetryPredictor:
    def __init__(self, use_kafka=True, use_mongodb=False):
        """Initialize the F1 telemetry predictor class"""
        self.use_kafka = use_kafka
        self.use_mongodb = use_mongodb
        self.data = None
        self.weather_data = None
        self.lap_time_model = None
        self.tire_deg_model = None
        self.pit_strategy_model = None
        self.race_outcome_model = None
        
        # Initialize Kafka consumer if needed
        if self.use_kafka:
            self.kafka_consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
        
        # Initialize MongoDB client if needed
        if self.use_mongodb:
            self.mongo_client = MongoClient(MONGO_URI)
            self.db = self.mongo_client[MONGO_DB]
            self.collection = self.db[MONGO_COLLECTION]
    
    def fetch_data_from_kafka(self, max_records=1000):
        """Fetch telemetry data from Kafka"""
        print("Fetching data from Kafka...")
        records = []
        count = 0
        
        # Set timeout for consumer poll
        for message in self.kafka_consumer:
            records.append(message.value)
            count += 1
            if count >= max_records:
                break
        
        if records:
            self.data = pd.DataFrame(records)
            print(f"Fetched {len(self.data)} records from Kafka")
            return True
        else:
            print("No records fetched from Kafka")
            return False
    
    def fetch_data_from_mongodb(self):
        """Fetch telemetry data from MongoDB"""
        print("Fetching data from MongoDB...")
        records = list(self.collection.find())
        
        if records:
            self.data = pd.DataFrame(records)
            # Drop MongoDB's _id field
            if '_id' in self.data.columns:
                self.data = self.data.drop('_id', axis=1)
            print(f"Fetched {len(self.data)} records from MongoDB")
            return True
        else:
            print("No records fetched from MongoDB")
            return False
    
    def clean_data(self):
        """Clean and preprocess the telemetry data"""
        if self.data is None or len(self.data) == 0:
            print("No data to clean")
            return False
        
        print("Cleaning data...")
        
        # Convert data types
        numeric_cols = ['LapNumber', 'LapTime', 'Sector1Time', 'Sector2Time', 'Sector3Time', 
                       'Distance', 'PitOutTime', 'PitInTime', 'Stint']
        
        for col in numeric_cols:
            if col in self.data.columns:
                # Try to convert to float, replacing errors with NaN
                self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
        
        # Handle missing values
        for col in self.data.columns:
            if self.data[col].dtype == 'float64' or self.data[col].dtype == 'int64':
                # Replace missing values with median for numeric columns
                self.data[col] = self.data[col].fillna(self.data[col].median())
            else:
                # Replace missing values with most frequent value for categorical columns
                self.data[col] = self.data[col].fillna(self.data[col].mode()[0] if not self.data[col].mode().empty else "Unknown")
        
        # Feature engineering
        if 'LapTime' in self.data.columns and 'Distance' in self.data.columns:
            self.data['AvgSpeed'] = self.data['Distance'] / self.data['LapTime']
        
        if 'Sector1Time' in self.data.columns and 'Sector2Time' in self.data.columns and 'Sector3Time' in self.data.columns:
            self.data['Sector1Ratio'] = self.data['Sector1Time'] / self.data['LapTime']
            self.data['Sector2Ratio'] = self.data['Sector2Time'] / self.data['LapTime']
            self.data['Sector3Ratio'] = self.data['Sector3Time'] / self.data['LapTime']
        
        if 'Stint' in self.data.columns and 'LapNumber' in self.data.columns:
            # Group by driver and stint to calculate laps per stint
            stint_laps = self.data.groupby(['Driver', 'Stint']).agg({'LapNumber': 'count'}).reset_index()
            stint_laps.columns = ['Driver', 'Stint', 'StintLaps']
            self.data = pd.merge(self.data, stint_laps, on=['Driver', 'Stint'], how='left')
        
        print("Data cleaning complete")
        return True
    
    def explore_data(self):
        """Explore the data and generate insights"""
        if self.data is None or len(self.data) == 0:
            print("No data to explore")
            return
        
        print("Data exploration:")
        print(f"Shape: {self.data.shape}")
        print("\nColumns:")
        for col in self.data.columns:
            print(f"- {col}: {self.data[col].dtype}")
        
        print("\nSummary statistics:")
        numeric_data = self.data.select_dtypes(include=['float64', 'int64'])
        print(numeric_data.describe())
        
        # Driver performance analysis
        if 'Driver' in self.data.columns and 'LapTime' in self.data.columns:
            driver_perf = self.data.groupby('Driver')['LapTime'].agg(['mean', 'min', 'max', 'std']).reset_index()
            driver_perf.columns = ['Driver', 'AvgLapTime', 'BestLapTime', 'WorstLapTime', 'LapTimeStdDev']
            print("\nDriver Performance:")
            print(driver_perf.sort_values('AvgLapTime').head(10))
        
        # Visualize lap time distribution
        if 'LapTime' in self.data.columns:
            plt.figure(figsize=(10, 6))
            sns.histplot(self.data['LapTime'].dropna(), kde=True)
            plt.title('Lap Time Distribution')
            plt.xlabel('Lap Time (s)')
            plt.ylabel('Frequency')
            plt.savefig('lap_time_distribution.png')
            plt.close()
        
        # Visualize driver comparison
        if 'Driver' in self.data.columns and 'LapTime' in self.data.columns:
            plt.figure(figsize=(12, 8))
            sns.boxplot(x='Driver', y='LapTime', data=self.data)
            plt.xticks(rotation=90)
            plt.title('Lap Time Comparison by Driver')
            plt.xlabel('Driver')
            plt.ylabel('Lap Time (s)')
            plt.savefig('driver_comparison.png')
            plt.close()
    
    def build_lap_time_prediction_model(self):
        """Build a model to predict lap times"""
        if self.data is None or len(self.data) == 0:
            print("No data to build lap time prediction model")
            return
        
        print("Building lap time prediction model...")
        
        # Feature selection for lap time prediction
        features = ['Driver', 'LapNumber', 'Stint', 'Compound']
        target = 'LapTime'
        
        # Ensure all features and target are present
        missing_cols = [col for col in features + [target] if col not in self.data.columns]
        if missing_cols:
            print(f"Missing columns: {missing_cols}")
            # Use available columns only
            features = [col for col in features if col in self.data.columns]
            if target not in self.data.columns:
                print(f"Target column '{target}' not available")
                return
        
        # Prepare data
        X = self.data[features].copy()
        y = self.data[target]
        
        # Handle missing values in target
        mask = ~y.isna()
        X = X[mask]
        y = y[mask]
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Define preprocessing for numerical and categorical features
        numerical_features = ['LapNumber', 'Stint'] if all(col in X.columns for col in ['LapNumber', 'Stint']) else []
        categorical_features = ['Driver', 'Compound'] if all(col in X.columns for col in ['Driver', 'Compound']) else []
        
        if not numerical_features and not categorical_features:
            print("No valid features available for preprocessing")
            return
        
        # Create preprocessor
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), numerical_features),
                ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
            ],
            remainder='drop'
        )
        
        # Create pipeline with RandomForest
        model = Pipeline([
            ('preprocessor', preprocessor),
            ('regressor', RandomForestRegressor(n_estimators=100, random_state=42))
        ])
        
        # Train model
        model.fit(X_train, y_train)
        
        # Evaluate model
        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        print(f"Lap Time Prediction Model Performance:")
        print(f"Mean Squared Error: {mse:.2f}")
        print(f"Mean Absolute Error: {mae:.2f} seconds")
        print(f"R² Score: {r2:.4f}")
        
        # Save model
        joblib.dump(model, os.path.join(MODELS_DIR, 'lap_time_model.pkl'))
        
        self.lap_time_model = model
        return model
    
    def build_tire_degradation_model(self):
        """Build a model to predict tire degradation"""
        if self.data is None or len(self.data) == 0:
            print("No data to build tire degradation model")
            return
        
        print("Building tire degradation model...")
        
        # Check if we have the necessary columns
        required_cols = ['Driver', 'Compound', 'LapNumber', 'Stint', 'LapTime']
        missing_cols = [col for col in required_cols if col not in self.data.columns]
        
        if missing_cols:
            print(f"Missing columns for tire degradation model: {missing_cols}")
            return
        
        # Group by driver, stint, and compound to analyze lap time increase
        stint_data = self.data.groupby(['Driver', 'Stint', 'Compound']).apply(
            lambda x: pd.Series({
                'StintLength': x['LapNumber'].max() - x['LapNumber'].min() + 1,
                'FirstLapTime': x.loc[x['LapNumber'] == x['LapNumber'].min(), 'LapTime'].values[0],
                'LastLapTime': x.loc[x['LapNumber'] == x['LapNumber'].max(), 'LapTime'].values[0],
                'TimeDegradation': x.loc[x['LapNumber'] == x['LapNumber'].max(), 'LapTime'].values[0] - 
                                  x.loc[x['LapNumber'] == x['LapNumber'].min(), 'LapTime'].values[0]
            })
        ).reset_index()
        
        # Calculate degradation per lap
        stint_data['DegradationPerLap'] = stint_data['TimeDegradation'] / stint_data['StintLength']
        
        # Features for tire degradation model
        X = stint_data[['Driver', 'Compound', 'StintLength']]
        y = stint_data['DegradationPerLap']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Preprocessing
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), ['StintLength']),
                ('cat', OneHotEncoder(handle_unknown='ignore'), ['Driver', 'Compound'])
            ]
        )
        
        # Create pipeline with Gradient Boosting
        model = Pipeline([
            ('preprocessor', preprocessor),
            ('regressor', GradientBoostingRegressor(n_estimators=100, random_state=42))
        ])
        
        # Train model
        model.fit(X_train, y_train)
        
        # Evaluate model
        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        print(f"Tire Degradation Model Performance:")
        print(f"Mean Squared Error: {mse:.6f}")
        print(f"Mean Absolute Error: {mae:.6f} seconds per lap")
        print(f"R² Score: {r2:.4f}")
        
        # Save model
        joblib.dump(model, os.path.join(MODELS_DIR, 'tire_degradation_model.pkl'))
        
        # Visualize tire degradation by compound
        plt.figure(figsize=(10, 6))
        sns.boxplot(x='Compound', y='DegradationPerLap', data=stint_data)
        plt.title('Tire Degradation by Compound')
        plt.xlabel('Tire Compound')
        plt.ylabel('Degradation (seconds per lap)')
        plt.savefig('tire_degradation.png')
        plt.close()
        
        self.tire_deg_model = model
        return model
    
    def build_pit_strategy_optimizer(self):
        """Build a model to optimize pit stop strategy"""
        if self.data is None or len(self.data) == 0:
            print("No data to build pit strategy optimizer")
            return
        
        print("Building pit strategy optimizer...")
        
        # Check if we have the necessary columns
        required_cols = ['Driver', 'Compound', 'LapNumber', 'Stint', 'LapTime', 'PitOutTime', 'PitInTime']
        missing_cols = [col for col in required_cols if col not in self.data.columns]
        
        if missing_cols:
            print(f"Missing columns for pit strategy optimizer: {missing_cols}")
            return
        
        # Calculate pit stop durations
        pit_stops = self.data[self.data['PitInTime'].notna() & self.data['PitOutTime'].notna()]
        
        if pit_stops.empty:
            print("No pit stop data available")
            return
        
        # Calculate pit stop duration
        pit_stops['PitStopDuration'] = pit_stops['PitOutTime'] - pit_stops['PitInTime']
        
        # Analyze pit stop performance by driver
        pit_perf = pit_stops.groupby('Driver')['PitStopDuration'].agg(['mean', 'min', 'count']).reset_index()
        pit_perf.columns = ['Driver', 'AvgPitStopTime', 'BestPitStopTime', 'PitStopCount']
        
        print("\nPit Stop Performance:")
        print(pit_perf.sort_values('AvgPitStopTime').head(10))
        
        # Analyze tire compounds and stint lengths
        stint_analysis = self.data.groupby(['Driver', 'Stint', 'Compound']).agg({
            'LapNumber': ['min', 'max', 'count'],
            'LapTime': ['mean', 'min', 'max']
        }).reset_index()
        
        stint_analysis.columns = [
            '_'.join(col).strip('_') for col in stint_analysis.columns.values
        ]
        
        stint_analysis['StintLength'] = stint_analysis['LapNumber_max'] - stint_analysis['LapNumber_min'] + 1
        stint_analysis['LapTimeDegradation'] = stint_analysis['LapTime_max'] - stint_analysis['LapTime_min']
        
        print("\nStint Analysis:")
        print(stint_analysis.head())
        
        # Visualize stint length by compound
        plt.figure(figsize=(10, 6))
        sns.boxplot(x='Compound', y='StintLength', data=stint_analysis)
        plt.title('Stint Length by Tire Compound')
        plt.xlabel('Tire Compound')
        plt.ylabel('Stint Length (laps)')
        plt.savefig('stint_length.png')
        plt.close()
        
        # Save stint analysis for strategy planning
        stint_analysis.to_csv('stint_analysis.csv', index=False)
        
        print("Pit strategy analysis complete")
        return stint_analysis
    
    def build_race_outcome_prediction(self):
        """Build a model to predict race outcomes"""
        if self.data is None or len(self.data) == 0:
            print("No data to build race outcome prediction")
            return
        
        print("Building race outcome prediction model...")
        
        # Check if we have the necessary columns
        required_cols = ['Driver', 'LapNumber', 'LapTime', 'Stint']
        missing_cols = [col for col in required_cols if col not in self.data.columns]
        
        if missing_cols:
            print(f"Missing columns for race outcome prediction: {missing_cols}")
            return
        
        # Aggregate race performance by driver
        race_perf = self.data.groupby('Driver').agg({
            'LapTime': ['mean', 'min', 'std'],
            'LapNumber': 'max'
        }).reset_index()
        
        race_perf.columns = ['Driver', 'AvgLapTime', 'BestLapTime', 'LapTimeConsistency', 'LapsCompleted']
        
        # Calculate theoretical race time based on average lap times
        total_laps = race_perf['LapsCompleted'].max()
        race_perf['TheoreticalRaceTime'] = race_perf['AvgLapTime'] * total_laps
        
        # Rank drivers by theoretical race time
        race_perf = race_perf.sort_values('TheoreticalRaceTime')
        race_perf['PredictedPosition'] = range(1, len(race_perf) + 1)
        
        print("\nPredicted Race Outcome:")
        print(race_perf[['Driver', 'PredictedPosition', 'TheoreticalRaceTime', 'AvgLapTime', 'BestLapTime']])
        
        # Visualize predicted race outcome
        plt.figure(figsize=(12, 8))
        sns.barplot(x='Driver', y='TheoreticalRaceTime', data=race_perf)
        plt.xticks(rotation=90)
        plt.title('Predicted Race Time by Driver')
        plt.xlabel('Driver')
        plt.ylabel('Theoretical Race Time (s)')
        plt.savefig('predicted_race_time.png')
        plt.close()
        
        print("Race outcome prediction complete")
        return race_perf
    
    def run_full_analysis(self):
        """Run the full analysis pipeline"""
        # Load data
        if self.use_kafka:
            data_loaded = self.fetch_data_from_kafka()
        elif self.use_mongodb:
            data_loaded = self.fetch_data_from_mongodb()
        else:
            print("No data source specified")
            return
        
        if not data_loaded:
            print("Failed to load data")
            return
        
        # Clean and prepare data
        if not self.clean_data():
            print("Data cleaning failed")
            return
        
        # Explore data
        self.explore_data()
        
        # Build prediction models
        print("\n=== Building Prediction Models ===")
        self.build_lap_time_prediction_model()
        self.build_tire_degradation_model()
        self.build_pit_strategy_optimizer()
        self.build_race_outcome_prediction()
        
        print("\n=== Analysis Complete ===")
        print("Results and visualizations have been saved")

if __name__ == "__main__":
    # Create predictor instance with Kafka as data source
    predictor = F1TelemetryPredictor(use_kafka=True, use_mongodb=False)
    
    # Run full analysis
    predictor.run_full_analysis() 