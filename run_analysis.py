#!/usr/bin/env python3
from f1_prediction_model_v2 import F1TelemetryPredictor
import os

def main():
    # Create predictor instance
    predictor = F1TelemetryPredictor(use_kafka=False, use_mongodb=True)
    
    # Fetch data from MongoDB
    if not predictor.fetch_data_from_mongodb():
        print("Failed to fetch data from MongoDB")
        return
    
    # Clean and prepare data
    if not predictor.clean_data():
        print("Failed to clean data")
        return
    
    # Run the full analysis
    predictor.run_full_analysis()
    
    # Print the final predictions
    if predictor.race_outcome_model is not None:
        print("\nFinal Australian GP Predictions:")
        print(predictor.race_outcome_model[['Driver', 'PredictedPosition', 'TheoreticalRaceTime', 
                                          'LapTime_mean', 'LapTime_min', 'TeamName']].head(10))
        
        print("\nPredicted Gaps to Leader:")
        print(predictor.race_outcome_model[['Driver', 'PredictedPosition', 'GapToLeader']].head(10))
        
        print("\nModel Accuracy:")
        actual_results = {
            'SAI': 1,
            'NOR': 2,
            'LEC': 3,
            'PIA': 4,
            'RUS': 5,
            'VER': 6,
        }
        
        correct = 0
        total = len(actual_results)
        
        for driver, actual_pos in actual_results.items():
            if driver in predictor.race_outcome_model['Driver'].values:
                predicted_pos = predictor.race_outcome_model[predictor.race_outcome_model['Driver'] == driver]['PredictedPosition'].values[0]
                is_correct = actual_pos == predicted_pos
                correct += 1 if is_correct else 0
                print(f"Driver: {driver} - Actual: {actual_pos}, Predicted: {predicted_pos} - {'✓' if is_correct else '✗'}")
        
        accuracy = (correct / total) * 100
        print(f"\nOverall accuracy: {accuracy:.2f}% ({correct}/{total} correct predictions)")

if __name__ == "__main__":
    main() 