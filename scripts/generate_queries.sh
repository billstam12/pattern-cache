#!/bin/bash

# Script to generate queries for all combinations of accuracy levels, segment sizes, datasets, and probabilities
# Based on the folder structure: queries/{acc_level}/{seg_size}/*_exp_queries_{prob}.txt

# Define arrays for different parameters
accuracies=("low_acc" "mid_acc" "high_acc")
accuracy_values=(0.9 0.95 0.99)
segment_sizes=("small" "mid" "big")
datasets=("manufacturing_exp" "intel_lab_exp" "soccer_exp")
measures=(1 2 9)  # manufacturing: 1, intel: 2, soccer: 9
probabilities=("10p")
probability_values=(0.1)  # 1p = 0.01, 100p = 1.0

echo "Starting query generation for all combinations..."

# Loop through all combinations
for acc_idx in "${!accuracies[@]}"; do
    acc_name="${accuracies[$acc_idx]}"
    acc_value="${accuracy_values[$acc_idx]}"
    
    for seg_size in "${segment_sizes[@]}"; do
        for dataset_idx in "${!datasets[@]}"; do
            dataset="${datasets[$dataset_idx]}"
            measure="${measures[$dataset_idx]}"
            type_to_use="trino"
            
            for prob_idx in "${!probabilities[@]}"; do
                prob_name="${probabilities[$prob_idx]}"
                prob_value="${probability_values[$prob_idx]}"
                
                # Create output directory structure
                output_dir="queries/${acc_name}/${seg_size}_seg"
                mkdir -p "$output_dir"
                
                echo "Generating queries for: ${acc_name} (${acc_value}) - ${seg_size}_seg - ${dataset} - ${prob_name} (${prob_value}) - ${type_to_use}"
                
                # Run the Java command
                java -jar target/pattern-cache-1.0-SNAPSHOT.jar \
                    -q "$prob_value" \
                    -mode generate \
                    -stateConfig config/pattern-hunter.properties \
                    -segSize "$seg_size" \
                    -type "$type_to_use" \
                    -table "$dataset" \
                    -out "$output_dir" \
                    -a "$acc_value" \
                    -measures "$measure" \
                    -seqCount 50
                
                # Check if command was successful
                if [ $? -eq 0 ]; then
                    echo "✓ Successfully generated queries for ${dataset} with ${prob_name} probability"
                else
                    echo "✗ Failed to generate queries for ${dataset} with ${prob_name} probability"
                fi
            done
        done
    done
done

echo "Query generation completed!"
echo "Generated queries are organized in: queries/{accuracy}/{segment_size}/{dataset}_queries_{probability}.txt"