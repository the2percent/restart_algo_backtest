#!/usr/bin/env python3
"""
Main script to run all analyzer modules in sequence.
This script executes all data analyzer modules one by one automatically.
"""

import subprocess
import sys
import time
from datetime import datetime

def run_module(module_name):
    """Run a Python module and handle the output."""
    print(f"\n{'='*80}")
    print(f"Starting: {module_name}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    start_time = time.time()
    
    try:
        # Run the module using subprocess with real-time output
        result = subprocess.run(
            [sys.executable, "-m", module_name],
            text=True,
            check=True
        )
        
        # No need to print output since it's already shown in real-time
            
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n✅ SUCCESS: {module_name} completed in {duration:.2f} seconds")
        return True
        
    except subprocess.CalledProcessError as e:
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n❌ ERROR: {module_name} failed after {duration:.2f} seconds")
        print(f"Return code: {e.returncode}")
        
        # Error output is already shown in real-time, no need to print again
            
        return False
    
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n❌ UNEXPECTED ERROR: {module_name} failed after {duration:.2f} seconds")
        print(f"Error: {str(e)}")
        return False

def main():
    """Main function to run all analyzer modules."""
    print("🚀 Starting All Data Analyzer Modules")
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # List of modules to run in order
    modules = [
        # Prepare data for analysis: download and resampling to daily, weekly, monthly
        "data_fetcher.read_instrument_details_from2000",
        "data_fetcher.prepare_output_folder",

        # Analyze the resampled data: ema / rsi / volume
        "data_analyser_algos.ema_analyser",
        "data_analyser_algos.rsi_analyser", 
        "data_analyser_algos.volume_analyser",

        # filter for results.
        "data_analyser_algos.misc_analyser"
    ]
    
    total_start_time = time.time()
    results = {}
    
    for i, module in enumerate(modules):
        success = run_module(module)
        results[module] = 'SUCCESS' if success else 'FAILED'
        
        # If module failed, stop execution and mark remaining modules as not run
        if not success:
            print(f"\n🛑 STOPPING EXECUTION: {module} failed")
            # Mark remaining modules as not run
            for remaining_module in modules[i+1:]:
                results[remaining_module] = 'NOT_RUN'
            break
        
        # Small delay between modules
        time.sleep(1)
    
    # Summary
    total_end_time = time.time()
    total_duration = total_end_time - total_start_time
    
    print(f"\n{'='*80}")
    print("📊 EXECUTION SUMMARY")
    print(f"{'='*80}")
    print(f"Total execution time: {total_duration/60:.2f} minutes")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    successful = 0
    failed = 0
    not_run = 0
    
    for module, status in results.items():
        if status == 'SUCCESS':
            print(f"✅ SUCCESS: {module}")
            successful += 1
        elif status == 'FAILED':
            print(f"❌ FAILED: {module}")
            failed += 1
        elif status == 'NOT_RUN':
            print(f"⚪ NOT RUN: {module}")
            not_run += 1
    
    print(f"\nSummary: {successful} successful | {failed} failed | {not_run} didn't run (Total: {len(modules)})")
    
    if failed > 0:
        print(f"⚠️  Execution stopped due to failure in module. {not_run} module(s) were not executed.")
        sys.exit(1)
    else:
        print("🎉 All modules completed successfully!")
        sys.exit(0)

if __name__ == "__main__":
    main()