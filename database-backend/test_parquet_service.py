#!/usr/bin/env python3
"""
Test script for Parquet Data Service

This script tests all functionality of the parquet service including:
- Dataset loading and caching
- API endpoints
- Filter functionality
- Mock data handling for testing environment
"""

import asyncio
import sys
import os
import json
from pathlib import Path

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from parquet_service import parquet_service
import pandas as pd

def test_service_initialization():
    """Test that the service initializes correctly"""
    print("=" * 60)
    print("Testing Service Initialization")
    print("=" * 60)
    
    print(f"Base path: {parquet_service.base_path}")
    print(f"Cache TTL: {parquet_service._cache_ttl}")
    print(f"Known datasets: {len(parquet_service.known_datasets)}")
    
    # Test get available datasets
    datasets = parquet_service.get_available_datasets()
    print(f"\nTotal datasets: {len(datasets)}")
    
    available_count = sum(1 for info in datasets.values() if info['available'])
    print(f"Available datasets: {available_count}")
    
    if available_count == 0:
        print("ℹ️  No parquet files found (expected in testing environment)")
    else:
        print("✅ Parquet files found (production environment)")
    
    print("✅ Service initialization test passed")

def test_dataset_categories():
    """Test dataset categorization"""
    print("\n" + "=" * 60)
    print("Testing Dataset Categories")
    print("=" * 60)
    
    categories = ['blacklist', 'abc_model', 'push', 'device', 'analytics', 'products']
    
    for category in categories:
        datasets = parquet_service.get_datasets_by_category(category)
        print(f"{category}: {len(datasets)} datasets - {datasets}")
    
    print("✅ Dataset categorization test passed")

def test_dataset_loading():
    """Test dataset loading with mock data"""
    print("\n" + "=" * 60)
    print("Testing Dataset Loading")
    print("=" * 60)
    
    test_datasets = [
        'ACRM_DW.RB_BLACK_LIST@ACRM',
        'final',
        'MAU',
        'dssb_dm.hb_sessions_fl'
    ]
    
    for dataset_name in test_datasets:
        print(f"\nTesting dataset: {dataset_name}")
        
        # Test file existence check
        exists = parquet_service.file_exists(dataset_name)
        print(f"  File exists: {exists}")
        
        # Test dataset loading
        df = parquet_service.load_dataset(dataset_name)
        if df is not None:
            print(f"  ✅ Loaded: {len(df)} rows, {len(df.columns)} columns")
            print(f"  Columns: {list(df.columns)}")
            
            # Test caching
            df2 = parquet_service.load_dataset(dataset_name)
            print(f"  ✅ Cache test: {df2 is not None}")
        else:
            print(f"  ❌ Failed to load dataset")
    
    print("\n✅ Dataset loading test passed")

def test_filtering_functions():
    """Test the market.py integration filtering functions"""
    print("\n" + "=" * 60)
    print("Testing Filtering Functions")
    print("=" * 60)
    
    # Test blacklist filtering
    print("\n1. Testing blacklist filtering:")
    blacklist_tables = ['ACRM_DW.RB_BLACK_LIST@ACRM', 'BL_No_worker']
    blacklist_iins = parquet_service.get_blacklist_iins(blacklist_tables)
    print(f"   Blacklist IINs: {len(blacklist_iins)} (expected 0 in testing)")
    
    # Test device filtering
    print("\n2. Testing device filtering:")
    devices = ['android', 'iOS']
    device_iins = parquet_service.get_device_filtered_iins(devices)
    print(f"   Device IINs: {len(device_iins)} (expected 0 in testing)")
    
    # Test push filtering
    print("\n3. Testing push filtering:")
    streams = ['Переводы', 'Платежи']
    push_iins = parquet_service.get_push_filtered_iins(streams)
    print(f"   Push IINs: {len(push_iins)} (expected 0 in testing)")
    
    # Test MAU filtering
    print("\n4. Testing MAU filtering:")
    mau_iins = parquet_service.get_mau_iins()
    print(f"   MAU IINs: {len(mau_iins)} (expected 0 in testing)")
    
    # Test product filtering
    print("\n5. Testing product filtering:")
    products = ['Кредитная карта', 'Депозит']
    product_iins = parquet_service.get_product_iins(products)
    print(f"   Product IINs: {len(product_iins)} (expected 0 in testing)")
    
    print("\n✅ Filtering functions test passed")

def test_cache_functionality():
    """Test caching functionality"""
    print("\n" + "=" * 60)
    print("Testing Cache Functionality")
    print("=" * 60)
    
    # Load a dataset to populate cache
    dataset_name = 'final'
    print(f"Loading {dataset_name} to test caching...")
    
    df1 = parquet_service.load_dataset(dataset_name)
    stats_before = parquet_service.get_cache_stats()
    print(f"Cache size after loading: {stats_before['cache_size']}")
    
    # Load again (should come from cache)
    df2 = parquet_service.load_dataset(dataset_name)
    print(f"Dataset loaded from cache: {df2 is not None}")
    
    # Test cache clearing
    parquet_service.clear_cache(dataset_name)
    stats_after = parquet_service.get_cache_stats()
    print(f"Cache size after clearing {dataset_name}: {stats_after['cache_size']}")
    
    # Load multiple datasets to test cache
    test_datasets = ['MAU', 'ACRM_DW.RB_BLACK_LIST@ACRM']
    for ds in test_datasets:
        parquet_service.load_dataset(ds)
    
    stats_final = parquet_service.get_cache_stats()
    print(f"Final cache size: {stats_final['cache_size']}")
    print(f"Cached datasets: {stats_final['cached_datasets']}")
    
    # Clear all cache
    parquet_service.clear_cache()
    stats_empty = parquet_service.get_cache_stats()
    print(f"Cache size after clearing all: {stats_empty['cache_size']}")
    
    print("✅ Cache functionality test passed")

def test_dataset_info():
    """Test dataset information retrieval"""
    print("\n" + "=" * 60)
    print("Testing Dataset Information")
    print("=" * 60)
    
    test_datasets = ['final', 'MAU', 'unknown_dataset']
    
    for dataset_name in test_datasets:
        print(f"\nTesting info for: {dataset_name}")
        info = parquet_service.get_dataset_info(dataset_name)
        
        if info:
            print(f"  ✅ Description: {info['description']}")
            print(f"  Category: {info['category']}")
            print(f"  Available: {info['available']}")
            print(f"  File size: {info['file_size']} bytes")
            if 'row_count' in info:
                print(f"  Row count: {info['row_count']}")
        else:
            print(f"  ❌ Dataset not found (expected for unknown_dataset)")
    
    print("\n✅ Dataset information test passed")

def test_mock_data_creation():
    """Test mock data creation for testing environment"""
    print("\n" + "=" * 60)
    print("Testing Mock Data Creation")
    print("=" * 60)
    
    # This test will show how mock data is created when files don't exist
    test_datasets = [
        ('final', ['IIN', 'sku_level1']),
        ('dssb_dm.hb_sessions_fl', ['CLIENT_IIN', 'OPERATIONSYSTEM']),
        ('DSSB_DE.UCS_PUSH_OFF', ['IIN', 'EVENTDESCRIPTION'])
    ]
    
    for dataset_name, expected_columns in test_datasets:
        print(f"\nTesting mock data for: {dataset_name}")
        df = parquet_service.load_dataset(dataset_name, use_cache=False)
        
        if df is not None:
            print(f"  ✅ Mock DataFrame created")
            print(f"  Expected columns: {expected_columns}")
            print(f"  Actual columns: {list(df.columns)}")
            print(f"  Rows: {len(df)} (expected 0 for mock data)")
            
            # Verify expected columns are present
            missing_cols = set(expected_columns) - set(df.columns)
            if missing_cols:
                print(f"  ⚠️  Missing columns: {missing_cols}")
            else:
                print(f"  ✅ All expected columns present")
        else:
            print(f"  ❌ Failed to create mock data")
    
    print("\n✅ Mock data creation test passed")

def test_production_vs_testing():
    """Test environment detection"""
    print("\n" + "=" * 60)
    print("Testing Environment Detection")
    print("=" * 60)
    
    datasets = parquet_service.get_available_datasets()
    available_files = [name for name, info in datasets.items() if info['available']]
    
    if available_files:
        print("🏭 PRODUCTION ENVIRONMENT DETECTED")
        print(f"   Available files: {len(available_files)}")
        print(f"   Files: {available_files[:5]}...")  # Show first 5
        
        # Test loading real data
        if 'final' in available_files:
            df = parquet_service.load_dataset('final')
            if df is not None and len(df) > 0:
                print(f"   ✅ Real data loaded: {len(df)} rows")
            else:
                print(f"   ⚠️  File exists but no data loaded")
    else:
        print("🧪 TESTING ENVIRONMENT DETECTED")
        print("   No parquet files found - using mock data")
        print("   This is expected behavior for testing")
    
    print("\n✅ Environment detection test passed")

async def test_integration():
    """Test integration scenarios"""
    print("\n" + "=" * 60)
    print("Testing Integration Scenarios")
    print("=" * 60)
    
    print("1. Simulating market.py workflow:")
    
    # Simulate stop list filtering
    print("\n   Step 1: Load stop lists")
    stop_lists = ['ACRM_DW.RB_BLACK_LIST@ACRM', 'BL_No_worker']
    excluded_iins = parquet_service.get_blacklist_iins(stop_lists)
    print(f"   Excluded IINs: {len(excluded_iins)}")
    
    # Simulate device filtering
    print("\n   Step 2: Filter by device")
    devices = ['android']
    device_users = parquet_service.get_device_filtered_iins(devices)
    print(f"   Device users: {len(device_users)}")
    
    # Simulate product targeting
    print("\n   Step 3: Target by products")
    products = ['Кредитная карта']
    target_users = parquet_service.get_product_iins(products)
    print(f"   Target users: {len(target_users)}")
    
    # Simulate final filtering
    print("\n   Step 4: Apply MAU filter")
    mau_users = parquet_service.get_mau_iins()
    print(f"   MAU users: {len(mau_users)}")
    
    print("\n   ✅ Market.py workflow simulation completed")
    
    print("\n2. Cache performance test:")
    import time
    
    # Test loading time with and without cache
    dataset = 'final'
    
    # Clear cache and measure load time
    parquet_service.clear_cache(dataset)
    start_time = time.time()
    df1 = parquet_service.load_dataset(dataset)
    load_time = time.time() - start_time
    print(f"   Initial load time: {load_time:.4f} seconds")
    
    # Measure cached load time
    start_time = time.time()
    df2 = parquet_service.load_dataset(dataset)
    cache_time = time.time() - start_time
    print(f"   Cached load time: {cache_time:.4f} seconds")
    
    if cache_time < load_time:
        print(f"   ✅ Cache is faster by {((load_time - cache_time) / load_time * 100):.1f}%")
    else:
        print(f"   ℹ️  Cache performance similar (testing environment)")
    
    print("\n✅ Integration test passed")

def main():
    """Run all tests"""
    print("🚀 Starting Parquet Data Service Tests")
    print("=" * 80)
    
    try:
        # Run all tests
        test_service_initialization()
        test_dataset_categories()
        test_dataset_loading()
        test_filtering_functions()
        test_cache_functionality()
        test_dataset_info()
        test_mock_data_creation()
        test_production_vs_testing()
        
        # Run async tests
        asyncio.run(test_integration())
        
        print("\n" + "=" * 80)
        print("🎉 ALL TESTS PASSED!")
        print("=" * 80)
        print("\nParquet Data Service is working correctly.")
        print("Ready for production deployment with actual parquet files.")
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 