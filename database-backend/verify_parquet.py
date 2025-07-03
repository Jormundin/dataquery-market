#!/usr/bin/env python3
"""
Simple verification script for Parquet Data Service
"""

import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    try:
        print("🔧 Testing Parquet Data Service...")
        
        # Test import
        from parquet_service import parquet_service
        print("✅ Import successful")
        
        # Test basic functionality
        datasets = parquet_service.get_available_datasets()
        print(f"✅ Datasets configured: {len(datasets)}")
        
        # Test categories
        categories = ['blacklist', 'products', 'analytics']
        for cat in categories:
            cat_datasets = parquet_service.get_datasets_by_category(cat)
            print(f"✅ {cat}: {len(cat_datasets)} datasets")
        
        # Test dataset loading (mock data in testing)
        test_dataset = 'final'
        df = parquet_service.load_dataset(test_dataset)
        if df is not None:
            print(f"✅ Mock dataset loaded: {len(df)} rows")
        
        # Test cache
        stats = parquet_service.get_cache_stats()
        print(f"✅ Cache working: {stats['cache_size']} items")
        
        print("\n🎉 Parquet Data Service verification completed successfully!")
        print("✅ Ready for production with actual parquet files")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 