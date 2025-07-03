#!/usr/bin/env python3
"""
Test script to verify all database connections are working.
Run this script to test the new database connection functionality.
"""

import os
import sys
from dotenv import load_dotenv

# Add the current directory to Python path to import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
load_dotenv()

def test_connections():
    """Test all database connections"""
    print("🔍 Testing Database Connections for DataQuery Pro")
    print("=" * 60)
    
    try:
        from database import (
            test_connection, 
            test_spss_connection, 
            test_dssb_ocds_connection,
            test_ed_ocds_connection,
            test_all_connections
        )
        
        # Test individual connections
        databases = [
            ("DSSB_APP", test_connection),
            ("SPSS", test_spss_connection),
            ("DSSB_OCDS", test_dssb_ocds_connection),
            ("ED_OCDS", test_ed_ocds_connection)
        ]
        
        print("\n📋 Individual Connection Tests:")
        print("-" * 40)
        
        results = {}
        for db_name, test_func in databases:
            try:
                result = test_func()
                status_icon = "✅" if result["connected"] else "❌"
                results[db_name] = result["connected"]
                print(f"{status_icon} {db_name:12} : {result['message']}")
            except Exception as e:
                results[db_name] = False
                print(f"❌ {db_name:12} : Error - {str(e)}")
        
        # Test all connections at once
        print(f"\n🔄 Testing All Connections:")
        print("-" * 40)
        
        try:
            all_results = test_all_connections()
            print(f"📊 Overall Status: {all_results['overall_status'].upper()}")
            print(f"📈 Success Rate: {all_results['successful_connections']}/{all_results['total_connections']}")
            print(f"💬 Message: {all_results['message']}")
            
            if all_results['overall_status'] == 'success':
                print("\n🎉 All database connections are working!")
                print("✨ Ready to proceed with campaign management features.")
                return True
            elif all_results['overall_status'] == 'partial':
                print("\n⚠️  Some database connections are not working.")
                print("📝 Check your .env file and network connectivity.")
                return False
            else:
                print("\n🚨 No database connections are working.")
                print("❗ Please check your configuration and network connectivity.")
                return False
                
        except Exception as e:
            print(f"❌ Error testing all connections: {str(e)}")
            return False
            
    except ImportError as e:
        print(f"❌ Import Error: {str(e)}")
        print("📋 Make sure you're running this from the database-backend directory")
        return False
    except Exception as e:
        print(f"❌ Unexpected Error: {str(e)}")
        return False

def check_environment():
    """Check if required environment variables are set"""
    print("\n🔧 Environment Configuration Check:")
    print("-" * 40)
    
    required_vars = [
        # DSSB_APP
        "ORACLE_HOST", "ORACLE_SID", "ORACLE_USER", "ORACLE_PASSWORD",
        # SPSS  
        "SPSS_ORACLE_HOST", "SPSS_ORACLE_SID", "SPSS_ORACLE_USER", "SPSS_ORACLE_PASSWORD",
        # DSSB_OCDS
        "DSSB_OCDS_ORACLE_HOST", "DSSB_OCDS_ORACLE_SID", "DSSB_OCDS_ORACLE_USER", "DSSB_OCDS_ORACLE_PASSWORD",
        # ED_OCDS
        "ED_OCDS_ORACLE_HOST", "ED_OCDS_ORACLE_SID", "ED_OCDS_ORACLE_USER", "ED_OCDS_ORACLE_PASSWORD"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
        else:
            print(f"✅ {var}")
    
    if missing_vars:
        print(f"\n❌ Missing environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print(f"\n📝 Please update your .env file with the missing variables.")
        print(f"📖 See DATABASE_CONNECTIONS_SETUP.md for more information.")
        return False
    else:
        print(f"\n✅ All required environment variables are set!")
        return True

if __name__ == "__main__":
    print("DataQuery Pro - Database Connection Test")
    print("🚀 Market.py Migration - Step 1: Database Connections")
    
    # Check environment first
    env_ok = check_environment()
    
    if env_ok:
        # Test connections
        success = test_connections()
        
        if success:
            print("\n" + "="*60)
            print("🎯 NEXT STEPS:")
            print("1. ✅ Database connections established")
            print("2. 🔄 Ready to implement campaign models")
            print("3. 🔄 Add filtering and stop list functionality") 
            print("4. 🔄 Implement campaign management UI")
            print("="*60)
        else:
            print("\n" + "="*60)
            print("🔧 TROUBLESHOOTING NEEDED:")
            print("1. Check database credentials in .env file")
            print("2. Verify network connectivity to Oracle hosts")
            print("3. Ensure Oracle client libraries are installed")
            print("4. Check firewall settings")
            print("="*60)
    else:
        print("\n" + "="*60)
        print("📝 CONFIGURATION REQUIRED:")
        print("1. Create/update .env file with database credentials")
        print("2. See DATABASE_CONNECTIONS_SETUP.md for guidance")
        print("3. Run this script again after configuration")
        print("="*60) 