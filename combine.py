# ============================================
# COMPLETE PYTHON CODE FOR COMBINING NEPAL BANK CSV FILES
# With Network Analysis Preparation for Tableau
# ============================================

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ============================================
# PART 1: SETUP AND CONFIGURATION
# ============================================

print("="*50)
print("NEPAL BANK DATA COMBINER")
print("="*50)

# List of all bank files
banks = [
    'ADBL', 'CZBIL', 'EBL', 'GBIME', 'HBL', 'KBL', 'MBL', 
    'NABIL', 'NBB', 'NBL', 'NICA', 'PCBL', 'PRVU', 
    'SANIMA', 'SBI', 'SBL', 'SCB'
]

# Bank full names for better readability
bank_full_names = {
    'ADBL': 'Agricultural Development Bank',
    'CZBIL': 'Citizens Bank',
    'EBL': 'Everest Bank',
    'GBIME': 'Global IME Bank',
    'HBL': 'Himalayan Bank',
    'KBL': 'Kumari Bank',
    'MBL': 'Machhapuchchhre Bank',
    'NABIL': 'Nabil Bank',
    'NBB': 'Nepal Bangladesh Bank',
    'NBL': 'Nepal Bank Limited',
    'NICA': 'NIC Asia Bank',
    'PCBL': 'Prime Commercial Bank',
    'PRVU': 'Prabhu Bank',
    'SANIMA': 'Sanima Bank',
    'SBI': 'Nepal SBI Bank',
    'SBL': 'Siddhartha Bank',
    'SCB': 'Standard Chartered Bank'
}

# Expected columns in each CSV
expected_columns = ['published_date', 'open', 'high', 'low', 'close', 
                    'per_change', 'traded_quantity', 'traded_amount', 'status']

print(f"\nFound {len(banks)} banks to process:")
for bank in banks:
    print(f"  - {bank}: {bank_full_names[bank]}")

# ============================================
# PART 2: READ AND COMBINE ALL FILES
# ============================================

def read_bank_file(bank_code):
    """Read a single bank CSV file and add metadata"""
    filename = f"{bank_code}.csv"
    
    if not os.path.exists(filename):
        print(f"  ⚠ Warning: {filename} not found, skipping...")
        return None
    
    try:
        # Read CSV
        df = pd.read_csv(filename)
        
        # Basic validation
        if len(df) == 0:
            print(f"  ⚠ {bank_code}: File is empty")
            return None
        
        # Check required columns
        missing_cols = [col for col in expected_columns if col not in df.columns]
        if missing_cols:
            print(f"  ⚠ {bank_code}: Missing columns: {missing_cols}")
            return None
        
        # Add bank metadata
        df['bank_code'] = bank_code
        df['bank_name'] = bank_full_names.get(bank_code, bank_code)
        
        # Convert date
        df['published_date'] = pd.to_datetime(df['published_date'])
        
        # Add time-based columns
        df['year'] = df['published_date'].dt.year
        df['month'] = df['published_date'].dt.month
        df['quarter'] = df['published_date'].dt.quarter
        df['day_of_week'] = df['published_date'].dt.dayofweek
        df['week'] = df['published_date'].dt.isocalendar().week
        
        # Calculate additional metrics
        df['price_range'] = df['high'] - df['low']
        df['avg_price'] = (df['high'] + df['low']) / 2
        df['price_movement'] = df['close'] - df['open']
        
        # Handle missing percentage change
        df['per_change'] = df['per_change'].fillna(0)
        
        # Calculate average trade value (where quantity > 0)
        df['avg_trade_value'] = np.where(
            df['traded_quantity'] > 0,
            df['traded_amount'] / df['traded_quantity'],
            0
        )
        
        # Create unique row ID
        df['row_id'] = df['bank_code'] + '_' + df['published_date'].dt.strftime('%Y%m%d')
        
        print(f"  ✅ {bank_code}: {len(df)} rows ({df['published_date'].min().date()} to {df['published_date'].max().date()})")
        
        return df
        
    except Exception as e:
        print(f"  ❌ {bank_code}: Error reading file - {str(e)}")
        return None

# Process all banks
print("\n" + "="*50)
print("PROCESSING BANK FILES")
print("="*50)

all_data = []
successful_banks = []
failed_banks = []

for bank in banks:
    df = read_bank_file(bank)
    if df is not None:
        all_data.append(df)
        successful_banks.append(bank)
    else:
        failed_banks.append(bank)

# ============================================
# PART 3: COMBINE ALL DATA
# ============================================

print("\n" + "="*50)
print("COMBINING DATA")
print("="*50)

if len(all_data) > 0:
    # Concatenate all dataframes
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Sort by date and bank
    combined_df = combined_df.sort_values(['published_date', 'bank_code']).reset_index(drop=True)
    
    print(f"\n✅ Successfully processed {len(successful_banks)} out of {len(banks)} banks")
    print(f"❌ Failed: {len(failed_banks)} banks")
    if failed_banks:
        print(f"   Failed banks: {', '.join(failed_banks)}")
    
    print(f"\n📊 Combined Dataset Summary:")
    print(f"   - Total rows: {len(combined_df):,}")
    print(f"   - Total columns: {len(combined_df.columns)}")
    print(f"   - Date range: {combined_df['published_date'].min().date()} to {combined_df['published_date'].max().date()}")
    print(f"   - Banks included: {combined_df['bank_code'].nunique()}")
    print(f"   - Years covered: {sorted(combined_df['year'].unique())}")
    
    # Display sample of the data
    print("\n📋 First 5 rows of combined data:")
    print(combined_df[['published_date', 'bank_code', 'open', 'high', 'low', 'close', 'per_change']].head())

# ============================================
# PART 4: SAVE COMBINED DATA
# ============================================

print("\n" + "="*50)
print("SAVING COMBINED DATA")
print("="*50)

# Save main combined file
output_file = 'all_nepal_banks_combined.csv'
combined_df.to_csv(output_file, index=False)
print(f"✅ Main file saved: {output_file}")

# ============================================
# PART 5: CREATE SUMMARY STATISTICS
# ============================================

print("\n" + "="*50)
print("CREATING SUMMARY STATISTICS")
print("="*50)

# Bank-wise summary
bank_summary = combined_df.groupby('bank_code').agg({
    'close': ['mean', 'std', 'min', 'max', lambda x: x.iloc[-1]],
    'per_change': ['mean', 'std', 'min', 'max'],
    'traded_quantity': ['sum', 'mean'],
    'traded_amount': ['sum', 'mean'],
    'published_date': ['min', 'max']
}).round(2)

# Flatten column names
bank_summary.columns = ['_'.join(col).strip() for col in bank_summary.columns.values]
bank_summary = bank_summary.reset_index()

# Rename columns for clarity
bank_summary = bank_summary.rename(columns={
    'close_mean': 'avg_close_price',
    'close_std': 'price_volatility',
    'close_min': 'min_price',
    'close_max': 'max_price',
    'close_<lambda_0>': 'latest_price',
    'per_change_mean': 'avg_daily_return',
    'per_change_std': 'return_volatility',
    'per_change_min': 'max_loss',
    'per_change_max': 'max_gain',
    'traded_quantity_sum': 'total_volume',
    'traded_quantity_mean': 'avg_daily_volume',
    'traded_amount_sum': 'total_value_traded',
    'traded_amount_mean': 'avg_daily_value',
    'published_date_min': 'first_trading_date',
    'published_date_max': 'last_trading_date'
})

# Add bank names
bank_summary['bank_name'] = bank_summary['bank_code'].map(bank_full_names)

# Save summary
bank_summary.to_csv('bank_summary_statistics.csv', index=False)
print("✅ Bank summary saved: bank_summary_statistics.csv")
print("\nBank Summary Preview:")
print(bank_summary[['bank_code', 'bank_name', 'avg_close_price', 'price_volatility', 
                    'total_volume', 'avg_daily_return']].head(10))

# ============================================
# PART 6: CREATE CORRELATION MATRIX FOR NETWORK ANALYSIS
# ============================================

print("\n" + "="*50)
print("CREATING CORRELATION MATRIX (FOR NETWORK ANALYSIS)")
print("="*50)

try:
    # Create pivot table of closing prices
    pivot_df = combined_df.pivot_table(
        index='published_date', 
        columns='bank_code', 
        values='close'
    )
    
    # Forward fill missing values (if any)
    pivot_df = pivot_df.fillna(method='ffill')
    
    # Remove any remaining NaN rows
    pivot_df = pivot_df.dropna()
    
    print(f"Pivot table shape: {pivot_df.shape}")
    print(f"Date range for correlation: {pivot_df.index.min().date()} to {pivot_df.index.max().date()}")
    
    # Calculate correlation matrix
    corr_matrix = pivot_df.corr()
    
    # Save correlation matrix
    corr_matrix.to_csv('bank_correlation_matrix.csv')
    print("✅ Correlation matrix saved: bank_correlation_matrix.csv")
    
    # Extract top correlations for each bank (for network edges)
    print("\n🔗 Top 3 correlations for each bank:")
    for bank in corr_matrix.columns:
        # Get correlations for this bank, excluding self-correlation
        corr_with_others = corr_matrix[bank].drop(bank).sort_values(ascending=False)
        top_3 = corr_with_others.head(3)
        print(f"  {bank}: {', '.join([f'{idx}({val:.2f})' for idx, val in top_3.items()])}")
        
except Exception as e:
    print(f"⚠ Could not create correlation matrix: {str(e)}")

# ============================================
# PART 7: CREATE TIME SERIES FOR EACH BANK (FOR TABLEAU)
# ============================================

print("\n" + "="*50)
print("CREATING ADDITIONAL FILES FOR TABLEAU")
print("="*50)

# Create monthly aggregated data
combined_df['year_month'] = combined_df['published_date'].dt.to_period('M')
monthly_data = combined_df.groupby(['bank_code', 'year_month']).agg({
    'close': 'mean',
    'high': 'max',
    'low': 'min',
    'per_change': 'mean',
    'traded_quantity': 'sum',
    'traded_amount': 'sum'
}).round(2).reset_index()

monthly_data['year_month'] = monthly_data['year_month'].astype(str)
monthly_data.to_csv('bank_monthly_data.csv', index=False)
print("✅ Monthly data saved: bank_monthly_data.csv")

# Create volatility metrics (30-day rolling)
# Sort by bank and date
combined_df_sorted = combined_df.sort_values(['bank_code', 'published_date'])

# Calculate rolling statistics for each bank
rolling_stats = combined_df_sorted.groupby('bank_code').apply(
    lambda x: x.assign(
        rolling_volatility_30d=x['close'].pct_change().rolling(30, min_periods=5).std() * 100,
        rolling_avg_30d=x['close'].rolling(30, min_periods=5).mean(),
        momentum_5d=x['close'].pct_change(5)
    )
).reset_index(drop=True)

# Select relevant columns for rolling stats
rolling_output = rolling_stats[['published_date', 'bank_code', 'close', 
                                'rolling_volatility_30d', 'rolling_avg_30d', 
                                'momentum_5d']].dropna()
rolling_output.to_csv('bank_rolling_metrics.csv', index=False)
print("✅ Rolling metrics saved: bank_rolling_metrics.csv")

# ============================================
# PART 8: CREATE NETWORK EDGE LIST
# ============================================

print("\n" + "="*50)
print("CREATING NETWORK EDGE LIST (FOR TABLEAU NETWORK GRAPHS)")
print("="*50)

try:
    # Create edge list from correlations
    edges = []
    banks_list = list(corr_matrix.columns)
    
    for i in range(len(banks_list)):
        for j in range(i+1, len(banks_list)):
            bank1 = banks_list[i]
            bank2 = banks_list[j]
            correlation = corr_matrix.loc[bank1, bank2]
            
            # Only include significant correlations (|r| > 0.3)
            if abs(correlation) > 0.3:
                edges.append({
                    'source': bank1,
                    'target': bank2,
                    'correlation': correlation,
                    'abs_correlation': abs(correlation),
                    'relationship_type': 'positive' if correlation > 0 else 'negative'
                })
    
    edges_df = pd.DataFrame(edges)
    edges_df = edges_df.sort_values('abs_correlation', ascending=False)
    edges_df.to_csv('bank_network_edges.csv', index=False)
    print(f"✅ Network edge list saved: bank_network_edges.csv")
    print(f"   Total edges: {len(edges_df)}")
    print(f"   Strongest correlation: {edges_df.iloc[0]['source']} - {edges_df.iloc[0]['target']}: {edges_df.iloc[0]['correlation']:.3f}")
    
except Exception as e:
    print(f"⚠ Could not create edge list: {str(e)}")

# ============================================
# PART 9: FINAL REPORT
# ============================================

print("\n" + "="*50)
print("FINAL SUMMARY")
print("="*50)

print("\n📁 Files Created:")
print("   1. all_nepal_banks_combined.csv - Main combined dataset")
print("   2. bank_summary_statistics.csv - Summary stats per bank")
print("   3. bank_correlation_matrix.csv - Correlation matrix")
print("   4. bank_monthly_data.csv - Monthly aggregated data")
print("   5. bank_rolling_metrics.csv - 30-day rolling metrics")
print("   6. bank_network_edges.csv - Network edge list for Tableau")

print("\n📊 Dataset Overview:")
print(f"   - Banks: {combined_df['bank_code'].nunique()}")
print(f"   - Total records: {len(combined_df):,}")
print(f"   - Date range: {combined_df['published_date'].min().date()} to {combined_df['published_date'].max().date()}")
print(f"   - Years: {sorted(combined_df['year'].unique())}")

print("\n💰 Market Summary:")
print(f"   - Total trading volume: {combined_df['traded_quantity'].sum():,.0f}")
print(f"   - Total value traded: Rs. {combined_df['traded_amount'].sum():,.0f}")
print(f"   - Average daily return: {combined_df['per_change'].mean():.2f}%")
print(f"   - Most volatile day: {combined_df.loc[combined_df['per_change'].idxmax(), 'published_date'].date()} "
      f"({combined_df['per_change'].max():.2f}%)")

print("\n✅ DONE! All files are ready for Tableau import.")
print("="*50)
