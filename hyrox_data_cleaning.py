import pandas as pd
import json

# ============================================
# STEP 1: LOAD DATA
# ============================================
print("=" * 60)
print("HYROX DATA PREPARATION FOR MONGODB")
print("=" * 60)

file_path = r'C:\Users\aldox\Documents\Master UCM Data Science\BDD_NoSQL\Tarea\hyrox_results.csv'

df = pd.read_csv(file_path, encoding='windows-1252')
print(f"\n✓ Loaded dataset: {df.shape[0]:,} rows, {df.shape[1]} columns")

# ============================================
# STEP 2: FIX EVENT NAMES (ENCODING ISSUES)
# ============================================
print("\n" + "=" * 60)
print("FIXING EVENT NAMES")
print("=" * 60)

event_corrections = {
    'JGDMS4JI5C9': 'S6 2023 Munich',
    'JGDMS4JI464': 'S5 2023 Munich',
    '2EFMS4JI2BE': 'S4 2022 Munich',
    'JGDMS4JI46D': 'S6 2023 Malmo',
    'JGDMS4JI468': 'S5 2023 Koln'
}

df.loc[df['event_id'].isin(event_corrections.keys()), 'event_name'] = df['event_id'].map(event_corrections)

print("✓ Fixed encoding issues in event names")
print(f"  - Updated {len(event_corrections)} events")

# ============================================
# STEP 3: CREATE is_championship COLUMN
# ============================================
print("\n" + "=" * 60)
print("CREATING CHAMPIONSHIP FLAG")
print("=" * 60)

df['is_championship'] = df['event_name'].str.contains('Championship', case=False, na=False)

print(f"✓ Created is_championship column")
print(f"  - Championship events: {df['is_championship'].sum():,}")
print(f"  - Regular events: {(~df['is_championship']).sum():,}")

# ============================================
# STEP 4: EXTRACT YEAR AND CITY
# ============================================
print("\n" + "=" * 60)
print("EXTRACTING EVENT YEAR AND CITY")
print("=" * 60)

df['event_year'] = df['event_name'].str.extract(r'(\d{4})')

def extract_city(row):
    """Extract city handling different championship formats"""
    event_name = row['event_name']
    is_champ = row['is_championship']
    
    if is_champ:
        if ' - ' in event_name:
            city = event_name.split(' - ')[0]
            city = city.split(maxsplit=2)[-1]
        elif 'Championships' in event_name and event_name.split()[-1] != 'Championships':
            city = event_name.split()[-1]
        else:
            parts = event_name.split()
            for i, part in enumerate(parts):
                if part.isdigit() and len(part) == 4:
                    city = parts[i + 1] if i + 1 < len(parts) else None
                    break
    else:
        parts = event_name.split(maxsplit=2)
        city = parts[2] if len(parts) > 2 else None
    
    return city

df['event_city'] = df.apply(extract_city, axis=1)
df['event_year'] = df['event_year'].astype('Int64')

print("✓ Extracted event_year and event_city")
print(f"  - Years: {sorted(df['event_year'].dropna().unique())}")
print(f"  - Unique cities: {df['event_city'].nunique()}")

# ============================================
# STEP 5: CONVERT TIMES TO SECONDS
# ============================================
print("\n" + "=" * 60)
print("CONVERTING TIMES TO SECONDS")
print("=" * 60)

def time_to_seconds(time_str):
    """Convert time string (HH:MM:SS or MM:SS) to total seconds"""
    if pd.isna(time_str) or time_str == '' or time_str == '0:00:00':
        return 0
    
    try:
        parts = str(time_str).split(':')
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        else:
            return 0
    except:
        return 0

time_columns = ['total_time', 'work_time', 'roxzone_time', 'run_time'] + \
               [f'run_{i}' for i in range(1, 9)] + \
               [f'work_{i}' for i in range(1, 9)] + \
               [f'roxzone_{i}' for i in range(1, 9)]

for col in time_columns:
    df[f'{col}_seconds'] = df[col].apply(time_to_seconds)

print(f"✓ Converted {len(time_columns)} time columns to seconds")

# ============================================
# STEP 6: VALIDATE ATHLETE COMPLETION
# ============================================
print("\n" + "=" * 60)
print("VALIDATING ATHLETE COMPLETION")
print("=" * 60)

def check_valid_completion(row):
    """
    Check if athlete completed all 8 splits with valid times.
    Valid = All runs and work stations have time > 0
    """
    for i in range(1, 9):
        run_time = row[f'run_{i}_seconds']
        work_time = row[f'work_{i}_seconds']
        
        # If any run or work time is 0 or missing, it's invalid
        if run_time <= 0 or work_time <= 0:
            return False
    
    return True

df['is_valid'] = df.apply(check_valid_completion, axis=1)

print(f"✓ Created is_valid column")
print(f"  - Valid completions (finished all 8 stations): {df['is_valid'].sum():,}")
print(f"  - Invalid/DNF (Did Not Finish): {(~df['is_valid']).sum():,}")
print(f"  - Completion rate: {(df['is_valid'].sum() / len(df) * 100):.2f}%")

# ============================================
# STEP 7: HANDLE MISSING VALUES
# ============================================
print("\n" + "=" * 60)
print("HANDLING MISSING VALUES")
print("=" * 60)

df['nationality'] = df['nationality'].fillna('Unknown')
df['age_group'] = df['age_group'].fillna('Not specified')

print("✓ Filled missing values")
print(f"  - Nationality: {(df['nationality'] == 'Unknown').sum():,} set to 'Unknown'")
print(f"  - Age group: {(df['age_group'] == 'Not specified').sum():,} set to 'Not specified'")

# ============================================
# STEP 8: STRUCTURE DATA FOR MONGODB
# ============================================
print("\n" + "=" * 60)
print("STRUCTURING DATA FOR MONGODB")
print("=" * 60)

def create_mongo_document(row):
    splits = []
    for i in range(1, 9):
        split = {
            'split_number': i,
            'run_time': row[f'run_{i}'],
            'run_seconds': int(row[f'run_{i}_seconds']),
            'work_time': row[f'work_{i}'],
            'work_seconds': int(row[f'work_{i}_seconds']),
            'roxzone_time': row[f'roxzone_{i}'],
            'roxzone_seconds': int(row[f'roxzone_{i}_seconds'])
        }
        splits.append(split)
    
    document = {
        'event': {
            'event_id': row['event_id'],
            'event_name': row['event_name'],
            'event_year': int(row['event_year']) if pd.notna(row['event_year']) else None,
            'event_city': row['event_city'],
            'is_championship': bool(row['is_championship'])
        },
        'athlete': {
            'gender': row['gender'],
            'nationality': row['nationality'],
            'age_group': row['age_group'],
            'division': row['division']
        },
        'performance': {
            'total_time': row['total_time'],
            'total_seconds': int(row['total_time_seconds']),
            'work_time': row['work_time'],
            'work_seconds': int(row['work_time_seconds']),
            'roxzone_time': row['roxzone_time'],
            'roxzone_seconds': int(row['roxzone_time_seconds']),
            'run_time': row['run_time'],
            'run_seconds': int(row['run_time_seconds']),
            'is_valid': bool(row['is_valid'])
        },
        'splits': splits
    }
    return document

print("Converting rows to MongoDB documents...")
mongo_documents = [create_mongo_document(row) for _, row in df.iterrows()]
print(f"✓ Created {len(mongo_documents):,} MongoDB documents")

# ============================================
# STEP 9: SAVE TO JSON FILE (NDJSON FORMAT)
# ============================================
print("\n" + "=" * 60)
print("SAVING TO JSON FILE")
print("=" * 60)

output_file = r'C:\Users\aldox\Documents\Master UCM Data Science\BDD_NoSQL\Tarea\hyrox_data.json'

with open(output_file, 'w', encoding='utf-8') as f:
    for doc in mongo_documents:
        f.write(json.dumps(doc, ensure_ascii=False) + '\n')

print(f"✓ Saved to: {output_file}")
print(f"  - Format: NDJSON (one document per line)")
print(f"  - Total documents: {len(mongo_documents):,}")

# ============================================
# STEP 10: DATA SUMMARY
# ============================================
print("\n" + "=" * 60)
print("DATA SUMMARY")
print("=" * 60)
print(f"Total athletes: {len(mongo_documents):,}")
print(f"Unique events: {df['event_name'].nunique()}")
print(f"Event years: {sorted(df['event_year'].dropna().unique())}")
print(f"Unique cities: {df['event_city'].nunique()}")
print(f"Countries: {df['nationality'].nunique()}")
print(f"Age groups: {df['age_group'].nunique()}")

print(f"\nGender distribution:")
for gender, count in df['gender'].value_counts().items():
    print(f"  - {gender}: {count:,}")

print(f"\nDivision distribution:")
for division, count in df['division'].value_counts().items():
    print(f"  - {division}: {count:,}")

print(f"\nChampionship breakdown:")
print(f"  - Regular events: {(~df['is_championship']).sum():,}")
print(f"  - Championship events: {df['is_championship'].sum():,}")

# ============================================
# STEP 11: PREVIEW DOCUMENT STRUCTURE
# ============================================
print("\n" + "=" * 60)
print("SAMPLE DOCUMENT STRUCTURE")
print("=" * 60)
print(json.dumps(mongo_documents[0], indent=2, ensure_ascii=False))

print("\n" + "=" * 60)
print("✓ DATA PREPARATION COMPLETE!")
print("=" * 60)