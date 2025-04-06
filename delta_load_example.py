import polars as pl
# import os
from pathlib import Path

def create_initial_delta_table(df, output_path):
    """
    Create an initial Delta table from a Polars DataFrame.
    
    Args:
        df (pl.DataFrame): The Polars DataFrame to write to Delta
        output_path (str): Path where the Delta table will be created
    """
    # Convert path to absolute path
    output_path = Path(output_path).resolve()
    print(f"Creating initial Delta table at: {output_path}")
    
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Creating initial Delta table at: {output_path}")
    
    # Write the DataFrame to a Delta table
    df.write_delta(output_path, mode='overwrite')
    
    print(f"Initial Delta table created successfully with {len(df)} rows")
    print("Initial Delta table contents:")
    output_path_str = str(output_path) + '/'
    print(pl.read_delta(output_path_str))

def merge_into_delta_table(source_df, target_path, merge_key):
    """
    Merge a source DataFrame into an existing Delta table.
    
    Args:
        source_df (pl.DataFrame): The source DataFrame to merge
        target_path (str): Path to the target Delta table
        merge_key (str): The key column to use for merging
    """
    # Convert path to absolute path
    target_path = Path(target_path).resolve()
    
    # Check if target Delta table exists
    if not target_path.exists():
        raise FileNotFoundError(f"Target Delta table not found at: {target_path}")
    
    print(f"Merging {len(source_df)} rows into Delta table at: {target_path}")
    
    # Perform the merge operation
    result = (
        source_df
        .write_delta(
            target_path,
            mode='merge',
            delta_merge_options={
                'predicate': f'source.{merge_key} = target.{merge_key}',
                'source_alias': 'source',
                'target_alias': 'target',
            },
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    
    print("Merge operation completed successfully")
    print(f"Merge statistics: {result}")
    
    print("Updated Delta table contents:")
    target_path_str = str(target_path) + '/'
    print(pl.read_delta(target_path_str))

def delete_from_delta_table(target_path, delete_condition):
    """
    Delete rows from a Delta table based on a condition.
    
    Args:
        target_path (str): Path to the target Delta table
        delete_condition (str): SQL-like condition for deletion
    """
    # Convert path to absolute path
    target_path = Path(target_path).resolve()
    
    # Check if target Delta table exists
    if not target_path.exists():
        raise FileNotFoundError(f"Target Delta table not found at: {target_path}")
    
    print(f"Deleting rows from Delta table at: {target_path} where {delete_condition}")
    
    # Create a DataFrame with the delete condition
    delete_df = pl.DataFrame({
        "condition": [delete_condition]
    })
    
    # Perform the delete operation
    result = (
        delete_df
        .write_delta(
            target_path,
            mode='merge',
            delta_merge_options={
                'predicate': f'target.{delete_condition}',
                'source_alias': 'source',
                'target_alias': 'target',
            },
        )
        .when_matched_delete()
        .execute()
    )
    
    print("Delete operation completed successfully")
    print(f"Delete statistics: {result}")
    
    print("Updated Delta table contents:")
    target_path_str = str(target_path) + '/'    
    print(pl.read_delta(target_path_str))

if __name__ == "__main__":
    # Example usage
    
    # Create a sample DataFrame
    df = pl.DataFrame(
        {
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 40, 45],
            'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
        }
    )
    
    # Path for the Delta table
    delta_path = "example_delta_table"
    
    try:
        # Step 1: Create the initial Delta table
        create_initial_delta_table(df, delta_path)
        
        # Step 2: Create a DataFrame with updates and new records
        updates_df = pl.DataFrame(
            {
                'id': [2, 3, 6],
                'name': ['Bob Updated', 'Charlie Updated', 'Frank'],
                'age': [31, 36, 50],
                'city': ['San Francisco', 'Boston', 'Seattle']
            }
        )
        
        # Step 3: Merge the updates into the Delta table
        merge_into_delta_table(updates_df, delta_path, 'id')
        
        # Step 4: Delete records from the Delta table
        delete_from_delta_table(delta_path, 'age > 40')
        
    except Exception as e:
        print(f"Error: {str(e)}") 