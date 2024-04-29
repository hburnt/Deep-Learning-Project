import pandas as pd


def filter_interactions(csv_file):
    """
    Filter interactions DataFrame by removing the 'review' column and
    keeping only rows with the values of the 100 most occurring recipe_ids
    and 10000 most occurring user_ids.

    Args:
    csv_file (str): File path of the CSV file containing the interaction data.

    Returns:
    pd.DataFrame: Filtered DataFrame with interactions.
    """
    # Load the dataset from CSV file into a DataFrame
    df = pd.read_csv(csv_file)

    # Remove the 'review' column
    df = df.drop(columns=['review', 'date'])

    # Find the 1000 most occurring recipe_ids
    top_recipe_ids = df['recipe_id'].value_counts().head(100).index

    # Find the 2000 most occurring user_ids
    top_user_ids = df['user_id'].value_counts().head(30000).index

    # Filter the DataFrame to keep only rows with the top 100 recipe_ids and top 10000 user_ids
    df_filtered = df[(df['recipe_id'].isin(top_recipe_ids)) & (df['user_id'].isin(top_user_ids))]

    # Convert 'user_id', 'recipe_id', and 'rating' columns to integers
    df_filtered['user_id'] = df_filtered['user_id'].astype(int)
    df_filtered['recipe_id'] = df_filtered['recipe_id'].astype(int)
    df_filtered['rating'] = df_filtered['rating'].astype(int)

    return df_filtered


def process_recipe_data(filtered_df, recipe_csv):
    """
    Read recipe data from a CSV file, match the rows with the same recipe_id as in the filtered DataFrame,
    rename the 'i' column to 'recipe_id_mapped', and process the 'techniques' column.

    Args:
    filtered_df (pd.DataFrame): Filtered DataFrame with interactions.
    recipe_csv (str): File path of the CSV file containing recipe data.

    Returns:
    pd.DataFrame: Processed DataFrame with recipe data.
    """
    # Read recipe data from CSV file into a DataFrame
    recipe_df = pd.read_csv(recipe_csv)

    # Remove the name_tokens, ingredient_tokens, and steps_tokens columns
    recipe_df = recipe_df.drop(columns=['name_tokens', 'ingredient_tokens', 'steps_tokens'])

    # Merge filtered DataFrame with recipe DataFrame on 'recipe_id'
    merged_df = pd.merge(filtered_df, recipe_df, left_on='recipe_id', right_on='id', how='left')

    # Rename the 'i' column to 'recipe_id_mapped'
    merged_df.rename(columns={'i': 'recipe_id_mapped'}, inplace=True)

    # Drop rows with NaN values in 'techniques' column
    merged_df = merged_df.dropna(subset=['techniques'])

    # Convert string representation of list to actual list of integers
    merged_df['techniques'] = merged_df['techniques'].apply(
        lambda x: [int(i.strip()) for i in x.strip('[]').split(',')])

    # Create new DataFrame for techniques
    techniques_df = pd.DataFrame(merged_df['techniques'].tolist()).add_prefix('technique_')

    # Ensure all elements in techniques_df are integers
    techniques_df = techniques_df.astype(int)

    # Concatenate merged_df and techniques_df along the column axis
    processed_df = pd.concat([merged_df, techniques_df], axis=1)

    # Drop the techniques column
    processed_df = processed_df.drop(columns=['techniques'])

    # Create a list of unique ingredient IDs
    unique_ingredient_ids = set()
    for ids_list in merged_df['ingredient_ids']:
        ids_list = ids_list.strip('[]').split(', ')
        for id_str in ids_list:
            unique_ingredient_ids.add(int(id_str))

    # Iterate over each row in merged_df
    ingredient_rows = []
    for index, row in merged_df.iterrows():
        ingredient_ids = [int(id_str) for id_str in row['ingredient_ids'].strip('[]').split(', ')]

        # Create a row to store ingredient presence
        ingredient_row = {}
        for id_val in unique_ingredient_ids:
            if id_val in ingredient_ids:
                ingredient_row[id_val] = 1
            else:
                ingredient_row[id_val] = 0

        ingredient_rows.append(ingredient_row)

    # Create DataFrame from the list of rows
    ingredients_df = pd.DataFrame(ingredient_rows)

    # Fill NaN values with 0
    ingredients_df = ingredients_df.fillna(0)

    # Generate CSV to map ingredients
    ingredients_df = filter_ingredient_mapping(ingredient_mapping_csv='ingredient_mapping.csv',
                                               ingredients_df=ingredients_df)

    # Concatenate merged_df and ingredients_df along the column axis
    processed_df = pd.concat([processed_df, ingredients_df], axis=1)

    # Drop the techniques column
    processed_df = processed_df.drop(columns=['ingredient_ids'])

    return processed_df


def filter_ingredient_mapping(ingredient_mapping_csv, ingredients_df):
    """
    Open ingredient mapping CSV file, load the data into a DataFrame called all_ingredients_df,
    and filter it based on the ingredients DataFrame.

    Args:
    ingredient_mapping_csv (str): File path of the CSV file containing ingredient mapping data.
    ingredients_df (pd.DataFrame): DataFrame containing ingredients data.

    Returns:
    pd.DataFrame: Filtered DataFrame containing ingredient mapping data.
    """
    # Read ingredient mapping data from CSV file into a DataFrame
    all_ingredients_df = pd.read_csv(ingredient_mapping_csv)

    # Filter ingredient mapping DataFrame based on ingredients DataFrame
    filtered_ingredients_df = pd.DataFrame(columns=all_ingredients_df.columns)
    for column in ingredients_df.columns:
        filtered_row = all_ingredients_df[all_ingredients_df['id'] == int(column)]
        filtered_ingredients_df = pd.concat([filtered_ingredients_df, filtered_row])

    # Drop duplicates to ensure only one row per unique ID
    # filtered_ingredients_df.drop_duplicates(subset='id', keep='first', inplace=True)

    # Replace column names in ingredients_df with values from 'replaced' column
    for column in ingredients_df.columns:
        replacement_value = \
            filtered_ingredients_df.loc[filtered_ingredients_df['id'] == int(column), 'replaced'].values[0]
        ingredients_df.rename(columns={column: replacement_value}, inplace=True)

    # Export filtered DataFrame to CSV file
    # filtered_ingredients_df.to_csv("filtered_ingredient_mapping.csv", index=False)

    return ingredients_df


def process_raw_recipes(raw_recipes_csv, processed_df):
    """
    Load data from RAW_recipes.csv, filter based on processed DataFrame,
    and map values to the processed DataFrame.

    Args:
    raw_recipes_csv (str): File path of the CSV file containing raw recipe data.
    processed_df (pd.DataFrame): DataFrame containing processed recipe data.

    Returns:
    pd.DataFrame: Processed DataFrame with mapped values from raw recipe data.
    """
    # Load data from RAW_recipes.csv into a DataFrame
    raw_recipe_df = pd.read_csv(raw_recipes_csv)

    # Drop unnecessary columns
    raw_recipe_df.drop(columns=['contributor_id', 'submitted', 'tags', 'steps', 'description', 'ingredients'],
                       inplace=True)

    # Filter rows in raw_recipe_df based on processed_df
    filtered_raw_recipe_df = raw_recipe_df[raw_recipe_df['id'].isin(processed_df['recipe_id'])]

    # Map values from raw_recipe_df to processed_df
    processed_df_mapped = pd.merge(processed_df, filtered_raw_recipe_df, left_on='recipe_id', right_on='id', how='left')

    # Drop unnecessary columns
    processed_df_mapped.rename(columns={'id_x': 'id'}, inplace=True)
    processed_df_mapped.drop(columns=['id_y'], inplace=True)

    # Filter to keep only the first occurrence of each unique recipe_id
    # processed_df_mapped.drop_duplicates(subset='recipe_id', inplace=True)

    # Export recipe_id and name columns to recipe_mapping.csv
    # recipe_mapping_df = processed_df_mapped[['recipe_id', 'name']]
    # recipe_mapping_df.to_csv('recipe_mapping.csv', index=False)

    return processed_df_mapped


def process_nutrition_column(df):
    """
    Process the 'nutrition' column to create new columns for each nutrition value.

    Args:
    df (pd.DataFrame): DataFrame containing the 'nutrition' column.

    Returns:
    pd.DataFrame: DataFrame with new columns for each nutrition value.
    """
    # Drop rows with NaN values in 'nutrition' column
    df = df.dropna(subset=['nutrition'])

    # Convert string representation of list to actual list of integers
    df['nutrition'] = df['nutrition'].apply(lambda x: [float(i) for i in x.strip('[]').split(',')])

    # Create new DataFrame for nutrition values
    nutrition_df = pd.DataFrame(df['nutrition'].tolist(), columns=['calories', 'percent_fat', 'percent_sugar',
                                                                   'percent_sodium', 'percent_protein',
                                                                   'percent_sat_fat', 'percent_carb'])

    # Concatenate df and nutrition_df along the column axis
    processed_df = pd.concat([df, nutrition_df], axis=1)

    # Drop unnecessary columns
    processed_df.drop(columns=['nutrition'], inplace=True)

    return processed_df


def main():
    # Path to the interaction CSV file
    interaction_csv = "RAW_interactions.csv"

    # Call filter_interactions function
    filtered_df = filter_interactions(interaction_csv)

    # Print information about the filtered DataFrame
    print("Information about the filtered DataFrame:")
    print("Size of the DataFrame (number of rows, number of columns):", filtered_df.shape)
    print("Columns of the DataFrame:")
    print(filtered_df.columns)

    # Display the first few rows of the filtered DataFrame
    print("\nFirst few rows of the filtered DataFrame:")
    print(filtered_df.head())

    # Path to the recipe CSV file
    recipe_csv = "PP_recipes.csv"

    # Process recipe data
    processed_df = process_recipe_data(filtered_df, recipe_csv)

    # Path to the RAW_recipes CSV file
    raw_recipes_csv = "RAW_recipes.csv"

    # Process raw recipes data
    longer_processed_df = process_raw_recipes(raw_recipes_csv, processed_df)

    # Unpack nutrition data info
    added_nutrition_df = process_nutrition_column(df=longer_processed_df)

    # Print information about the processed DataFrame
    print("\nInformation about the processed DataFrame:")
    print("Size of the DataFrame (number of rows, number of columns):", processed_df.shape)
    print("Columns of the DataFrame:")
    print(added_nutrition_df.columns)

    # Display the first few rows of the processed DataFrame
    print("\nFirst few rows of the processed DataFrame:")
    print(added_nutrition_df.head())

    # Export added_nutrition_df to a CSV called "final_dataset.csv"
    added_nutrition_df.to_csv("final_dataset.csv", index=False)


if __name__ == "__main__":
    main()
