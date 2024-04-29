import os
import pickle
import pandas as pd


def load_pickle(file_path):
    try:
        with open(file_path, 'rb') as f:
            data = pickle.load(f)
            return data
    except FileNotFoundError:
        print("File not found!")
    except Exception as e:
        print("Error loading pickle file:", e)


def main():
    # Get the directory of the current Python script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the path to the .pkl file
    file_path = os.path.join(script_dir, 'ingr_map.pkl')

    # Load the data from the .pkl file
    loaded_data = load_pickle(file_path)

    # Convert loaded data to pandas DataFrame
    if loaded_data is not None:
        df = pd.DataFrame(loaded_data)

        # Define path to CSV file
        csv_file = os.path.join(script_dir, 'ingredient_mapping.csv')

        # Write DataFrame to CSV
        df.to_csv(csv_file, index=False)
        print("DataFrame exported to", csv_file)


if __name__ == "__main__":
    main()
