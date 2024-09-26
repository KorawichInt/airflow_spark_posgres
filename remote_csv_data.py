# from datasets import load_dataset
# import pandas as pd

# def read_csv_from_hf():
#     dataset = load_dataset("fka/awesome-chatgpt-prompts", data_files="prompts.csv", split="train")
#     df = pd.DataFrame(dataset)
#     return df

from datasets import load_dataset
import pandas as pd

def read_csv_from_hf():
    # Load the dataset from Hugging Face (CSV file)
    dataset = load_dataset("fka/awesome-chatgpt-prompts", data_files="prompts.csv", split="train")
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(dataset)
    
    # Save the DataFrame to CSV
    csv_file_path = "src/spark/assets/data/prompts.csv"
    df.to_csv(csv_file_path, index=False)
    print(f"CSV file saved to: {csv_file_path}")
    
    return csv_file_path

if __name__ == "__main__":
    read_csv_from_hf()

