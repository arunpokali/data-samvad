#!/usr/bin/env python3
import os
import glob
import pandas as pd
from typing import List
from multiprocessing import Pool
from tqdm import tqdm

from langchain.document_loaders import CSVLoader, TextLoader
from langchain.docstore.document import Document
from langchain.vectorstores import Chroma 
from langchain.embeddings import HuggingFaceEmbeddings
from constants import CHROMA_SETTINGS

# Load environment variables
persist_directory = os.environ.get('PERSIST_DIRECTORY', 'db')
source_directory = os.environ.get('SOURCE_DIRECTORY', 'source_documents')
embeddings_model_name = os.environ.get('EMBEDDINGS_MODEL_NAME', 'all-mpnet-base-v2')

# Custom CSV loader
class MyCSVLoader(CSVLoader):
    """Custom CSV Loader to load only the top 10 rows"""

    def load(self) -> List[Document]:
        """Load only the top 10 rows of the CSV file"""
        try:
            df = pd.read_csv(self.file_path, nrows=10)
        except Exception as e:
            raise RuntimeError(f"Error reading CSV file {self.file_path}: {e}") from e

        documents = []
        for _, row in df.iterrows():
            content = str(row.to_dict())
            doc = Document(page_content=content)
            documents.append(doc)

        return documents

# Custom Text Loader
class MyTextLoader(TextLoader):
    """Custom Text Loader for .txt files"""

    def load(self) -> List[Document]:
        """Load the content of a text file"""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                content = file.read()
        except Exception as e:
            raise RuntimeError(f"Error reading text file {self.file_path}: {e}") from e

        return [Document(page_content=content)]

# Map file extensions to document loaders and their arguments
LOADER_MAPPING = {
    ".csv": (MyCSVLoader, {}),
    ".txt": (MyTextLoader, {}),
}

def load_single_document(file_path: str) -> List[Document]:
    ext = "." + file_path.rsplit(".", 1)[-1]
    if ext in LOADER_MAPPING:
        loader_class, loader_args = LOADER_MAPPING[ext]
        loader = loader_class(file_path, **loader_args)
        return loader.load()

    raise ValueError(f"Unsupported file extension '{ext}'")

def load_documents(source_dir: str) -> List[Document]:
    """
    Loads documents from the source documents directory
    """
    all_files = []
    for ext in LOADER_MAPPING:
        all_files.extend(glob.glob(os.path.join(source_dir, f"**/*{ext}"), recursive=True))

    with Pool(processes=os.cpu_count()) as pool:
        results = []
        with tqdm(total=len(all_files), desc='Loading new documents', ncols=80) as pbar:
            for i, docs in enumerate(pool.imap_unordered(load_single_document, all_files)):
                results.extend(docs)
                pbar.update()

    return results

def does_vectorstore_exist(persist_directory: str) -> bool:
    """
    Checks if vectorstore exists
    """
    if os.path.exists(os.path.join(persist_directory, 'index')):
        if os.path.exists(os.path.join(persist_directory, 'chroma-collections.parquet')) and os.path.exists(os.path.join(persist_directory, 'chroma-embeddings.parquet')):
            list_index_files = glob.glob(os.path.join(persist_directory, 'index/*.bin'))
            list_index_files += glob.glob(os.path.join(persist_directory, 'index/*.pkl'))
            if len(list_index_files) > 3:
                return True
    return False

def main():
    try:
        # Create embeddings
        embeddings = HuggingFaceEmbeddings(model_name=embeddings_model_name)

        if does_vectorstore_exist(persist_directory):
            print(f"Appending to existing vectorstore at {persist_directory}")
            Chroma().delete_collection()
            db = Chroma(persist_directory=persist_directory, embedding_function=embeddings, client_settings=CHROMA_SETTINGS)
            collection = db.get()

            documents = load_documents(source_directory)
            print(f"Creating embeddings for loaded documents...")
            db.add_documents(documents)
        else:
            print("Creating new vectorstore")
            documents = load_documents(source_directory)
            print(f"Creating embeddings for loaded documents...")
            db = Chroma.from_documents(documents, embeddings, persist_directory=persist_directory)
        db.persist()
        db = None

        print(f"Ingestion complete! You can now run privateGPT.py to query your documents")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Exiting the script.")
        exit(0)

if __name__ == "__main__":
    main()
