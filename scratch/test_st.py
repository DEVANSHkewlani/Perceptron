import time
print("Importing sentence_transformers...")
t0 = time.time()
from sentence_transformers import SentenceTransformer
print(f"Imported in {time.time() - t0:.2f}s. Loading model...")
t1 = time.time()
model = SentenceTransformer("all-MiniLM-L6-v2")
print(f"Model loaded in {time.time() - t1:.2f}s. Total time: {time.time() - t0:.2f}s.")
