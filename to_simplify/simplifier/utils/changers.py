def get_samples_hash(db_id: str, question: str):
    samples_hash = f"{db_id}#{question}"
    return samples_hash
