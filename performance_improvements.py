"""
Performance improvements for NAR Database processing
"""

# Quick fixes you can apply:

# 1. Increase chunk size in processor.py
# Change line ~182: chunk_size=10000 → chunk_size=50000

# 2. Increase database batch size in database.py  
# Change line ~80: batch_size: int = 10000 → batch_size: int = 50000

# 3. Use faster pandas operations instead of iterrows()
# Replace the slow row iteration with vectorized operations

# 4. Enable SQLite optimizations
# Add PRAGMA settings for faster writes

# These changes could make it 3-5x faster immediately
